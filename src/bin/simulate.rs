use clap::Parser;
use consensus_benchmarking::PostgresClient;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use rand_distr::Normal;
use tokio_postgres::config::SslMode;
use tracing_subscriber::EnvFilter;

const MAX_APPENDS_BEFORE_TRUNCATE: u32 = 5000;
const MAX_STATE_BYTES: u32 = 4000;

/// Program to generate some mock data that mimics consensus.
#[derive(clap::Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Url of the database.
    #[arg(long)]
    url: String,
    /// Port to connect to.
    #[arg(long)]
    port: Option<u16>,
    /// Username to connect as.
    #[arg(long)]
    username: String,
    /// Password to connect with.
    #[arg(long)]
    password: String,
    /// Seed to use for the rng to generate data.
    #[arg(long)]
    seed: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .user(args.username)
        .password(args.password)
        .host(args.url)
        .ssl_mode(SslMode::Require);
    if let Some(port) = args.port {
        pg_config.port(port);
    }
    let pool = PostgresClient::open(pg_config)?;
    let mut rng = rand::rng();

    let client = pool.get_connection().await?;
    let result = client
        .query("SELECT DISTINCT(shard) FROM materialize1.consensus", &[])
        .await?;
    let shards: Vec<_> = result
        .into_iter()
        .map(|row| row.get::<_, &str>(0).to_string())
        .collect();
    tracing::info!(num_shards = %shards.len(), "starting simulation");

    let mut tasks = Vec::with_capacity(shards.len());
    for shard in shards {
        let mut simulation = ShardSimulation::new(pool.clone(), &mut rng, shard).await?;
        let handle = tokio::spawn(async move {
            simulation.run().await;
        });
        tasks.push(handle);
    }

    let results = futures::future::join_all(tasks).await;
    println!("{results:?}");

    Ok(())
}

struct ShardSimulation {
    /// Connection pool to upstream postgres.
    client: PostgresClient,
    /// Shard we're running a simulation for.
    shard: String,
    /// Current sequence number we know about.
    max_seq_no: i64,
    /// Minimum sequence number we still maintain.
    min_seq_no: i64,
    /// Random number generator to drive some events.
    rng: SmallRng,

    /// The number of sequence number advances we've made since the last truncate.
    state_num_appends: u32,
    /// The number of truncates we've made.
    state_num_deletes: u32,
}

impl ShardSimulation {
    /// Get the current state of a shard to begin a simulation.
    pub async fn new(
        client: PostgresClient,
        rng: &mut impl Rng,
        shard: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let connection = client.get_connection().await?;
        let mut result = connection
            .query(
                "SELECT MAX(sequence_number), MIN(sequence_number) FROM materialize1.consensus WHERE shard = $1",
                &[&shard],
            )
            .await?;
        let row = result.pop().expect("at least one row");
        assert!(result.is_empty(), "at most one row");

        let max_seq_no = row.get(0);
        let min_seq_no = row.get(1);
        let rng = SmallRng::from_rng(rng);

        Ok(ShardSimulation {
            client,
            shard,
            max_seq_no,
            min_seq_no,
            rng,
            state_num_appends: 0,
            state_num_deletes: 0,
        })
    }

    pub async fn run(&mut self) {
        let mut num_failures = 1;
        loop {
            let result = if let Some(point) = self.should_truncate() {
                self.truncate(point).await
            } else {
                self.append().await
            };
            // Only print an error if we've failed many times in a row.
            if let Err(err) = result {
                num_failures += 1;
                if num_failures > 5 {
                    tracing::error!(?err, "something failed");
                }
            } else {
                num_failures = 1;
            }

            // Scale the sleep based on 
            let sleep_duration = self.sleep_duration() * num_failures;
            if num_failures > 5 {
                tracing::info!(?sleep_duration, shard = %self.shard, "sleeping");
            } else {
                tracing::debug!(?sleep_duration, shard = %self.shard, "sleeping");
            }

            tokio::time::sleep(sleep_duration).await;
        }
    }

    async fn append(&mut self) -> Result<(), anyhow::Error> {
        static APPEND_QUERY_A: &str =
            "INSERT INTO materialize1.consensus (shard, sequence_number, data)
        SELECT $1, $2, $3
        WHERE (SELECT sequence_number FROM materialize1.consensus
                WHERE shard = $1
                ORDER BY sequence_number DESC LIMIT 1) = $4;
        ";

        static APPEND_QUERY_B: &str = 
        "WITH last_seq AS (
            SELECT sequence_number
            FROM materialize1.consensus
            WHERE shard = $1
            ORDER BY sequence_number DESC
            LIMIT 1
            FOR UPDATE
        )
        INSERT INTO materialize1.consensus (shard, sequence_number, data)
        SELECT $1, $2, $3
        FROM last_seq
        WHERE last_seq.sequence_number = $4;
        ";

        let connection = self.client.get_connection().await?;
        let statement = connection.prepare_cached(APPEND_QUERY_B).await?;

        let expct_seq_no = self.max_seq_no;
        let next_seq_no = expct_seq_no.checked_add(1).unwrap();
        let data = self.random_state();

        let num_rows = connection
            .execute(
                &statement,
                &[&self.shard, &next_seq_no, &data, &expct_seq_no],
            )
            .await?;

        if num_rows == 1 {
            tracing::debug!(%next_seq_no, shard = %self.shard, "appended");
            self.max_seq_no = next_seq_no;
            self.state_num_appends += 1;
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "unexpected number of rows when appending {num_rows}"
            ))
        }
    }

    async fn truncate(&mut self, point: i64) -> Result<(), anyhow::Error> {
        static TRUNCATE_QUERY: &str = "DELETE FROM materialize1.consensus
        WHERE shard = $1 AND sequence_number < $2 AND
        EXISTS (
            SELECT * FROM materialize1.consensus WHERE shard = $1 AND sequence_number >= $2
        )";

        let connection = self.client.get_connection().await?;
        let statement = connection.prepare_cached(TRUNCATE_QUERY).await?;
        let num_rows = connection
            .execute(&statement, &[&self.shard, &point])
            .await?;
        tracing::info!(?num_rows, num_appends = %self.state_num_appends, shard = %self.shard, "truncated rows");
        self.min_seq_no = point;
        self.state_num_appends = 0;

        Ok(())
    }

    fn sleep_duration(&mut self) -> std::time::Duration {
        let normal = Normal::new(20.0, 5.0).unwrap();
        let sleep: f64 = self.rng.sample(normal);
        let sleep = sleep.abs().round() as u64;

        let sleep = sleep.max(1);
        let sleep = sleep.min(10_000);

        std::time::Duration::from_millis(sleep)
    }

    fn should_truncate(&mut self) -> Option<i64> {
        let prob = (self.state_num_appends as f64) / (MAX_APPENDS_BEFORE_TRUNCATE as f64);
        let prob = prob.min(1.0);

        if self.rng.random_bool(prob) {
            let truncate_point = (self.max_seq_no - self.min_seq_no) + self.min_seq_no;
            Some(truncate_point)
        } else {
            None
        }
    }

    fn random_state(&mut self) -> Vec<u8> {
        let state_size: u32 = self.rng.next_u32() % MAX_STATE_BYTES;
        let state_size: usize = state_size as usize;
        let mut state: Vec<u8> = vec![0; state_size];
        self.rng.fill_bytes(&mut state[..state_size]);
        state
    }
}
