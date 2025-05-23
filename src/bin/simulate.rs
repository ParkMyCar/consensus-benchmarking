/*
cargo r --bin simulate -- \
    --url postgres://<user>:<pass>@<host>:<port>/<dbname> --iterations 100
*/

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use consensus_benchmarking::PostgresClient;
use hdrhistogram::{Histogram, RecordError};
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use rand_distr::Normal;
use tokio::sync::RwLock;
use tokio_postgres::config::SslMode;
use tracing_subscriber::EnvFilter;

const MAX_APPENDS_BEFORE_TRUNCATE: u32 = 1000;
const MAX_STATE_BYTES: u32 = 4000;

/// Program to generate some mock data that mimics consensus.
#[derive(clap::Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Url of the database, e.g. postgres://<user>:<password>@<host>:<port>/<dbname>.
    #[arg(long)]
    url: String,
    /// Do not use TLS for DB connection.
    #[arg(long)]
    notls: bool,
    /// Seed to use for the rng to generate data.
    #[arg(long)]
    seed: Option<String>,
    /// Number of iterations to run.
    #[arg(long, default_value_t = 10)]
    iterations: u32,
}

struct Metrics {
    histograms: HashMap<String, RwLock<Histogram<u64>>>,
}
impl Metrics {
    pub fn new<T: IntoIterator<Item = String>>(operations: T) -> Self {
        Metrics {
            histograms: HashMap::from_iter(operations.into_iter().map(|key| {
                (
                    key,
                    RwLock::new(Histogram::<u64>::new_with_bounds(1, 3_600_000, 2).unwrap()),
                )
            })),
        }
    }

    pub async fn record_measure(&self, op: &str, value: u64) -> Result<(), RecordError> {
        self.histograms.get(op).unwrap().write().await.record(value)
    }

    pub async fn get_map(&self) -> HashMap<String, Histogram<u64>> {
        let mut res = HashMap::with_capacity(self.histograms.capacity());
        for (op, locker) in self.histograms.iter() {
            let hist = locker.read().await.clone();
            res.insert(op.clone(), hist);
        }
        res
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // console_subscriber::init();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    let mut pg_config = tokio_postgres::Config::from_str(&args.url)?;
    if !args.notls {
        pg_config.ssl_mode(SslMode::Require);
    }

    let pool = PostgresClient::open(pg_config)?;
    let mut rng = rand::rng();

    let client = pool.get_connection(Some("start".into())).await?;
    let result = client
        .query("SELECT DISTINCT(shard) FROM materialize1.consensus", &[])
        .await?;
    let shards: Vec<_> = result
        .into_iter()
        .map(|row| row.get::<_, &str>(0).to_string())
        .collect();
    tracing::info!(num_shards = %shards.len(), "starting simulation");

    let metrics = Arc::new(Metrics::new(["append".into(), "truncate".into()]));
    let mut tasks = Vec::with_capacity(shards.len());
    drop(client);
    for shard in shards {
        let mut simulation = ShardSimulation::new(
            pool.clone(),
            &mut rng,
            shard,
            metrics.clone(),
            args.iterations,
        )
        .await?;
        let handle = tokio::spawn(async move {
            simulation.run().await;
        });
        tasks.push(handle);
    }

    let results = futures::future::join_all(tasks).await;
    println!("{results:?}");
    for (op, histogram) in metrics.get_map().await.iter() {
        println!("\n=== operation: {op}");
        for q in [0.5, 0.95, 0.99, 1.0] {
            let quantile_value = histogram.value_at_quantile(q);
            println!(
                "{}'th percentile: {} milliseconds {} samples",
                q * 100.0,
                quantile_value,
                histogram.count_at(quantile_value),
            );
        }
    }
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
    /// A place to record operational latency.
    metrics: Arc<Metrics>,
    /// The number of iterations to execute.
    iterations: u32,
}

impl ShardSimulation {
    /// Get the current state of a shard to begin a simulation.
    pub async fn new(
        client: PostgresClient,
        rng: &mut impl Rng,
        shard: String,
        metrics: Arc<Metrics>,
        iterations: u32,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let connection = client
            .get_connection(Some(format!("new simulation {shard}")))
            .await?;
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
            metrics,
            iterations,
        })
    }

    pub async fn run(&mut self) {
        let mut num_failures = 1;
        for _ in 0..self.iterations {
            tracing::debug!(shard = %self.shard, "running iteration");
            let result = if let Some(point) = self.should_truncate() {
                self.truncate(point).await
            } else {
                self.append().await
            };
            tracing::debug!(shard = %self.shard, "finish iteration");
            // Only print an error if we've failed many times in a row.
            if let Err(err) = result {
                if !err.to_string().contains("had to update") {
                    num_failures += 1;
                    if num_failures > 3 {
                        tracing::error!(?err, "something failed");
                    }
                }
            } else {
                num_failures = 1;
            }

            // Scale the sleep based on
            let sleep_duration = self.sleep_duration() * num_failures;
            let sleep_duration = sleep_duration.min(Duration::from_secs(16));
            if sleep_duration > std::time::Duration::from_secs(5) {
                tracing::info!(?sleep_duration, shard = %self.shard, "sleeping");
            } else {
                tracing::debug!(?sleep_duration, shard = %self.shard, "sleeping");
            }

            tokio::time::sleep(sleep_duration).await;
        }
    }

    async fn update_head(&mut self) -> Result<(), anyhow::Error> {
        static HEAD_QUERY: &str = "SELECT sequence_number, data FROM materialize1.consensus
             WHERE shard = $1 ORDER BY sequence_number DESC LIMIT 1";

        tracing::debug!(shard = %self.shard, "starting update head");
        let connection = self
            .client
            .get_connection(Some(format!("update head {}", self.shard)))
            .await?;
        let statement = connection.prepare_cached(HEAD_QUERY).await?;
        let mut result = connection.query(&statement, &[&self.shard]).await?;
        tracing::debug!(shard = %self.shard, "finished update head");
        let row = result.pop().expect("at least 1");
        assert!(result.is_empty(), "at most 1");

        let max_seq_no = row.get("sequence_number");
        self.max_seq_no = max_seq_no;

        Ok(())
    }

    async fn append(&mut self) -> Result<(), anyhow::Error> {
        static APPEND_QUERY_A: &str =
            "INSERT INTO materialize1.consensus (shard, sequence_number, data)
        SELECT $1, $2, $3
        WHERE (SELECT sequence_number FROM materialize1.consensus
                WHERE shard = $1
                ORDER BY sequence_number DESC LIMIT 1) = $4;
        ";

        static APPEND_QUERY_B: &str = "WITH last_seq AS (
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

        tracing::debug!(shard = %self.shard, "starting append");
        let connection = self
            .client
            .get_connection(Some(format!("appen {}", self.shard)))
            .await?;
        let expct_seq_no = self.max_seq_no;
        let next_seq_no = expct_seq_no.checked_add(1).unwrap();
        let data = vec![42u8; 8];

        let start = Instant::now();
        let statement = connection.prepare_cached(APPEND_QUERY_B).await?;
        let num_rows = connection
            .execute(
                &statement,
                &[&self.shard, &next_seq_no, &data, &expct_seq_no],
            )
            .await?;
        tracing::debug!(shard = %self.shard, "finished append");
        self.metrics
            .record_measure("append", start.elapsed().as_millis() as u64)
            .await?;

        if num_rows == 0 {
            tracing::debug!(%expct_seq_no, shard = %self.shard, "wrong expected sequence number");
            drop(connection);
            self.update_head().await?;
            Err(anyhow::anyhow!("had to update sequence number"))
        } else if num_rows == 1 {
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
        static TRUNCATE_QUERY_A: &str = "DELETE FROM materialize1.consensus
        WHERE shard = $1 AND sequence_number < $2 AND
        EXISTS (
            SELECT * FROM materialize1.consensus WHERE shard = $1 AND sequence_number >= $2
        )";

        static TRUNCATE_QUERY_B: &str = "
        WITH newer_exists AS (
            SELECT * FROM materialize1.consensus
            WHERE shard = $1
                AND sequence_number >= $2
            ORDER BY sequence_number ASC
            LIMIT 1
            FOR UPDATE
        ),
        to_lock AS (
            SELECT ctid FROM materialize1.consensus
            WHERE shard = $1
            AND sequence_number < $2
            AND EXISTS (SELECT * FROM newer_exists)
            ORDER BY sequence_number DESC
            FOR UPDATE
        )
        DELETE FROM materialize1.consensus
        USING to_lock
        WHERE materialize1.consensus.ctid = to_lock.ctid;
        ";

        tracing::debug!(shard = %self.shard, "starting truncate");
        let connection = self
            .client
            .get_connection(Some(format!("truncate {}", self.shard)))
            .await?;
        let start = Instant::now();
        let statement = connection.prepare_cached(TRUNCATE_QUERY_B).await?;
        let num_rows = connection
            .execute(&statement, &[&self.shard, &point])
            .await?;
        tracing::debug!(shard = %self.shard, "finished truncate");
        self.metrics
            .record_measure("truncate", start.elapsed().as_millis() as u64)
            .await?;
        tracing::info!(?num_rows, num_appends = %self.state_num_appends, shard = %self.shard, "truncated rows");
        self.min_seq_no = point;
        self.state_num_appends = 0;

        Ok(())
    }

    fn sleep_duration(&mut self) -> std::time::Duration {
        let normal = Normal::new(70.0, 20.0).unwrap();
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
