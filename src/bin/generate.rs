/*
cargo r --bin generate -- \
    --url postgres://<user>:<pass>@<host>:<port>/<dbname>
*/

use std::{str::FromStr, sync::Arc};

use clap::Parser;
use consensus_benchmarking::PostgresClient;
use rand::SeedableRng;
use tokio_postgres::config::SslMode;

const MAX_ENTRIES_PER_SHARD: u32 = 4000;
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut pg_config = tokio_postgres::Config::from_str(&args.url)?;
    if !args.notls {
        pg_config.ssl_mode(SslMode::Require);
    }

    let pool = Arc::new(PostgresClient::open(pg_config)?);
    let mut rng = rand::rng();
    let shards = (0..200).map(|_| uuid::Uuid::new_v4());
    let mut tasks = Vec::default();
    let limiter = Arc::new(tokio::sync::Semaphore::new(49));

    for shard in shards {
        let pool_ = pool.clone();
        let limiter_ = limiter.clone();
        let mut rng = rand::rngs::SmallRng::from_rng(&mut rng);

        let handle = tokio::spawn(async move {
            let _permit = tokio::sync::Semaphore::acquire_owned(limiter_)
                .await
                .map_err(|e| format!("{e}"))?;

            println!("processing {shard}");
            let connection = pool_
                .get_connection(Some("generate".into()))
                .await
                .map_err(|e| format!("{e}"))?;
            let shard_entries = generate_data_for_shard(shard, &mut rng);

            for (shard_id, seq_no, data) in shard_entries {
                let shard_id = format!("s{shard_id}");
                connection
                    .execute(
                        "INSERT INTO materialize1.consensus VALUES ($1, $2, $3)",
                        &[&shard_id, &seq_no, &data],
                    )
                    .await
                    .map_err(|err| format!("{err}"))?;
            }

            Ok::<_, String>(())
        });
        tasks.push(handle);
    }

    let results = futures::future::join_all(tasks).await;
    println!("{results:?}");

    Ok(())
}

/// Given a [`uuid::Uuid`] that represent a shard, return an iterator that
/// yields mock state updates.
fn generate_data_for_shard(
    shard_id: uuid::Uuid,
    rng: &mut impl rand::Rng,
) -> impl Iterator<Item = (uuid::Uuid, i64, Vec<u8>)> {
    let num_entries: u32 = rng.next_u32() % MAX_ENTRIES_PER_SHARD;

    (0..num_entries).map(move |seq_no| {
        let state_size: u32 = rng.next_u32() % MAX_STATE_BYTES;
        let state_size: usize = state_size as usize;
        let mut state: Vec<u8> = vec![0; state_size];
        rng.fill_bytes(&mut state[..state_size]);

        let uuid = shard_id.clone();
        let seq_no: i64 = seq_no.into();

        (uuid, seq_no, state)
    })
}
