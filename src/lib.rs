use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use deadpool_postgres::{
    Hook, HookError, ManagerConfig, Object, Pool, PoolError, RecyclingMethod, Runtime,
};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;

const DEFAULT_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const DEFAULT_MAX_SIZE: usize = 120;

static COUNTER: AtomicUsize = AtomicUsize::new(0);
static TOTAL_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct PostgresClient {
    conn_pool: Pool,
}

impl PostgresClient {
    pub fn open(pg_config: tokio_postgres::Config) -> Result<Self, Box<dyn std::error::Error>> {
        let pool_config = ManagerConfig {
            recycling_method: RecyclingMethod::Clean,
        };

        let connector = TlsConnector::builder()
            .danger_accept_invalid_hostnames(true)
            .danger_accept_invalid_certs(true)
            .build()?;
        let tls = MakeTlsConnector::new(connector);
        let manager = deadpool_postgres::Manager::from_config(pg_config, tls, pool_config);

        let conn_pool = Pool::builder(manager)
            .wait_timeout(Some(DEFAULT_WAIT_TIMEOUT))
            .runtime(Runtime::Tokio1)
            .max_size(DEFAULT_MAX_SIZE)
            .post_create(Hook::async_fn(move |client, _| {
                Box::pin(async move {
                    tracing::warn!("creating connection");
                    client.batch_execute(
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                ).await.map_err(|e| HookError::Backend(e))
                })
            }))
            .build()
            .expect("failed to create pool");

        Ok(PostgresClient { conn_pool })
    }

    pub async fn get_connection(&self) -> Result<ConnectionWrapper, PoolError> {
        let conn = self.conn_pool.get().await?;
        Ok(ConnectionWrapper::new(conn))
    }
}

pub struct ConnectionWrapper {
    conn: Object,
    idx: usize,
}

impl ConnectionWrapper {
    pub fn new(obj: Object) -> ConnectionWrapper {
        let idx = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let total = TOTAL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // tracing::info!(%idx, %total, "returning new connection");
        ConnectionWrapper { conn: obj, idx }
    }
}

impl Deref for ConnectionWrapper {
    type Target = Object;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl Drop for ConnectionWrapper {
    fn drop(&mut self) {
        let total = TOTAL_COUNT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        // tracing::info!(idx = %self.idx, %total, "dropping connection");
    }
}
