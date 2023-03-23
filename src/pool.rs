use std::{
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use concurrent_queue::ConcurrentQueue;

/// A pool of SQLite connections
#[derive(Clone)]
pub struct Pool {
    queue: Arc<ConcurrentQueue<rusqlite::Connection>>,
    path: PathBuf,
}

impl Pool {
    /// Opens a new pool of SQLite connections.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let path: PathBuf = path.as_ref().to_owned();
        let flags: rusqlite::OpenFlags = rusqlite::OpenFlags::default();
        let db = rusqlite::Connection::open_with_flags(&path, flags)?;
        db.query_row("PRAGMA journal_mode = WAL;", [], |f| f.get::<_, String>(0))?;
        db.execute("PRAGMA synchronous = NORMAL;", [])?;
        let queue = Arc::new(ConcurrentQueue::unbounded());
        queue.push(db).unwrap();

        let toret = Self { queue, path };
        {
            loop {
                let toret = toret.clone();
                match std::thread::Builder::new()
                    .name("blkidx-optimize".into())
                    .spawn(move || loop {
                        log::info!("optimizing database...");
                        let start = Instant::now();
                        let conn = toret.get_conn();
                        let _ = conn
                            .execute("PRAGMA optimize;", [])
                            .map_err(|e| format!("error while optimizing database {:?}", e));
                        log::info!("optimized in {:?}", start.elapsed());
                        std::thread::sleep(Duration::from_secs(3600));
                    }) {
                    Ok(_) => {
                        log::info!("successfully optimized database");
                        break;
                    }
                    Err(e) => log::warn!("failed to optimize database, trying again: {:?}", e),
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        Ok(toret)
    }

    /// Obtains one connection from the pool.
    pub fn get_conn(&self) -> impl DerefMut<Target = rusqlite::Connection> {
        let conn = self.queue.pop().unwrap_or_else(|_| {
            let flags: rusqlite::OpenFlags = rusqlite::OpenFlags::default();
            let db = rusqlite::Connection::open_with_flags(&self.path, flags).unwrap();
            db.query_row("PRAGMA journal_mode = WAL;", [], |f| f.get::<_, String>(0))
                .unwrap();
            db.execute("PRAGMA synchronous = NORMAL;", []).unwrap();
            db
        });
        WrappedConnection {
            queue: self.queue.clone(),
            inner: Some(conn),
        }
    }
}

struct WrappedConnection {
    queue: Arc<ConcurrentQueue<rusqlite::Connection>>,
    inner: Option<rusqlite::Connection>,
}

impl Drop for WrappedConnection {
    fn drop(&mut self) {
        let _ = self.queue.push(self.inner.take().unwrap());
    }
}

impl Deref for WrappedConnection {
    type Target = rusqlite::Connection;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl DerefMut for WrappedConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}
