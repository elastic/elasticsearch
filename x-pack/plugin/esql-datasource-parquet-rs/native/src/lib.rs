mod filter;
mod jni_utils;
mod metadata;
mod reader;
mod store;

use std::sync::LazyLock;
use tokio::runtime::Runtime;

static ASYNC_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(16);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("parquet-rs-io")
        .enable_all()
        .build()
        .expect("tokio runtime")
});
