mod cache;
mod filter;
mod jni_utils;
mod metadata;
mod reader;
mod store;

use std::sync::LazyLock;
use tokio::runtime::Runtime;

static ASYNC_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("parquet-rs-io")
        .enable_all()
        .build()
        .expect("tokio runtime")
});
