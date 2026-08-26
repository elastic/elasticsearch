mod cache;
mod coalescing;
mod filter;
mod jni_utils;
mod logging;
mod metadata;
mod reader;
mod store;

use std::sync::LazyLock;
use tokio::runtime::Runtime;

// Piggy-back on the tokio runtime LazyLock: it is touched by every async JNI
// entry point exactly once, before any work begins, so logging is initialized
// before any log macro could fire from within this crate. Sync-only entry
// points still call `logging::init()` explicitly.
static ASYNC_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    logging::init();
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("parquet-rs-io")
        .enable_all()
        .build()
        .expect("tokio runtime")
});
