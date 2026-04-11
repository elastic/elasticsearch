use std::sync::LazyLock;

mod filter;
mod jni_utils;
mod metadata;
mod reader;

static ASYNC_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});
