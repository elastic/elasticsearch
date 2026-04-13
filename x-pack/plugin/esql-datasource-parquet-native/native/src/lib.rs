use std::sync::{Arc, LazyLock, Mutex};

mod filter;
mod jni_utils;
mod metadata;
mod objstore;
mod reader;

static ASYNC_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

/// Shared RuntimeEnv so all SessionContexts reuse a single instance instead of
/// each creating (and leaking) their own tokio runtime.
static SHARED_RUNTIME_ENV: LazyLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>> =
    LazyLock::new(|| {
        datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .build_arc()
            .expect("Failed to create shared RuntimeEnv")
    });

/// Serializes SessionContext construction to prevent concurrent access to
/// DataFusion's non-thread-safe initialization code (built-in function
/// registration, physical optimizer setup, etc.).
static SESSION_BUILD_LOCK: Mutex<()> = Mutex::new(());
