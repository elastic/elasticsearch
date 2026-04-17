mod filter;
mod jni_utils;
mod metadata;
mod reader;

use std::sync::LazyLock;
use tokio::runtime::Runtime;

static ASYNC_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("tokio runtime"));
