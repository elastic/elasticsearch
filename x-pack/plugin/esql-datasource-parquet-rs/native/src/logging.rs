//! Logging setup for the native parquet-rs bridge.
//!
//! We use the `log` crate facade with `env_logger` as the backend. The default
//! level is `warn`; raise it for triage by setting the `RUST_LOG` environment
//! variable in the JVM environment before starting Elasticsearch, e.g.:
//!
//! ```text
//! RUST_LOG=esql_parquet_rs=debug
//! RUST_LOG=esql_parquet_rs::pruning=trace,esql_parquet_rs=info
//! ```
//!
//! Output goes to stderr, which Elasticsearch captures into its log pipeline.
//!
//! # Performance
//!
//! `log::debug!` / `log::trace!` expand to a `STATIC_MAX_LEVEL` const check
//! plus an atomic relaxed load on the global level filter. When the level is
//! below threshold the format args are never built and no allocation happens.
//!
//! `trace!` is compiled out entirely in release builds via the
//! `release_max_level_debug` feature on the `log` crate (see Cargo.toml), so
//! trace sites have literally zero codegen in production. Use `trace!` for
//! per-row-group / per-page hot-path diagnostics, `debug!` for per-file or
//! per-query summaries, `info!`/`warn!`/`error!` for events worth keeping on
//! by default.
//!
//! # Targets
//!
//! All log sites in this crate use targets under the `esql_parquet_rs::*`
//! namespace so users can filter by subsystem:
//!
//! - `esql_parquet_rs::pruning`  — row-group + page-index pruning decisions
//! - `esql_parquet_rs::reader`   — reader builder setup, projection, splits
//! - `esql_parquet_rs::filter`   — row filter evaluation
//! - `esql_parquet_rs::store`    — object-store / IO

use std::sync::Once;

static INIT: Once = Once::new();

/// Initialize the logging backend exactly once. Safe to call from multiple
/// threads and from any number of JNI entry points.
pub fn init() {
    INIT.call_once(|| {
        let _ = env_logger::Builder::from_env(
            env_logger::Env::default().default_filter_or("warn"),
        )
        .format_timestamp_micros()
        .format_module_path(true)
        .try_init();
    });
}
