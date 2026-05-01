# parquet-rs — Rust Native Parquet Library for Elasticsearch

`libs/parquet-rs` provides Parquet file operations backed by a Rust native
library (`libes_parquet_rs`), loaded at runtime via Panama FFI. Higher-level
Java abstractions are built on top of the low-level FFI bindings in `libs/native`.

## Layout

```
libs/parquet-rs/
├── src/main/java/              # Java module (org.elasticsearch.parquetrs)
│   └── ParquetRs.java          #   Entry point — delegates to libs/native
├── native/                     # Rust native library (libes_parquet_rs)
│   ├── src/                    #   Rust source (see native/README.md)
│   ├── Cargo.toml / Cargo.lock #   Rust dependencies (pinned for reproducibility)
│   ├── Makefile                #   Cross-compilation build (all platforms)
│   ├── Dockerfile.rust-toolchain
│   └── publish_pqrs_binaries.sh
└── build.gradle
```

### Related code in other modules

- **`libs/native`** — Low-level Panama FFI bindings
  - `ParquetRsLibrary.java` — interface declaring native function signatures
  - `JdkParquetRsLibrary.java` — Panama implementation, loads `libes_parquet_rs`
  - `ParquetRsFunctions.java` — public facade with error handling (throws `IOException`)
  - `ParquetRsFunctionsTests.java` — unit tests for the FFI functions
- **`x-pack/plugin/esql-datasource-parquet-rs`** — ESQL plugin consumer
  - `ParquetRsNativeTests.java` — end-to-end integration tests with real Parquet files

## Building and testing

See [`native/README.md`](native/README.md) for Rust build instructions,
cross-compilation, and the publish workflow.

### Running tests

```bash
# Low-level FFI tests (libs/native)
./gradlew :libs:native:test --tests "org.elasticsearch.nativeaccess.ParquetRsFunctionsTests"

# End-to-end integration tests (plugin module)
./gradlew :x-pack:plugin:esql-datasource-parquet-rs:test
```

Both require the native library to be available — either from Artifactory
(automatic) or built locally via `make install` in `native/`. When using a
locally built library, set `LOCAL_PQRS_BINARY=true` to skip the Artifactory
download:

```bash
LOCAL_PQRS_BINARY=true ./gradlew :libs:native:test --tests "org.elasticsearch.nativeaccess.ParquetRsFunctionsTests"
```
