# es-parquet-rs — Native Rust Library

This directory contains the Rust source for `libes_parquet_rs`, a shared library
that exposes Parquet file operations over the C ABI. Java calls into it via
Panama FFI through the bindings in `libs/native`.

## Prerequisites

- **Rust** — stable toolchain via [rustup](https://rustup.rs/)
- **Docker** — for cross-compilation (all three platform targets are built
  inside a Docker container)

## Rust source structure

```
src/
├── lib.rs          # Crate root — declares modules
├── ffi.rs          # FFI plumbing: thread-local error storage, ffi_try! macro
├── config.rs       # JSON config parsing (storage configuration)
└── metadata.rs     # Exported C ABI functions (pqrs_*)
```

## Conventions

### ABI prefix

All exported `extern "C"` functions use the `pqrs_` prefix (e.g.
`pqrs_get_statistics`, `pqrs_get_schema_ffi`, `pqrs_last_error`).

### Error handling

Rust errors are stored in a thread-local `LAST_ERROR` string (in `ffi.rs`).
Functions return a status code (0 = success, -1 = error). The Java side calls
`pqrs_last_error` to retrieve the message and throws an `IOException`.

The `ffi_try!` macro wraps this pattern — it evaluates an expression that
returns `Result`, and on error stores the message and returns -1.

### String parameters

Functions accept null-terminated C strings (`*const c_char`). On the Java side
this maps to `Arena.allocateFrom(string)` which produces a null-terminated
UTF-8 segment. Nullable parameters (like `configJson`) are passed as null
pointers when absent.

### Adding a new function

To expose a new Rust function to Java:

1. **Rust** — add the `extern "C"` function in `metadata.rs` (or a new module).
   Use `ffi_try!` for error handling. Follow the `pqrs_` naming convention.
2. **Java interface** — add the method signature to `ParquetRsLibrary.java`
   in `libs/native`.
3. **Panama implementation** — implement the method in `JdkParquetRsLibrary.java`
   using `MethodHandle` lookups against the native library symbols.
4. **Public facade** — expose the method through `ParquetRsFunctions.java`
   with error handling (check return code, call `lastError()`, throw
   `IOException`).
5. **Tests** — add tests in `ParquetRsFunctionsTests.java` (low-level FFI)
   and/or `ParquetRsNativeTests.java` (end-to-end with real Parquet files).
6. **Rebuild** — run `make install` to compile and install the updated library,
   then run the tests.

### Adding a new Rust dependency

Add the crate to `Cargo.toml`. The `Cargo.lock` is committed for reproducible
builds, so run `cargo update` or `cargo build` to regenerate it. After
publishing a new version of the native library, bump `pqrsVersion` in
`libs/native/libraries/build.gradle`.

## Building locally

```bash
make local      # compile for the current platform (release mode)
make install    # compile and copy to where Gradle tests expect it
```

`make install` places the library in
`libs/native/libraries/build/platform/<os>-<arch>/` so that Gradle tests can
use it instead of fetching from Artifactory. Set `LOCAL_PQRS_BINARY=true` to
skip the Artifactory download:

```bash
make install
LOCAL_PQRS_BINARY=true ./gradlew :libs:native:test --tests "org.elasticsearch.nativeaccess.ParquetRsFunctionsTests"
```

## Running tests

After `make install`:

```bash
# Low-level FFI tests
./gradlew :libs:native:test --tests "org.elasticsearch.nativeaccess.ParquetRsFunctionsTests"

# End-to-end integration tests
./gradlew :x-pack:plugin:esql-datasource-parquet-rs:test
```

## Cross-compilation

All three platform binaries are cross-compiled from Linux inside a Docker
container:

| Target              | Toolchain                                             |
|---------------------|-------------------------------------------------------|
| darwin-aarch64      | `rust-lld` + `-undefined dynamic_lookup` + TBD stubs  |
| linux-aarch64       | GCC cross-compiler (`aarch64-linux-gnu-gcc`)           |
| linux-x64           | GCC cross-compiler (`x86_64-linux-gnu-gcc`)            |

The Darwin target does not require the macOS SDK. Instead, the Makefile
generates minimal Apple TBD (text-based definition) stub files at build time
that satisfy the linker's library lookups, while `-undefined dynamic_lookup`
defers all symbol resolution to macOS `dyld` at load time.

### Build the Docker toolchain image

```bash
./build_rust_toolchain_image.sh --local    # local image for testing
./build_rust_toolchain_image.sh            # build and push to registry
```

### Build all platforms

```bash
# Inside Docker (what the publish script does)
docker run --rm -v "$(pwd)":/workspace -w /workspace \
  es-rust-cross-toolchain:local make all

# Or use make targets individually
make darwin     # cross-compile Darwin binary
make linux      # cross-compile both Linux binaries
make all        # all three
```

### Build and publish to Artifactory

```bash
# Set your API key
export ARTIFACTORY_API_KEY=...

# Build all platforms in Docker and upload
./publish_pqrs_binaries.sh

# Or build locally (skip upload)
./publish_pqrs_binaries.sh --local

# Build locally then upload
./publish_pqrs_binaries.sh --local --force-upload
```

Before publishing a new version, bump the version in all three places:

1. `native/Cargo.toml` — `version` field
2. `native/publish_pqrs_binaries.sh` — `VERSION` variable
3. `libs/native/libraries/build.gradle` — `pqrsVersion` variable

### Cleaning

```bash
make clean      # removes target/ and build/
```
