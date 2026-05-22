# Native Parquet reader library.

## Building

The multi-platform libraries are **not built by default**. Build them with
`./build-libraries.sh` and commit the resulting files. This is a temporary
workaround until we have a better artifact management system.

## Local development

If you have a Rust toolchain installed (i.e. `cargo` exists), the build will
automatically pick up the local version of the libraries.
