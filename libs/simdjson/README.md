# simdjson — Native JSON Parsing for ESCF

`libs/simdjson` provides native-accelerated JSON parsing for Elasticsearch columnar
source encoding (ESCF). Stage 1 structural indexing runs in `libsimdjson` (SIMD-backed
C++ from the [simdjson](https://github.com/simdjson/simdjson) project). Stage 2 is
fused with token walking via `SimdJsonDirectWalker` — no intermediate DOM or tape —
streaming field events straight to a `JsonDocumentHandler`.

Upstream simdjson uses **stage 1** (structural indexing: byte offsets of `{ } [ ] : ,`
and value starts) and **stage 2** (tape construction and value materialization). This
module runs native stage 1 only; see
[simdjson implementation notes](https://github.com/simdjson/simdjson/blob/master/doc/implementation.md).

Scalar and string parsing utilities are vendored from
[simdjson-java](https://github.com/simdjson/simdjson-java) under
`internal.parsers`. Elasticsearch-specific integration (native stage 1, field-name
cache, direct walker) lives in the exported API and sibling `internal` packages.

## Layout

```
libs/simdjson/
├── src/                              # Java module (org.elasticsearch.simdjson)
│   └── main/java/
│       ├── module-info.java          #   Exports org.elasticsearch.simdjson only
│       └── org/elasticsearch/simdjson/
│           ├── SimdJsonParserPool.java   # Public entry point (thread-local document parsers)
│           ├── JsonDocumentParser.java   # Single-document parser (stage 1 indexer + walker)
│           ├── SimdJsonParser.java       # Stage 1 + per-document index windows
│           ├── SimdJsonDirectWalker.java # Fused stage 2 / token walk
│           ├── JsonDocumentHandler.java  # Callback API for field events
│           └── internal/
│               ├── StructuralIndexer.java    # Native stage 1 wrapper
│               ├── SimdJsonLibrary.java      # FFM binding to libsimdjson
│               ├── parsers/                  # Vendored from simdjson-java
│               └── fieldnames/               # Per-batch field name cache
├── native/                           # Native C++ library (libsimdjson)
│   ├── src/
│   │   ├── es_simdjson.cpp           #   Elasticsearch stage 1 FFI surface
│   │   ├── simdjson.cpp              #   Vendored simdjson amalgamation
│   │   └── simdjson.h
│   ├── Makefile                      #   Cross-compilation build (all platforms)
│   └── publish_simdjson_binaries.sh  #   Build + Artifactory upload
├── licenses/                         # Vendored simdjson C++ notices
└── build.gradle
```

## Related code in other modules

- **`server`** — ESCF encoding integration
  - `org.elasticsearch.escf.EscfDocumentHandler` — `JsonDocumentHandler` implementation
  - `org.elasticsearch.escf.EscfEncoder` — feature flag, simdjson vs Jackson encode path; resolves
    its thread's `JsonDocumentParser` once at construction
- **`libs/native/libraries`** — downloads `org.elasticsearch:libsimdjson` native zips at
     build time.
- **`benchmarks`** — `SimdJsonParserBenchmark` JMH harness

## Parsing pipeline

1. **`SimdJsonParserPool.forCurrentThread`** — returns this thread's `JsonDocumentParser`, creating
   it on first call. Parsers are keyed by thread, so the number of native contexts is bounded by
   the number of threads that ever parse, not by how many units of work are in flight.
2. **`JsonDocumentParser.parseDocument`** — indexes one document (native stage 1) and walks it in
   a single call.
3. **`SimdJsonParser.stage1`** / **`prepareDocumentWindow`** and
   **`SimdJsonDirectWalker.walkDocument`** — the lower-level steps `parseDocument` sequences;
   stage 2 resolves field names, parses strings/numbers inline, and emits handler events. Used
   directly in tests and for multi-document batches.
4. **`JsonDocumentParser.publishFieldNames`** — at batch boundaries, merges newly learned field
   names into the shared table so other threads can reuse them.

For a **multi-document batch** (concatenated JSON in one buffer), `beginBatch` /
`prepareDocumentWindowChunked` run stage 1 lazily in **buffer chunks** of at most
`CHUNK_BYTE_LIMIT` bytes (default 256 KB). ESCF today uses the single-document path only.

Requires `SimdJsonSupport.isSupported()` (native library loaded and vector API
available). Windows x64 and Intel macOS are excluded at the FFM binding layer.

## Building the native library

The native library is built via the `Makefile` in `native/`. For cross-compilation
of all three platform binaries (darwin-aarch64, linux-aarch64, linux-x64), use the
shared Docker-based toolchain image (`es-native-cross-toolchain`, shared with
`libs/simdvec`):

```bash
# Build the cross-compilation toolchain image (from libs/simdvec/native)
../../simdvec/native/build_cross_toolchain_image.sh

# Build and publish binaries
./publish_simdjson_binaries.sh
```

For local development on the current platform:

```bash
cd native
make local       # builds for the host platform
```

Set `SIMDJSON_NATIVE_BUILD=host` to build from source via Gradle instead of
fetching from Artifactory:

```bash
SIMDJSON_NATIVE_BUILD=host ./gradlew :libs:simdjson:test
```

Or cross-compile every platform inside the toolchain container:

```bash
SIMDJSON_NATIVE_BUILD=docker ./gradlew :libs:simdjson:buildNativeLibrary
```

When `SIMDJSON_NATIVE_BUILD` is unset (or set to `artifactory`), the binary is
resolved from Artifactory like other elasticsearch-native artifacts.

`make install` (without Gradle) still copies a locally built library into
`libs/native/libraries/build/platform/<os>-<arch>/` for ad-hoc workflows.

Inside the cross-compilation container, run `make verify-linux-abi` after `make all`
to confirm Linux `.so` files meet the RHEL 8 baseline (GLIBCXX ≤ 3.4.25, GLIBC ≤ 2.28).

## Testing

```bash
# Run simdjson tests (from repo root)
./gradlew :libs:simdjson:test

# ESCF integration tests that exercise the simdjson encode path
./gradlew :server:test --tests org.elasticsearch.escf.EscfEncoderSimdJsonTests
```

To fully exercise this module, run the unit suite under the JDK versions and vector
widths the code supports, and use a local native binary when iterating on C++ changes.

**JDK 21 runtime.** Stage 1 downcalls use `@Critical` with a heap-segment fallback on
JDK 21. Verify that path explicitly:

```bash
./gradlew :libs:simdjson:test -Druntime.java=21
```

**Vector API bit widths.** `StringParser` selects its species from
`-Dtests.vectorsize` (see `SimdJsonVectorSupport`). Run all three fixed widths so
the vector and scalar tail paths are covered on every platform:

```bash
for width in 128 256 512; do
  ./gradlew :libs:simdjson:test -Dtests.vectorsize=$width
done
```

**Local native library.** When changing `native/`, build from source before testing
(see [Building the native library](#building-the-native-library)):

```bash
SIMDJSON_NATIVE_BUILD=host ./gradlew :libs:simdjson:test
```

Use `--rerun-tasks` to force Gradle to re-execute the test task (for example after
a prior successful run with the same arguments). The Elasticsearch-specific
`-Dtests.timestamp=$(date +%s)` property is only needed when re-running with
*identical* JVM args and bypassing per-seed result caching; see CONTRIBUTING.md.
