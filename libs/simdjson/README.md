# simdjson — Native JSON Parsing for ESCF

`libs/simdjson` provides native-accelerated JSON parsing for Elasticsearch columnar
source encoding (ESCF). Stage 1 structural indexing runs in `libsimdjson` (SIMD-backed
C++ from the [simdjson](https://github.com/simdjson/simdjson) project). Stage 2 is
fused with token walking via `SimdJsonDirectWalker` — no intermediate DOM or tape —
streaming field events straight to a `JsonDocumentHandler`.

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
│           ├── SimdJsonParserPool.java   # Public entry point (thread-local parsers)
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
  - `org.elasticsearch.escf.SimdJsonPool` — feature flag, document size limits, pool wiring
  - `org.elasticsearch.escf.EscfDocumentHandler` — `JsonDocumentHandler` implementation
  - `org.elasticsearch.escf.EscfEncoder` — simdjson vs Jackson encode path
- **`libs/native/libraries`** — downloads `org.elasticsearch:simdjson` native zips at build time
- **`benchmarks`** — `SimdJsonParserBenchmark` JMH harness

## Parsing pipeline

1. **`SimdJsonParser.stage1`** — native structural indexing; writes byte offsets of
   structural characters and value starts into `BitIndexes`.
2. **`SimdJsonParser.prepareDocumentWindow`** — slices the index to one document
   (supports chunked batches via `beginBatch` / `prepareDocumentWindowChunked`).
3. **`SimdJsonDirectWalker.walkDocument`** — walks the index, resolves field names
   through the frozen name table, parses strings/numbers inline, emits handler events.
4. **`SimdJsonParserPool.releaseNames`** — at batch boundaries, merges newly learned
   field names back to the shared parent table.

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
make install     # copies the binary where Gradle tests expect it
```

`make install` places the library in
`libs/native/libraries/build/platform/<os>-<arch>/` so that Gradle tests can use it
instead of fetching from Artifactory. Set `LOCAL_SIMDJSON_BINARY=1` to skip the
Artifactory download:

```bash
make install
LOCAL_SIMDJSON_BINARY=1 ./gradlew :libs:simdjson:test
```

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

**Local native library.** When changing `native/`, build and install before testing
(see [Building the native library](#building-the-native-library)):

```bash
cd native && make install
LOCAL_SIMDJSON_BINARY=1 ./gradlew :libs:simdjson:test
```

Use `--rerun-tasks` to force Gradle to re-execute the test task (for example after
a prior successful run with the same arguments). The Elasticsearch-specific
`-Dtests.timestamp=$(date +%s)` property is only needed when re-running with
*identical* JVM args and bypassing per-seed result caching; see CONTRIBUTING.md.
