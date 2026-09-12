# zstd — Native Zstandard Compression for Elasticsearch

`libs/zstd` provides Elasticsearch's zstd (Zstandard) compression support. It
owns the entire zstd surface — the FFI binding to the native `libzstd`
library, the JDK-21 `@Critical` fallback, and the public `Zstd` Java wrapper —
behind a single entry point, `Zstd.instance()`. The native FFI binding
(`ZstdLibrary`) is package-private and not visible to consumers.

## Layout

```
libs/zstd/
├── src/
│   ├── main/java/          # Java module (org.elasticsearch.zstd)
│   │                       #   Zstd.java          - public wrapper + Zstd.instance()
│   │                       #   ZstdLibrary.java    - package-private @LibrarySpecification
│   │                       #   ZstdHeapFallback.java - package-private @Critical fallback
│   └── test/               # Unit tests
└── native/                 # Build tooling for the prebuilt libzstd binaries
    ├── publish_zstd_binaries.sh
    └── zstd.Dockerfile
```

The prebuilt `libzstd` artifacts themselves are still resolved centrally by
`:libs:native:native-libraries` (`libs/native/libraries/build.gradle`), which
aggregates zstd, simdvec, and parquet-rs native libraries into the single
directory that the native-library-path mechanism expects.

## Building libzstd locally

```bash
cd libs/zstd/native
./publish_zstd_binaries.sh --local-only
cd ../../..
LOCAL_ZSTD_BINARY=1 ./gradlew :libs:zstd:test
```

## Testing

```bash
./gradlew :libs:zstd:test
```
