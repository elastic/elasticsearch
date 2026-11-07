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
    ├── build_zstd.sh
    └── zstd-inputs.sh
```

Applying `elasticsearch.native-library-build` makes `libs:zstd` a native
library producer. By default it downloads a published artifact addressed by the
hash of its pinned source and inputs. The native-library collector
aggregates that output with the other native libraries.

## Building libzstd locally

```bash
ZSTD_NATIVE_BUILD=host ./gradlew :libs:zstd:test
```

Host mode builds libzstd from source on Linux and stages the pinned upstream
binary for the current macOS or Windows platform. To build every supported
platform in the cross-compilation toolchain, use
`ZSTD_NATIVE_BUILD=docker`.

## Testing

```bash
./gradlew :libs:zstd:test
```
