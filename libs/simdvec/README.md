# simdvec — Native SIMD Vector Scoring for Elasticsearch

`libs/simdvec` provides optimized vector distance and scoring kernels used by
Elasticsearch's vector search (kNN, BBQ, scalar quantization). It contains both
Java-side Panama SIMD code and a native C++ library (`libvec`) with hand-tuned
SIMD kernels, loaded at runtime via FFI.

## Layout

```
libs/simdvec/
├── src/                    # Java module (org.elasticsearch.simdvec)
│   ├── main/java/          #   Public API, scorer suppliers, and Panama SIMD paths
│   ├── test/               #   Unit and scorer-level tests
│   └── testFixtures/       #   Shared test utilities
├── native/                 # Native C++ library (libvec)
│   ├── src/vec/c/
│   │   ├── aarch64/        #     ARM kernels (NEON baseline, SVE in *_2.cpp)
│   │   └── amd64/          #     x64 kernels (AVX2 baseline, AVX-512 in *_2.cpp)
│   ├── src/vec/headers/    #     Shared and platform-specific headers
│   ├── Makefile            #     Cross-compilation build (all platforms)
│   └── Dockerfile.cross-toolchain
└── build.gradle            # Gradle build config (multi-release JAR, JDK 21 coverage)
```

### Related code in other modules

- **`libs/native`** — Low-level Panama FFI bindings
  - `VectorLibrary.java` — interface declaring native function signatures
  - `JdkVectorLibrary.java` — Panama implementation, loads `libvec`
  - `VectorSimilarityFunctions.java` — public facade
  - FFI-level tests for vector scoring functions

## Native code tiers

Source files follow a naming convention based on the ISA tier they target:

- **Tier 1** (e.g. `vec_1.cpp`) — baseline: AVX2 on x64, NEON + dotprod on ARM.
- **Tier 2** (e.g. `vec_2.cpp`) — extended: AVX-512 (icelake) on x64, SVE on ARM.
- **Tier 3** (e.g. `vec_bf16_3.cpp`) — cooperlake on x64: adds `vdpbf16ps` for native BF16 dot product.

At runtime, `caps.cpp` probes for CPU and OS support and the Java side selects
the appropriate tier.

## Quantization formats

The native kernels cover single-pair and bulk scoring for:

- **int7** (unsigned):
  - Spaces: dot-product, squared-euclidean, cosine
  - Architectures: AVX2, AVX-512
- **int8** (signed):
  - Spaces: dot-product, squared-euclidean, cosine
  - Architectures: AVX2, AVX-512, ARM/NEON
- **int4** (packed nibble):
  - Spaces: dot-product
  - Architectures: AVX2, AVX-512, ARM/NEON
- **BBQ** (binary quantized):
  - Spaces: 1-bit and 4-bit-to-1-bit dot products, with correction terms
  - Architectures: AVX2, AVX-512, ARM/NEON, ARM/SVE
- **BFloat16**:
  - Spaces: dot-product
  - Architectures: AVX-512, AVX-512-BF16 (cooperlake), ARM/NEON
- **float32**:
  - Spaces: dot-product, squared-euclidean
  - Architectures: AVX2, AVX-512, ARM/NEON, ARM/SVE

## Building the native library

The native library is built via the `Makefile` in `native/`. For
cross-compilation of all three platform binaries (darwin-aarch64,
linux-aarch64, linux-x64), use the Docker-based toolchain:

```bash
# Build the cross-compilation toolchain image
./build_cross_toolchain_image.sh

# Build and publish binaries
./publish_vec_binaries.sh
```

For local development on the current platform:

```bash
cd native
make local       # builds for the host platform
make install     # copies the binary where Gradle tests expect it
```

`make install` places the library in
`libs/native/libraries/build/platform/<os>-<arch>/` so that Gradle tests can
use it instead of fetching from Artifactory. Set `LOCAL_VEC_BINARY_OS=true` to
skip the Artifactory download:

```bash
make install
LOCAL_VEC_BINARY_OS=true ./gradlew :libs:simdvec:test
```

## Testing

```bash
# Run simdvec tests (from repo root)
./gradlew :libs:simdvec:test
```

The Gradle build also runs a `testJava21` task to verify runtime version guards
when running/testing with a JDK newer than 21.
