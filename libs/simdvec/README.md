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
└── build.gradle            # Gradle build config
```

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
linux-aarch64, linux-x64), we use a shared Docker-based toolchain image
(`es-native-cross-toolchain`, also used by `libs/simdjson`).

The build is integrated with Gradle; Gradle detects which version of the native
sources are present and will fetch the matching binaries from Artifactory. If
the source code is new (no matching binaries on Artifactory), it compiles libvec
from source. You can drive this behaviour by setting the `VEC_NATIVE_BUILD`
environment variable:

```bash
# Cross-compile all platforms in Docker (CI mode)
VEC_NATIVE_BUILD=docker ./gradlew :libs:simdvec:buildNativeLibrary

# Build for the host platform only (dev iteration)
VEC_NATIVE_BUILD=host ./gradlew :libs:simdvec:buildNativeLibrary
```
NOTE: the Gradle daemon might not be able to access your docker installation.
If you receive a message like:
```
A problem occurred starting process 'command 'docker''
```
add `--no-daemon` to the Gradle command line.

When `VEC_NATIVE_BUILD` is unset (or set to `artifactory`), the binary is
fetched from Artifactory (no compiler or Docker required).

Building from source **replaces** the published artifact rather than
complementing it.
In `host` mode that means `libs/native/libraries/build/platform/` holds libvec
for your platform only. Anything that needs other platforms (e.g. assembling
a distribution for a different OS or architecture) needs `docker` mode or the
published artifact.

In the rare case in which your changes require a new `es-native-cross-toolchain`
docker image (e.g. new clang version, additional build tools, etc.) you can
change the Dockerfile and then build and push the cross-compilation toolchain
image with:
```bash
./build_cross_toolchain_image.sh
```

## Testing

Java tests for this project are designed to cover both Java and native code.
To run them (from the repo root):
```bash
./gradlew :libs:simdvec:test
```
It is possible to run them using a locally built native library (e.g. to test changes to the native code):
```bash
VEC_NATIVE_BUILD=host ./gradlew :libs:simdvec:test
```

The Gradle build also runs a `testJava21` task to verify runtime version guards when running/testing with a JDK newer than 21.

## Benchmarking

In order to run JMH micro-benchmarks, run:
```bash
./gradlew :libs:simdvec:benchmark
```
you can pass parameters down to JMH with `--args`, e.g.
```bash
./gradlew :libs:simdvec:benchmark --args 'VectorScorerFloat32BulkBenchmark.scoreMultipleBulk -pfunction=DOT_PRODUCT -pbulkSize=32 -pimplementation=NATIVE -pnumVectors=65000'
```
will run the float32 bulk benchmarks for the native dot-product implementation, with fixed bulk size of 32 over 65000 vectors ("L2-cache-spilling" size).
