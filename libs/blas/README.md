# blas — OpenBLAS native bindings for Elasticsearch

`libs/blas` provides Java FFM bindings for OpenBLAS.

## Threading

OpenBLAS is built with `USE_THREAD=0`, so all calls run on the calling thread.
Callers that want parallelism distribute independent calls across ES executor threads.

## Building from source

```
# Build for the local host only (linux-x64 or linux-aarch64):
BLAS_NATIVE_BUILD=host ./gradlew :libs:blas:compileJava

# Cross-compile all four platforms in the toolchain container:
BLAS_NATIVE_BUILD=docker ./gradlew :libs:native:native-libraries:extractLibs
```

The Docker cross-build requires `docker.elastic.co/elasticsearch-infra/es-native-cross-toolchain:7`.
The Makefile fetches the OpenBLAS source tarball from GitHub and verifies its SHA-256 before building.

## Publishing

Run `libs/blas/native/publish_blas_binaries.sh` (requires `ARTIFACTORY_API_KEY`) to upload
a release zip to Artifactory. Bump `VERSION` in the script and `openblasVersion` in
`libs/native/libraries/build.gradle` together before publishing.

## License

OpenBLAS is BSD-3-Clause. License text is in `licenses/openblas-LICENSE.txt`.
