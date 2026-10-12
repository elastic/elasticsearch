# Native library cross-compilation toolchain

Every native library under `libs/` (libvec in `libs/simdvec`, libsimdjson in `libs/simdjson`, ...) is
built with the toolchain in this directory. Each library keeps its own `Makefile` and publish script
in `libs/<library>/native/`.

All four targets — `darwin-aarch64`, `linux-aarch64`, `linux-x64`, `windows-x64` — are
cross-compiled inside a single toolchain image. Windows uses llvm-mingw (clang, mingw-w64 and UCRT),
installed into the image from its pinned release.

For the Linux targets everything comes from Debian packages: clang plus the
`libstdc++-*-dev-{arm64,amd64}-cross` sysroots, installed straight into the image. Darwin has no
equivalent package, so its sysroot is assembled during the image build from Apple's open-source
distributions (Libc, xnu, Libm, libpthread, libplatform, libmalloc) plus upstream libc++ headers.
That assembly is why most of this document is about Darwin; the Linux targets need nothing beyond
the `--target=` flag.

This document explains the workflow for building a library for all four targets.
Details and the reasoning behind each piece is in the comments of the files themselves.
Paths such as `./build_cross_toolchain_image.sh` are relative to this directory; Gradle commands
run from the repository root.

| File | Role                                                                                                        |
|---|-------------------------------------------------------------------------------------------------------------|
| `Dockerfile.cross-toolchain` | Defines the docker image: clang, the Linux cross sysroots, and the Darwin sysroot at `/opt/darwin-sysroot`. |
| `build_cross_toolchain_image.sh` | Builds and pushes the image. Holds the image `VERSION`.                                                     |
| `darwin-sysroot/versions.env` | Pinned Apple component tags and libc++ version.                                                             |
| `darwin-sysroot/assemble.sh` | Assembles the Darwin sysroot. Runs during the image build only.                                             |
| `darwin-sysroot/probe.cpp` | Declares which system headers the sysroot must support.                                                     |
| `libs/<library>/native/Makefile` | Compile and link rules for one library.                                                    |
| `libs/<library>/native/publish_<library>_binaries.sh` | Runs `make all` in the image and uploads the result. Holds the library `VERSION`. |

`probe.cpp` is the one to know about: `assemble.sh` compiles it to decide which xnu headers to
keep, so the sysroot contains exactly the system headers reachable from the includes listed
there.

## Build and test a library

Each library has an environment variable (`VEC_NATIVE_BUILD` for libvec, `SIMDJSON_NATIVE_BUILD` for
libsimdjson, ...) that makes Gradle build it from source instead of using the published artifact;
tests then run against what was just built. The examples below use libvec.

Fast iteration, using the host compiler and (on a Mac) the Xcode SDK, for the host platform only:

```sh
VEC_NATIVE_BUILD=host ./gradlew --no-daemon :libs:simdvec:test
```

The real cross build of all four targets, using the toolchain image and the assembled sysroot.
This is what CI and `publish` produce:

```sh
VEC_NATIVE_BUILD=docker ./gradlew --no-daemon :libs:simdvec:test
```

To try a toolchain image you built locally, point the build at it with `NATIVE_TOOLCHAIN_IMAGE`:

```sh
libs/native-toolchain/build_cross_toolchain_image.sh --local  # tags es-native-cross-toolchain:local
rm -rf libs/simdvec/native/build                              # make does not see an image change
NATIVE_TOOLCHAIN_IMAGE=es-native-cross-toolchain:local VEC_NATIVE_BUILD=docker \
  ./gradlew --no-daemon :libs:simdvec:test
```

`make` only rebuilds what is out of date relative to the sources, so delete the library's
`native/build` whenever the outputs must be rebuilt for another reason, such as a new image.
`--no-daemon` avoids a reused Gradle daemon whose environment cannot start `docker` ("A problem
occurred starting process 'command 'docker''").

To publish, run the library's `publish_<library>_binaries.sh` from its `native/` directory (see
*Publish a library*). With `--local` it builds with `es-native-cross-toolchain:local` and only
writes a local zip. Publishing needs `ARTIFACTORY_API_KEY`, and refuses to overwrite an existing
version.

Useful checks on a Darwin build (libvec, from `libs/simdvec/native`):

```sh
# imports; must all exist on the target OS, as the Darwin link resolves them at load time
nm -u build/libs/vec/shared/aarch64/libvec.dylib

# deployment target; minos must match the --target= in the Makefile
otool -l build/libs/vec/shared/aarch64/libvec.dylib | grep -A3 LC_BUILD_VERSION

# what the sysroot was built from: component tags, licences, header counts
docker run --rm es-native-cross-toolchain:local cat /opt/darwin-sysroot/MANIFEST
```

## Add a system header

When a library starts including a system header that no other library uses, the new `#include`
will result in a compilation error, as the toolchain will fails to resolve it.
To fix it, you will need to add the missing system header(s) to the Darwin sysroot:

1. Add the include to `darwin-sysroot/probe.cpp`, in the matching group.
2. Rebuild and verify: `./build_cross_toolchain_image.sh --local`. `assemble.sh` fails the build
   if the header is missing from the sysroot or carries no open-source licence. Then build the
   library against it with `NATIVE_TOOLCHAIN_IMAGE=es-native-cross-toolchain:local` (see
   *Build and test a library*).
3. Bump `VERSION` in `build_cross_toolchain_image.sh`, then
   `./build_cross_toolchain_image.sh` to push it.
4. Point every reference at the new tag (`git grep es-native-cross-toolchain:` finds them all).

Steps 3 and 4 are needed because the sysroot is baked into the image.

## Add a native library

1. Create `libs/<library>/native/` with a `Makefile` and a `publish_<library>_binaries.sh`,
   modelled on the existing ones.
2. Copy the Darwin flags verbatim. The `-isystem` order must be kept as is for the C pre-processor to resolve headers correctly:

   ```make
   MACOS_SYSROOT ?= /opt/darwin-sysroot
   CLANG_RESOURCE = $(shell $(CLANG_CXX) -print-resource-dir)
   CLANG      = $(CLANG_CXX) --target=arm64-apple-macos14 -nostdinc \
                  -isystem $(MACOS_SYSROOT)/usr/include/c++/v1 \
                  -isystem $(CLANG_RESOURCE)/include \
                  -isystem $(MACOS_SYSROOT)/usr/include
   CLANG_LINK = $(CLANG_CXX) --target=arm64-apple-macos14 -fuse-ld=lld -nostdlib \
                  -Wl,-undefined,dynamic_lookup
   ```

3. Wire the build into Gradle: apply `elasticsearch.native-library-build` in
   `libs/<library>/build.gradle` and declare a `nativeLibraryBuild {}` block (mode variable, sources,
   toolchain image, docker and host commands, and where each platform's output is collected from),
   modelled on `libs/simdvec/build.gradle`.
4. Build it with `<LIBRARY>_NATIVE_BUILD=docker`. If a system header is missing, follow
   *Add a system header* above.
5. Declare the artifact in `libs/native/libraries/build.gradle`: a version variable, an entry in
   `nativeLibraries {}` (`modeEnvironmentVariable`, `publishedModule` and `builtBy`, as for `vec`),
   and an `includeModule` in the repository `filter`.

## Bump the sysroot components

Do not bump the Apple component tags individually: they share
types and macros across headers, so they only work as the set Apple shipped together.
If you need to bump them, you can find that set in the `apple-oss-distributions/distribution-macOS`
repository, which tracks every component as a git submodule and has one branch per
macOS release (`rel/macOS-15`, `rel/macOS-26`, ...).
The submodule commits on a branch are the coherent set.
It is possible to map each to its tag with:

```sh
gh api repos/apple-oss-distributions/distribution-macOS/git/trees/rel/macOS-15 \
  --jq '.tree[] | select(.type=="commit") | "\(.path) \(.sha)"'
gh api repos/apple-oss-distributions/<component>/tags --jq '.[] | select(.commit.sha=="<sha>") | .name'
```

NOTE: Libm is not in that manifest. It has its own tag, but it is unlikely you need to worry about it, as
it has not changed since 2002.

1. Edit `darwin-sysroot/versions.env`.
2. `./build_cross_toolchain_image.sh --local`. The licence scan and the `probe.cpp` compile both
   run here, so a component that drops a header or changes licensing fails the build.
3. Diff `/opt/darwin-sysroot/xnu-closure.txt` against the previous image to see which headers
   entered or left.
4. Bump the image `VERSION` and push, as in *Add a system header*.

Raising the deployment target above `arm64-apple-macos14` also means raising `LIBCXX_MAJOR` to
the libc++ release Apple ships in that macOS version; `versions.env` documents the mapping.

## Publish a library

1. `./build_cross_toolchain_image.sh` ONLY if the image changed.
2. Bump `VERSION` in `publish_<library>_binaries.sh`, then run it.
3. Bump the matching version in `libs/native/libraries/build.gradle`.
