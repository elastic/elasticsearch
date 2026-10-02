# Building the native libraries

All three targets — `darwin-aarch64`, `linux-aarch64`, `linux-x64` — are cross-compiled inside a
single toolchain image.

For the Linux targets everything comes from Debian packages: clang plus the
`libstdc++-*-dev-{arm64,amd64}-cross` sysroots, installed straight into the image. Darwin has no
equivalent package, so its sysroot is assembled during the image build from Apple's open-source
distributions (Libc, xnu, Libm, libpthread, libplatform, libmalloc) plus upstream libc++ headers.
That assembly is why most of this document is about Darwin; the Linux targets need nothing beyond
the `--target=` flag.

This document explains the workflow for building a library for all three targets.
Details and the reasoning behind each piece is in the comments of the files themselves.

| File | Role                                                                                                        |
|---|-------------------------------------------------------------------------------------------------------------|
| `Dockerfile.cross-toolchain` | Defines the docker image: clang, the Linux cross sysroots, and the Darwin sysroot at `/opt/darwin-sysroot`. |
| `build_cross_toolchain_image.sh` | Builds and pushes the image. Holds the image `VERSION`.                                                     |
| `darwin-sysroot/versions.env` | Pinned Apple component tags and libc++ version.                                                             |
| `darwin-sysroot/assemble.sh` | Assembles the Darwin sysroot. Runs during the image build only.                                             |
| `darwin-sysroot/probe.cpp` | Declares which system headers the sysroot must support.                                                     |
| `Makefile` | Compile and link rules for one library.                                                                     |
| `publish_vec_binaries.sh` | Runs `make all` in the image and uploads the result. Holds the library `VERSION`.                           |

`probe.cpp` is the one to know about: `assemble.sh` compiles it to decide which xnu headers to
keep, so the sysroot contains exactly the system headers reachable from the includes listed
there.

## Build and test a library

Fast iteration, using the host compiler and (on a Mac) the Xcode SDK:

```sh
cd libs/simdvec/native
make install                    # builds for the host platform and copies into place
cd ../../.. && LOCAL_VEC_BINARY_OS=darwin ./gradlew :libs:simdvec:test
```

The real cross build, using the toolchain image and the assembled sysroot, but still keeping everything local. This is akin to what CI and `publish` produce, and it exercises the Darwin sysroot:

```sh
cd libs/simdvec/native
./build_cross_toolchain_image.sh --local          # tags es-native-cross-toolchain:local
rm -rf build/obj build/libs/vec/shared            # publish does not clean; stale objects are reused
./publish_vec_binaries.sh --local                 # all three targets + a local zip

cp build/libs/vec/shared/aarch64/libvec.dylib \
   ../../native/libraries/build/platform/darwin-aarch64/
cd ../../.. && LOCAL_VEC_BINARY_OS=darwin ./gradlew :libs:simdvec:test
```

Omit `--local` on either script to use the published image and upload to Artifactory. Publishing
needs `ARTIFACTORY_API_KEY`, and refuses to overwrite an existing version.

Useful checks on a Darwin build:

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
   if the header is missing from the sysroot or carries no open-source licence.
3. Bump `VERSION` in `build_cross_toolchain_image.sh`, then
   `./build_cross_toolchain_image.sh` to push it.
4. Point every `publish_*_binaries.sh` at the new `TOOLCHAIN_IMAGE` tag.

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

3. Build it. If a system header is missing, follow *Add a system header* above.
4. Declare the artifact in `libs/native/libraries/build.gradle`: add a version variable, add the
   module to the repository `filter`, and add the `libs` dependency behind a
   `LOCAL_<LIBRARY>_BINARY` env check.

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
