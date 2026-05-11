#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

set -euo pipefail

OUTPUT=build/libraries
TARGETS="x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin"

# Move to the "native" directory
cd $(dirname $0)/..

SCRIPT="build-tools/$(basename $0)"

if [ $# = 0 ]; then
  echo "Building cross-compilation Docker image"
  docker build --quiet -f build-tools/Dockerfile.cargo-zigbuild -t cargo-zigbuild .
  docker run \
    --rm \
    -v $PWD:/workspace -w /workspace \
    -v $HOME/.cargo/registry:/usr/local/cargo/registry \
    cargo-zigbuild ./"$SCRIPT" --build
  exit
fi

if [ "$1" != "--build" ]; then
  echo "Unknown parameter $1"
  exit 1
fi

if [ ! -f "/usr/local/cargo/bin/cargo-zigbuild" ]; then
  echo "Use '--build' only when running in the cargo-zigbuild container"
  exit 1
fi

#--- MacOS specific stuff
export CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup"
export SDKROOT=$(mktemp -d)

# TAPI files are text placeholders for MacOS SDK libraries that the linker will pick up if present.
# The files produced below don't define actual symbols. This works in combination with the linker parameters
# `-undefined -C link-arg=dynamic_lookup` defined above, that let it silently ignore missing symbols.
for lib in libc libm libiconv libSystem; do
  file=$SDKROOT/usr/lib/$lib.tbd
  mkdir -p "$(dirname $file)"
  cat > $file << EOF
--- !tapi-tbd
tbd-version: 4
targets: [ arm64-macos ]
install-name: /usr/lib/$lib.dylib
exports: []
...
EOF
done

for fwk in CoreFoundation Security; do
  file=$SDKROOT/System/Library/Frameworks/$fwk.framework/$fwk.tbd
  mkdir -p "$(dirname $file)"
  cat > $file << EOF
--- !tapi-tbd
tbd-version: 4
targets: [ arm64-macos ]
install-name: /System/Library/Frameworks/$fwk.framework/$fwk
exports: []
...
EOF
done

#--- Build
rm -rf $OUTPUT
mkdir -p $OUTPUT

for target in $TARGETS; do
  echo
  echo "Building target $target"
  cargo zigbuild --release --target "$target"
  mkdir -p $OUTPUT/$target
  ext="so"
  if [ "$target" = "aarch64-apple-darwin" ]; then ext="dylib"; fi
  cp target/$target/release/*.$ext $OUTPUT/$target
done
