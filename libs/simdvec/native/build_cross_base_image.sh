#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

# Builds and pushes the cross-compilation base image containing the macOS SDK.
# Run this script when the macOS SDK needs to be updated.
#
# Must be run on a Mac with Xcode CLI tools installed.
# Increment VERSION when building a new image.
#
# Usage:
#   ./build_cross_base_image.sh          # build and push to the Elastic registry
#   ./build_cross_base_image.sh --local  # build locally only, skip push

set -euo pipefail

LOCAL=false
if [ "${1:-}" = "--local" ]; then
  LOCAL=true
fi

if [ "$(uname -s)" != "Darwin" ]; then
  echo "Error: this script must be run on a Mac."
  exit 1
fi

VERSION=1
HOST=docker.elastic.co
REPOSITORY=es-dev/es-native-cross-base
IMAGE=$HOST/$REPOSITORY:$VERSION
if [ "$LOCAL" = true ]; then
  IMAGE=es-native-cross-base:local
fi
SDK_TARBALL=MacOSX.sdk.tar.gz

cd "$(dirname "$0")"

SDK_PATH=$(xcrun --show-sdk-path 2>/dev/null) || true
if [ -z "$SDK_PATH" ] || [ ! -d "$SDK_PATH" ]; then
  echo "Error: macOS SDK not found. Install Xcode Command Line Tools with:"
  echo "  xcode-select --install"
  exit 1
fi

echo "Creating macOS SDK tarball from $SDK_PATH ..."
tar -czf "$SDK_TARBALL" -C "$SDK_PATH/.." MacOSX.sdk

echo "Building $IMAGE ..."
docker build --platform linux/amd64 \
  --build-arg SDK_TARBALL="$SDK_TARBALL" \
  -f Dockerfile.cross-base \
  -t "$IMAGE" \
  .

rm -f "$SDK_TARBALL"

if [ "$LOCAL" = true ]; then
  echo "Local build complete. Image tagged as $IMAGE."
  echo "To use it: update Dockerfile.cross-toolchain to reference $IMAGE."
else
  echo "Pushing $IMAGE ..."
  # Authenticate at https://docker-auth.elastic.co if not already logged in.
  docker push "$IMAGE"
  echo "Done. Update Dockerfile.cross-toolchain to reference $REPOSITORY:$VERSION."
fi
