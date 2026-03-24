#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

# Builds and pushes the cross-compilation toolchain image for libvec.
# Run this script when compiler versions need updating.
# No Mac or macOS SDK required — can be run on any machine with Docker.
#
# Increment VERSION when building a new image.
#
# Usage:
#   ./build_cross_toolchain_image.sh          # build and push to the Elastic registry
#   ./build_cross_toolchain_image.sh --local  # build locally only, skip push

set -euo pipefail

LOCAL=false
if [ "${1:-}" = "--local" ]; then
  LOCAL=true
fi

VERSION=1
HOST=docker.elastic.co
REPOSITORY=es-dev/es-native-cross-toolchain
BASE_IMAGE=$HOST/es-dev/es-native-cross-base:$VERSION
IMAGE=$HOST/$REPOSITORY:$VERSION
if [ "$LOCAL" = true ]; then
  BASE_IMAGE=es-native-cross-base:local
  IMAGE=es-native-cross-toolchain:local
fi

cd "$(dirname "$0")"

echo "Building $IMAGE (base: $BASE_IMAGE) ..."
docker build --platform linux/amd64 \
  --build-arg BASE_IMAGE="$BASE_IMAGE" \
  -f Dockerfile.cross-toolchain \
  -t "$IMAGE" \
  .

if [ "$LOCAL" = true ]; then
  echo "Local build complete. Image tagged as $IMAGE."
  echo "To use it: set the docker run image to $IMAGE in publish_vec_binaries.sh."
else
  echo "Pushing $IMAGE ..."
  # Authenticate at https://docker-auth.elastic.co if not already logged in.
  docker push "$IMAGE"
  echo "Done. Update publish_vec_binaries.sh to reference $REPOSITORY:$VERSION."
fi
