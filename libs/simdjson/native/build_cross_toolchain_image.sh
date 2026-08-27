#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

# Builds the cross-compilation toolchain image for libes_simdjson.
# Increment VERSION when building a new image.
#
# Usage:
#   ./build_cross_toolchain_image.sh          # build and push to the Elastic registry
#   ./build_cross_toolchain_image.sh --local  # build locally only, skip push

set -euo pipefail

LOCAL=false
case "${1:-}" in
  "")      ;;
  --local) LOCAL=true ;;
  *)       echo "Usage: $0 [--local]" >&2; exit 1 ;;
esac

VERSION=1
HOST=docker.elastic.co
REPOSITORY=elasticsearch-infra/es-simdjson-cross-toolchain
IMAGE=$HOST/$REPOSITORY:$VERSION
if [ "$LOCAL" = true ]; then
  IMAGE=es-simdjson-cross-toolchain:local
fi

cd "$(dirname "$0")"

if [ "$LOCAL" = true ]; then
  echo "Building $IMAGE (host platform only) ..."
  # Legacy builder uses the locally cached base image without registry auth.
  DOCKER_BUILDKIT=0 docker build \
    -f Dockerfile.cross-toolchain \
    -t "$IMAGE" \
    .
  echo "Local build complete. Image tagged as $IMAGE."
  echo "To use it: TOOLCHAIN_IMAGE=$IMAGE ./publish_simdjson_binaries.sh --local"
else
  echo "Building and pushing $IMAGE (linux/amd64 + linux/arm64) ..."
  docker buildx build --platform linux/amd64,linux/arm64 \
    -f Dockerfile.cross-toolchain \
    -t "$IMAGE" \
    --push \
    .
  echo "Done. Update publish_simdjson_binaries.sh to reference $REPOSITORY:$VERSION."
fi
