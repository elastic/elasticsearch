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
case "${1:-}" in
  "")      ;;
  --local) LOCAL=true ;;
  *)       echo "Usage: $0 [--local]" >&2; exit 1 ;;
esac

VERSION=2
HOST=docker.elastic.co
REPOSITORY=elasticsearch-infra/es-native-cross-toolchain
IMAGE=$HOST/$REPOSITORY:$VERSION
if [ "$LOCAL" = true ]; then
  IMAGE=es-native-cross-toolchain:local
fi

cd "$(dirname "$0")"

echo "Building $IMAGE ..."
docker build --platform linux/amd64 \
  -f Dockerfile.cross-toolchain \
  -t "$IMAGE" \
  .

if [ "$LOCAL" = true ]; then
  echo "Local build complete. Image tagged as $IMAGE."
  echo "To use it: set the docker run image to $IMAGE in publish_vec_binaries.sh."
else
  echo "Pushing $IMAGE ..."
  # Authenticate at https://docker-auth.elastic.co if not already logged in.
  # If you get a 500, or if https://docker-auth.elastic.co is a blank page,
  # first do ` docker logout docker.elastic.co`, then visit https://docker-auth.elastic.co again
  # and follow the instructions to login.
  docker push "$IMAGE"
  echo "Done. Update publish_vec_binaries.sh to reference $REPOSITORY:$VERSION."
fi
