#!/usr/bin/env bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

# Builds libes_parquet_rs for all platforms and uploads the artifact to Artifactory.
#
# All three targets (Darwin aarch64, Linux aarch64, Linux x64) are cross-compiled
# inside a Docker container. Can be run on any host with Docker.
#
# Usage:
#   ./publish_pqrs_binaries.sh                       # build and upload to Artifactory
#   ./publish_pqrs_binaries.sh --local               # build and package only, skip upload
#   ./publish_pqrs_binaries.sh --local --force-upload # build locally, then upload to Artifactory

set -euo pipefail

VERSION="0.2.0"

LOCAL=false
FORCE_UPLOAD=false
for arg in "$@"; do
  case "$arg" in
    --local)        LOCAL=true ;;
    --force-upload) FORCE_UPLOAD=true ;;
    *) echo "Unknown option: $arg"; exit 1 ;;
  esac
done

UPLOAD=false
if [ "$LOCAL" = false ] || [ "$FORCE_UPLOAD" = true ]; then
  UPLOAD=true
fi

if [ "$UPLOAD" = true ] && [ -z "${ARTIFACTORY_API_KEY:-}" ]; then
  echo 'Error: The ARTIFACTORY_API_KEY environment variable must be set.'
  exit 1;
fi

TOOLCHAIN_IMAGE="docker.elastic.co/elasticsearch-infra/es-rust-cross-toolchain:1"
if [ "$LOCAL" = true ]; then
  TOOLCHAIN_IMAGE="es-rust-cross-toolchain:local"
fi
ARTIFACTORY_REPOSITORY="${ARTIFACTORY_REPOSITORY:-https://artifactory.elastic.dev/artifactory/elasticsearch-native/}"
TEMP=$(mktemp -d)

if [ "$UPLOAD" = true ]; then
  if curl -sS -I --fail --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/es-parquet-rs/${VERSION}/es-parquet-rs-${VERSION}.zip" > /dev/null 2>&1; then
    echo "Error: Artifacts already exist for version '${VERSION}'. Bump version before republishing."
    exit 1;
  fi
fi

echo 'Building all binaries (cross-compile in Docker)...'
docker run --rm \
  -v "$(pwd)":/workspace \
  -w /workspace \
  "$TOOLCHAIN_IMAGE" \
  make all

mkdir -p "$TEMP/darwin-aarch64"
mkdir -p "$TEMP/linux-aarch64"
mkdir -p "$TEMP/linux-x64"
cp build/darwin-aarch64/libes_parquet_rs.dylib "$TEMP/darwin-aarch64/"
cp build/linux-aarch64/libes_parquet_rs.so     "$TEMP/linux-aarch64/"
cp build/linux-x64/libes_parquet_rs.so         "$TEMP/linux-x64/"

if [ "$UPLOAD" = true ]; then
  echo 'Uploading to Artifactory...'
  (cd "$TEMP" && zip -rq - .) | curl -sSf -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary @- --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/es-parquet-rs/${VERSION}/es-parquet-rs-${VERSION}.zip"
  rm -rf "$TEMP"
else
  ZIP="$(pwd)/es-parquet-rs-${VERSION}-local.zip"
  (cd "$TEMP" && zip -rq "$ZIP" .)
  rm -rf "$TEMP"
  echo "Local build complete. Artifact: $ZIP"
fi
