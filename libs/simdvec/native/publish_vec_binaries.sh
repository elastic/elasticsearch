#!/usr/bin/env bash
#
 # Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 # or more contributor license agreements. Licensed under the "Elastic License
 # 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 # Public License v 1"; you may not use this file except in compliance with, at
 # your election, the "Elastic License 2.0", the "GNU Affero General Public
 # License v3.0 only", or the "Server Side Public License, v 1".
#

set -e

if [ "$(uname -sm)" != "Darwin arm64" ]; then
  echo 'This script must be run on an aarch64 MacOS system.'
  exit 1;
fi

if [ -z "$ARTIFACTORY_API_KEY" ]; then
  echo 'Error: The ARTIFACTORY_API_KEY environment variable must be set.'
  exit 1;
fi

VERSION="1.0.11"
ARTIFACTORY_REPOSITORY="${ARTIFACTORY_REPOSITORY:-https://artifactory.elastic.dev/artifactory/elasticsearch-native/}"
TEMP=$(mktemp -d)

if curl -sS -I --fail --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/vec/${VERSION}/vec-${VERSION}.zip" > /dev/null 2>&1; then
  echo "Error: Artifacts already exist for version '${VERSION}'. Bump version before republishing."
  exit 1;
fi

echo 'Building Darwin binary...'
./gradlew --quiet --console=plain clean vecAarch64SharedLibrary

echo 'Building Linux binary...'
mkdir -p build/libs/vec/shared/aarch64/
DOCKER_IMAGE=$(docker build --platform linux/arm64 --quiet --file=Dockerfile.aarch64 .)
docker run $DOCKER_IMAGE > build/libs/vec/shared/aarch64/libvec.so

echo 'Building Linux x64 binary...'
DOCKER_IMAGE=$(docker build --platform linux/amd64 --quiet --file=Dockerfile.amd64 .)
mkdir -p build/libs/vec/shared/amd64
docker run --platform linux/amd64 $DOCKER_IMAGE > build/libs/vec/shared/amd64/libvec.so

mkdir -p $TEMP/darwin-aarch64
mkdir -p $TEMP/linux-aarch64
mkdir -p $TEMP/linux-x64
cp build/libs/vec/shared/aarch64/libvec.dylib $TEMP/darwin-aarch64/
cp build/libs/vec/shared/aarch64/libvec.so $TEMP/linux-aarch64/
cp build/libs/vec/shared/amd64/libvec.so $TEMP/linux-x64/

echo 'Uploading to Artifactory...'
(cd $TEMP && zip -rq - .) | curl -sS -X PUT -H "X-JFrog-Art-Api: ${ARTIFACTORY_API_KEY}" --data-binary @- --location "${ARTIFACTORY_REPOSITORY}/org/elasticsearch/vec/${VERSION}/vec-${VERSION}.zip"

rm -rf $TEMP
