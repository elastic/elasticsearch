#!/bin/bash

set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:-snapshot}"
BRANCH="${BUILDKITE_BRANCH:-}"

# Don't publish main branch to staging
if [[ ("$BRANCH" == "main" || "$BRANCH" == *.x) && "$WORKFLOW" == "staging" ]]; then
  exit 0
fi

echo --- Preparing

# TODO move this to image
sudo NEEDRESTART_MODE=l apt-get update -y
sudo NEEDRESTART_MODE=l apt-get install -y libxml2-utils python3.10-venv

RM_BRANCH="$BRANCH"
if [[ "$BRANCH" == "main" ]]; then
  RM_BRANCH=master
fi

ES_VERSION=$(grep elasticsearch build-tools-internal/version.properties | sed "s/elasticsearch *= *//g")
echo "ES_VERSION=$ES_VERSION"

VERSION_SUFFIX=""
if [[ "$WORKFLOW" == "snapshot" ]]; then
  VERSION_SUFFIX="-SNAPSHOT"
fi

BEATS_BUILD_ID="$(./.ci/scripts/resolve-dra-manifest.sh beats "$RM_BRANCH" "$ES_VERSION" "$WORKFLOW")"
echo "BEATS_BUILD_ID=$BEATS_BUILD_ID"

ML_CPP_BUILD_ID="$(./.ci/scripts/resolve-dra-manifest.sh ml-cpp "$RM_BRANCH" "$ES_VERSION" "$WORKFLOW")"
echo "ML_CPP_BUILD_ID=$ML_CPP_BUILD_ID"

LICENSE_KEY_ARG=""
BUILD_SNAPSHOT_ARG=""

if [[ "$WORKFLOW" == "staging" ]]; then
  LICENSE_KEY=$(mktemp -d)/license.key
  # Notice that only the public key is being read here, which isn't really secret
  vault read -field pubkey secret/ci/elastic-elasticsearch/migrated/license | base64 --decode > "$LICENSE_KEY"
  LICENSE_KEY_ARG="-Dlicense.key=$LICENSE_KEY"

  BUILD_SNAPSHOT_ARG="-Dbuild.snapshot=false"
fi

echo --- Building release artifacts

.ci/scripts/run-gradle.sh -Ddra.artifacts=true \
  -Ddra.artifacts.dependency.beats="${BEATS_BUILD_ID}" \
  -Ddra.artifacts.dependency.ml-cpp="${ML_CPP_BUILD_ID}" \
  -Ddra.workflow="$WORKFLOW" \
  -Dcsv="$WORKSPACE/build/distributions/dependencies-${ES_VERSION}${VERSION_SUFFIX}.csv" \
  $LICENSE_KEY_ARG \
  $BUILD_SNAPSHOT_ARG \
  buildReleaseArtifacts \
  exportCompressedDockerImages \
  :distribution:generateDependenciesReport

PATH="$PATH:${JAVA_HOME}/bin" # Required by the following script
x-pack/plugin/sql/connectors/tableau/package.sh asm qualifier="$VERSION_SUFFIX"

# we regenerate this file as part of the release manager invocation
rm "build/distributions/elasticsearch-jdbc-${ES_VERSION}${VERSION_SUFFIX}.taco.sha512"

# Allow other users access to read the artifacts so they are readable in the
# container
find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

# Allow other users write access to create checksum files
find "$WORKSPACE" -type d -path "*/build/distributions" -exec chmod a+w {} \;

echo --- Running release-manager

# Artifacts should be generated
docker run --rm \
  --name release-manager \
  -e VAULT_ADDR="$DRA_VAULT_ADDR" \
  -e VAULT_ROLE_ID="$DRA_VAULT_ROLE_ID_SECRET" \
  -e VAULT_SECRET_ID="$DRA_VAULT_SECRET_ID_SECRET" \
  --mount type=bind,readonly=false,src="$PWD",target=/artifacts \
  docker.elastic.co/infra/release-manager:latest \
  cli collect \
  --project elasticsearch \
  --branch "$RM_BRANCH" \
  --commit "$BUILDKITE_COMMIT" \
  --workflow "$WORKFLOW" \
  --version "$ES_VERSION" \
  --artifact-set main \
  --dependency "beats:https://artifacts-${WORKFLOW}.elastic.co/beats/${BEATS_BUILD_ID}/manifest-${ES_VERSION}${VERSION_SUFFIX}.json" \
  --dependency "ml-cpp:https://artifacts-${WORKFLOW}.elastic.co/ml-cpp/${ML_CPP_BUILD_ID}/manifest-${ES_VERSION}${VERSION_SUFFIX}.json"
