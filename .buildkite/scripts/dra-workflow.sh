#!/bin/bash

set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:-snapshot}"

# Don't publish main branch to staging
if [[ "$BUILDKITE_BRANCH" == "main" && "$WORKFLOW" == "staging" ]]; then
  exit 0
fi

RM_BRANCH="$BUILDKITE_BRANCH"
if [[ "$BUILDKITE_BRANCH" == "main" ]]; then
  RM_BRANCH=master
fi

ES_VERSION=$(grep elasticsearch build-tools-internal/version.properties | sed "s/elasticsearch *= *//g")

VERSION_SUFFIX=""
if [[ "$WORKFLOW" == "snapshot" ]]; then
  VERSION_SUFFIX="-SNAPSHOT"
fi

BEATS_BUILD_ID="$(./.ci/scripts/resolve-dra-manifest.sh beats "$RM_BRANCH" "$ES_VERSION" "$WORKFLOW")"
ML_CPP_BUILD_ID="$(./.ci/scripts/resolve-dra-manifest.sh ml-cpp "$RM_BRANCH" "$ES_VERSION" "$WORKFLOW")"

LICENSE_KEY_ARG=""
BUILD_SNAPSHOT_ARG=""

if [[ "$WORKFLOW" == "staging" ]]; then
  LICENSE_KEY=$(mktemp -d)/license.key
  # Intentionally leaving this secret here instead of a hook
  # Since it's getting redirected straight to a file, it's probably even safer to just leave it here rather than stick it in an env var for a short period of time
  vault read -field pubkey secret/ci/elastic-elasticsearch/migrated/license | base64 --decode > "$LICENSE_KEY"
  LICENSE_KEY_ARG="-Dlicense.key=$LICENSE_KEY"

  BUILD_SNAPSHOT_ARG="-Dbuild.snapshot=false"
fi

.ci/scripts/run-gradle.sh -Ddra.artifacts=true \
  -Ddra.artifacts.dependency.beats="${BEATS_BUILD_ID}" \
  -Ddra.artifacts.dependency.ml-cpp="${ML_CPP_BUILD_ID}" \
  -Ddra.workflow="$WORKFLOW" \
  -Dcsv="$WORKSPACE/build/distributions/dependencies-${ES_VERSION}${VERSION_SUFFIX}.csv" \
  -Dbuild.snapshot=false \
  $LICENSE_KEY_ARG \
  $BUILD_SNAPSHOT_ARG \
  buildReleaseArtifacts \
  exportCompressedDockerImages \
  :distribution:generateDependenciesReport

x-pack/plugin/sql/connectors/tableau/package.sh asm qualifier="$VERSION_SUFFIX"

# we regenerate this file as part of the release manager invocation
rm "build/distributions/elasticsearch-jdbc-${ES_VERSION}${VERSION_SUFFIX}.taco.sha512"

# Allow other users access to read the artifacts so they are readable in the
# container
find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

# Allow other users write access to create checksum files
find "$WORKSPACE" -type d -path "*/build/distributions" -exec chmod a+w {} \;

# Artifacts should be generated
# TODO remove echo, fix DRA_VAULT vars
echo docker run --rm \
  --name release-manager \
  -e VAULT_ADDR="$DRA_VAULT_ADDR" \
  -e VAULT_ROLE_ID="DRA_VAULT_ROLE_ID_SECRET" \
  -e VAULT_SECRET_ID="DRA_VAULT_SECRET_ID_SECRET" \
  --mount type=bind,readonly=false,src="$PWD",target=/artifacts \
  docker.elastic.co/infra/release-manager:latest \
  cli collect \
  --project elasticsearch \
  --branch "$RM_BRANCH" \
  --commit "$GIT_COMMIT" \
  --workflow "$WORKFLOW" \
  --version "$ES_VERSION" \
  --artifact-set main \
  --dependency "beats:https://artifacts-${WORKFLOW}.elastic.co/beats/${BEATS_BUILD_ID}/manifest-${ES_VERSION}${VERSION_SUFFIX}.json" \
  --dependency "ml-cpp:https://artifacts-${WORKFLOW}.elastic.co/ml-cpp/${ML_CPP_BUILD_ID}/manifest-${ES_VERSION}${VERSION_SUFFIX}.json"
