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

# Branch used to resolve dependency manifests (beats, ml-cpp). Defaults to the
# current Buildkite branch, but is overridable so feature branches can point at
# a real release branch's manifests when testing DRA changes (the ml-cpp / beats
# DRA pipelines only build the actual release branches, so a feature branch would
# otherwise fail manifest lookup).
RM_BRANCH="${RM_BRANCH:-$BRANCH}"
if [[ "$RM_BRANCH" == "main" ]]; then
  RM_BRANCH=master
fi

ES_VERSION=$(grep elasticsearch build-tools-internal/version.properties | sed "s/elasticsearch *= *//g")
echo "ES_VERSION=$ES_VERSION"

VERSION_SUFFIX=""
if [[ "$WORKFLOW" == "snapshot" ]]; then
  VERSION_SUFFIX="-SNAPSHOT"
fi

if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  ES_VERSION="${ES_VERSION}-${VERSION_QUALIFIER}"
  echo "Version qualifier specified. ES_VERSION=${ES_VERSION}."
fi

BEATS_BUILD_ID="$(./.ci/scripts/resolve-dra-manifest.sh beats "$RM_BRANCH" "$ES_VERSION" "$WORKFLOW")"
echo "BEATS_BUILD_ID=$BEATS_BUILD_ID"

ML_CPP_BUILD_ID="$(./.ci/scripts/resolve-dra-manifest.sh ml-cpp "$RM_BRANCH" "$ES_VERSION" "$WORKFLOW")"
echo "ML_CPP_BUILD_ID=$ML_CPP_BUILD_ID"

LICENSE_KEY_ARG=""
BUILD_SNAPSHOT_ARG=""
VERSION_QUALIFIER_ARG=""

if [[ "$WORKFLOW" == "staging" ]]; then
  LICENSE_KEY=$(mktemp -d)/license.key
  # Notice that only the public key is being read here, which isn't really secret
  vault read -field pubkey secret/ci/elastic-elasticsearch/migrated/license | base64 --decode > "$LICENSE_KEY"
  LICENSE_KEY_ARG="-Dlicense.key=$LICENSE_KEY"

  BUILD_SNAPSHOT_ARG="-Dbuild.snapshot=false"
fi

if [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  VERSION_QUALIFIER_ARG="-Dbuild.version_qualifier=$VERSION_QUALIFIER"
fi

echo --- install qemu for aarch64 docker image builds
# NOTE: qemu-v9.2.2 mishandles openat2(O_NOFOLLOW) on aarch64 (glibc tar's
# CVE-2025-45582 fix triggers this), causing "tar: ...: Cannot open: Invalid
# argument" during linux/arm64 cross builds. qemu-v10.2.3 has the upstream
# fix. See https://github.com/tonistiigi/binfmt/issues/285.
docker run --privileged --rm tonistiigi/binfmt:qemu-v10.2.3 --install all
docker buildx create --driver docker-container --use --bootstrap

echo --- Building release artifacts

.ci/scripts/run-gradle.sh -Ddra.artifacts=true \
  -Ddra.artifacts.dependency.beats="${BEATS_BUILD_ID}" \
  -Ddra.artifacts.dependency.ml-cpp="${ML_CPP_BUILD_ID}" \
  -Ddra.workflow="$WORKFLOW" \
  -Dcsv="$WORKSPACE/build/distributions/dependencies-${ES_VERSION}${VERSION_SUFFIX}.csv" \
  $LICENSE_KEY_ARG \
  $BUILD_SNAPSHOT_ARG \
  $VERSION_QUALIFIER_ARG \
  buildReleaseArtifacts \
  exportCompressedDockerImages \
  exportDockerContexts \
  :zipAggregation \
  :prepareDraSnapshotMavenAggregation \
  :distribution:generateDependenciesReport

PATH="$PATH:${JAVA_HOME}/bin" # Required by the following script
if [[ -z "${VERSION_QUALIFIER:-}" ]]; then
x-pack/plugin/sql/connectors/tableau/package.sh asm qualifier="$VERSION_SUFFIX"
else
x-pack/plugin/sql/connectors/tableau/package.sh asm qualifier="-$VERSION_QUALIFIER"
fi

# dractl generates its own checksums; remove the Gradle-produced .sha512 to
# avoid a duplicate checksum file for the TACO connector in artifacts/.
rm "build/distributions/elasticsearch-jdbc-${ES_VERSION}${VERSION_SUFFIX}.taco.sha512"

# Allow other users access to read the artifacts so they are readable in the
# container
find "$WORKSPACE" -type f -path "*/build/distributions/*" -exec chmod a+r {} \;

# Allow other users write access to create checksum files
find "$WORKSPACE" -type d -path "*/build/distributions" -exec chmod a+w {} \;

echo --- Publishing maven aggregation to S3
DRA_WORKFLOW="$WORKFLOW" \
  .buildkite/scripts/dra-maven-snapshots-publish.sh

echo --- Staging artifacts for dra-prep plugin

# Expose the runtime-computed stack version and dependency manifest URLs to the
# dra-prep post-command hook. The plugin reads BUILDKITE_PLUGIN_DRA_PREP_* env
# vars; writing them to BUILDKITE_ENV_FILE makes them available to hooks that
# run after this command (Buildkite processes the env file between command and
# post-command hooks, allowing overrides of values set from the pipeline YAML).
if [[ -n "${BUILDKITE_ENV_FILE:-}" ]]; then
  {
    echo "BUILDKITE_PLUGIN_DRA_PREP_STACK_VERSION=${ES_VERSION}${VERSION_SUFFIX}"
    echo "BUILDKITE_PLUGIN_DRA_PREP_DEPENDENCIES_0=beats:https://artifacts-${WORKFLOW}.elastic.co/beats/${BEATS_BUILD_ID}/manifest-${ES_VERSION}${VERSION_SUFFIX}.json"
    echo "BUILDKITE_PLUGIN_DRA_PREP_DEPENDENCIES_1=ml-cpp:https://artifacts-${WORKFLOW}.elastic.co/ml-cpp/${ML_CPP_BUILD_ID}/manifest-${ES_VERSION}${VERSION_SUFFIX}.json"
  } >> "$BUILDKITE_ENV_FILE"
fi

# Collect all distribution artifacts into a flat artifacts/ directory for dractl.
# Excludes .sha512 and .asc files since dractl generates its own checksums.
mkdir -p artifacts
find "$WORKSPACE" -type f -path "*/build/distributions/*" \
  ! -name "*.sha512" \
  ! -name "*.asc" \
  -exec cp {} artifacts/ \;

if ! ls artifacts/* 1>/dev/null 2>&1; then
  echo "ERROR: no artifacts staged; expected files under build/distributions/" >&2
  exit 1
fi

echo "Staged artifacts:"
ls -1 artifacts/

# Emit the unified-release DRA processing trigger as a dynamic step so that
# the runtime-computed version is substituted correctly. depends_on: dra-prep
# ensures the trigger fires only after this step (including the dra-prep
# post-command hook that uploads to GCS) has fully completed.
echo --- Emitting unified-release DRA processing trigger
buildkite-agent pipeline upload << PIPELINE
steps:
  - label: ":pipeline: DRA processing for elasticsearch / ${ES_VERSION}${VERSION_SUFFIX} / ${WORKFLOW}"
    trigger: unified-release-dra-processing
    async: true
    depends_on: dra-prep
    build:
      env:
        DRA_PRODUCT_ID: elasticsearch
        DRA_STACK_VERSION: "${ES_VERSION}${VERSION_SUFFIX}"
        DRA_WORKFLOW: "${WORKFLOW}"
PIPELINE
