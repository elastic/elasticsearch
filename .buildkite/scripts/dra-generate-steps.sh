#!/bin/bash

# Resolves the DRA version and dependency build IDs, then uploads the DRA build
# step with the dra-prep plugin attached. The plugin config needs these values
# as literals (Buildkite interpolates plugin config at upload time), and running
# the plugin in the build step itself avoids handing the artifacts over through
# the Buildkite artifact store.

set -euo pipefail

WORKFLOW="${DRA_WORKFLOW:-snapshot}"
BRANCH="${BUILDKITE_BRANCH:-}"

# Don't publish main branch to staging
if [[ ("$BRANCH" == "main" || "$BRANCH" == *.x) && "$WORKFLOW" == "staging" ]]; then
  exit 0
fi

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
BASE_VERSION="$ES_VERSION"
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

# Fail here rather than after starting the build machine
: "${BEATS_BUILD_ID:?could not resolve the beats build ID}"
: "${ML_CPP_BUILD_ID:?could not resolve the ml-cpp build ID}"

echo --- Uploading DRA pipeline steps
cat <<EOF | buildkite-agent pipeline upload
steps:
  - label: ":package: DRA build ${ES_VERSION}${VERSION_SUFFIX}"
    key: dra-build
    command: .buildkite/scripts/dra-workflow.sh
    env:
      BEATS_BUILD_ID: "${BEATS_BUILD_ID}"
      ML_CPP_BUILD_ID: "${ML_CPP_BUILD_ID}"
      USE_MAVEN_GPG: "true"
      USE_MAVEN_S3_CREDENTIALS: "true"
      USE_PROD_DOCKER_CREDENTIALS: "true"
    agents:
      provider: gcp
      image: family/elasticsearch-ubuntu-2204
      machineType: custom-32-98304
      localSsds: 1
      localSsdInterface: nvme
      diskSizeGb: 350
    plugins:
      - elastic/dra-prep#v0.1.8:
          product_id: elasticsearch
          stack_version: "${BASE_VERSION}"
          workflow: "${WORKFLOW}"
          dependencies:
            - "beats:https://artifacts-${WORKFLOW}.elastic.co/beats/${BEATS_BUILD_ID}/manifest-${ES_VERSION}${VERSION_SUFFIX}.json"
            - "ml-cpp:https://artifacts-${WORKFLOW}.elastic.co/ml-cpp/${ML_CPP_BUILD_ID}/manifest-${ES_VERSION}${VERSION_SUFFIX}.json"

  - label: ":pipeline: DRA processing for elasticsearch / ${BASE_VERSION} / ${WORKFLOW}"
    trigger: unified-release-dra-processing
    async: true
    depends_on: dra-build
    build:
      env:
        DRA_PRODUCT_ID: elasticsearch
        DRA_STACK_VERSION: "${BASE_VERSION}"
        DRA_WORKFLOW: "${WORKFLOW}"

  - trigger: elasticsearch-hadoop-dra-workflow
    label: "Trigger DRA staging workflow for elasticsearch-hadoop"
    async: true
    depends_on: dra-build
    build:
      branch: "${BUILDKITE_BRANCH}"
      env:
        DRA_WORKFLOW: staging
        USE_PROD_DOCKER_CREDENTIALS: "true"
    if: build.env('DRA_WORKFLOW') == 'staging'
EOF
