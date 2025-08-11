#!/bin/bash

set -euo pipefail

# Fetch ML artifacts
export ES_VERSION=$(grep 'elasticsearch' build-tools-internal/version.properties | awk '{print $3}')
export ML_IVY_REPO=$(mktemp -d)
mkdir -p ${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}
curl --fail -o "${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}/ml-cpp-${ES_VERSION}.zip" https://artifacts-snapshot.elastic.co/ml-cpp/${ES_VERSION}-SNAPSHOT/downloads/ml-cpp/ml-cpp-${ES_VERSION}-SNAPSHOT.zip

.ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dbuild.snapshot=false -Dbuild.ml_cpp.repo=file://${ML_IVY_REPO} \
  -Dtests.jvm.argline=-Dbuild.snapshot=false -Dlicense.key=${WORKSPACE}/x-pack/license-tools/src/test/resources/public.key -Dbuild.id=deadbeef assemble functionalTests
