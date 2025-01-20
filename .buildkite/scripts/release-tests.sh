#!/bin/bash

set -euo pipefail

# Fetch beats artifacts
export ES_VERSION=$(grep 'elasticsearch' build-tools-internal/version.properties | awk '{print $3}')
export BEATS_DIR=$(pwd)/distribution/docker/build/artifacts/beats

mkdir -p ${BEATS_DIR}
curl --fail -o "${BEATS_DIR}/metricbeat-${ES_VERSION}-linux-x86_64.tar.gz" https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/metricbeat/metricbeat-${ES_VERSION}-SNAPSHOT-linux-x86_64.tar.gz
curl --fail -o "${BEATS_DIR}/metricbeat-${ES_VERSION}-linux-arm64.tar.gz" https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/metricbeat/metricbeat-${ES_VERSION}-SNAPSHOT-linux-arm64.tar.gz
curl --fail -o "${BEATS_DIR}/filebeat-${ES_VERSION}-linux-x86_64.tar.gz" https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/metricbeat/metricbeat-${ES_VERSION}-SNAPSHOT-linux-x86_64.tar.gz
curl --fail -o "${BEATS_DIR}/filebeat-${ES_VERSION}-linux-arm64.tar.gz" https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/filebeat/filebeat-${ES_VERSION}-SNAPSHOT-linux-arm64.tar.gz

# Fetch ML artifacts
export ML_IVY_REPO=$(mktemp -d)
mkdir -p ${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}
curl --fail -o "${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}/ml-cpp-${ES_VERSION}-deps.zip" https://artifacts-snapshot.elastic.co/ml-cpp/${ES_VERSION}-SNAPSHOT/downloads/ml-cpp/ml-cpp-${ES_VERSION}-SNAPSHOT-deps.zip
curl --fail -o "${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}/ml-cpp-${ES_VERSION}-nodeps.zip" https://artifacts-snapshot.elastic.co/ml-cpp/${ES_VERSION}-SNAPSHOT/downloads/ml-cpp/ml-cpp-${ES_VERSION}-SNAPSHOT-nodeps.zip
curl --fail -o "${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}/ml-cpp-${ES_VERSION}.zip" https://artifacts-snapshot.elastic.co/ml-cpp/${ES_VERSION}-SNAPSHOT/downloads/ml-cpp/ml-cpp-${ES_VERSION}-SNAPSHOT.zip

.ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dbuild.snapshot=false -Dbuild.ml_cpp.repo=file://${ML_IVY_REPO} \
  -Dtests.jvm.argline=-Dbuild.snapshot=false -Dlicense.key=${WORKSPACE}/x-pack/license-tools/src/test/resources/public.key -Dbuild.id=deadbeef assemble functionalTests
