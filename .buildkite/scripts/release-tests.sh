#!/bin/bash

set -euo pipefail

curl_retrying() {
  curl --fail --location --show-error --silent \
    --retry 5 --retry-connrefused --retry-delay 2 --retry-max-time 120 "$@"
}

curl_with_retry() {
  local output_path="$1"
  local url="$2"

  echo "Downloading $(basename "${output_path}")"
  curl_retrying --retry-all-errors -o "${output_path}" "${url}"
}

# Resolve the id of the most recent published snapshot build of a DRA project.
#
# DRA publishes each build under an immutable "<build-id>/" prefix and updates a
# "latest/<version>-SNAPSHOT.json" pointer to name it. Only these two are
# guaranteed; the floating "<version>-SNAPSHOT/" prefix is not maintained by all
# projects, so artifacts must be fetched via the resolved build id. The pointer
# is keyed by version rather than branch on purpose: this script also runs on
# pull request branches, whose names carry no relation to the publishing project.
resolve_latest_build_id() {
  local project="$1"
  local version="$2"
  local pointer_url="https://artifacts-snapshot.elastic.co/${project}/latest/${version}-SNAPSHOT.json"
  local pointer_json
  local build_id

  if ! pointer_json=$(curl_retrying "${pointer_url}"); then
    echo "No ${project} snapshot build pointer exists at ${pointer_url}." >&2
    echo "If Elasticsearch was just bumped to ${version}, ${project} has not published that version yet." >&2
    return 1
  fi

  if ! build_id=$(jq -r '.build_id // empty' <<<"${pointer_json}"); then
    echo "Could not parse the ${project} build pointer at ${pointer_url}; it returned:" >&2
    echo "${pointer_json}" >&2
    return 1
  fi

  if [[ -z "${build_id}" ]]; then
    echo "The ${project} build pointer at ${pointer_url} names no build id; it returned:" >&2
    echo "${pointer_json}" >&2
    return 1
  fi

  echo "${build_id}"
}


# The build resolves dependencies without the -SNAPSHOT qualifier under
# -Dbuild.snapshot=false, so the snapshot artifacts are staged into a local ivy
# repository under their release names.

# Fetch beats artifacts
export ES_VERSION=$(grep 'elasticsearch' build-tools-internal/version.properties | awk '{print $3}')
export BEATS_DIR=$(pwd)/distribution/docker/build/artifacts/beats

mkdir -p ${BEATS_DIR}
curl_with_retry "${BEATS_DIR}/metricbeat-${ES_VERSION}-linux-x86_64.tar.gz" "https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/metricbeat/metricbeat-${ES_VERSION}-SNAPSHOT-linux-x86_64.tar.gz"
curl_with_retry "${BEATS_DIR}/metricbeat-${ES_VERSION}-linux-arm64.tar.gz" "https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/metricbeat/metricbeat-${ES_VERSION}-SNAPSHOT-linux-arm64.tar.gz"
curl_with_retry "${BEATS_DIR}/metricbeat-fips-${ES_VERSION}-linux-x86_64.tar.gz" "https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/metricbeat/metricbeat-fips-${ES_VERSION}-SNAPSHOT-linux-x86_64.tar.gz"
curl_with_retry "${BEATS_DIR}/metricbeat-fips-${ES_VERSION}-linux-arm64.tar.gz" "https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/metricbeat/metricbeat-fips-${ES_VERSION}-SNAPSHOT-linux-arm64.tar.gz"

curl_with_retry "${BEATS_DIR}/filebeat-${ES_VERSION}-linux-x86_64.tar.gz" "https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/filebeat/filebeat-${ES_VERSION}-SNAPSHOT-linux-x86_64.tar.gz"
curl_with_retry "${BEATS_DIR}/filebeat-${ES_VERSION}-linux-arm64.tar.gz" "https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/filebeat/filebeat-${ES_VERSION}-SNAPSHOT-linux-arm64.tar.gz"
curl_with_retry "${BEATS_DIR}/filebeat-fips-${ES_VERSION}-linux-x86_64.tar.gz" "https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/filebeat/filebeat-fips-${ES_VERSION}-SNAPSHOT-linux-x86_64.tar.gz"
curl_with_retry "${BEATS_DIR}/filebeat-fips-${ES_VERSION}-linux-arm64.tar.gz" "https://artifacts-snapshot.elastic.co/beats/${ES_VERSION}-SNAPSHOT/downloads/beats/filebeat/filebeat-fips-${ES_VERSION}-SNAPSHOT-linux-arm64.tar.gz"

# Fetch ML artifacts
ML_CPP_BUILD_ID=$(resolve_latest_build_id ml-cpp "${ES_VERSION}")
echo "ML_CPP_BUILD_ID=${ML_CPP_BUILD_ID}"

ML_CPP_DOWNLOADS="https://artifacts-snapshot.elastic.co/ml-cpp/${ML_CPP_BUILD_ID}/downloads/ml-cpp"
export ML_IVY_REPO=$(mktemp -d)
mkdir -p ${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}
curl_with_retry "${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}/ml-cpp-${ES_VERSION}-deps.zip" "${ML_CPP_DOWNLOADS}/ml-cpp-${ES_VERSION}-SNAPSHOT-deps.zip"
curl_with_retry "${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}/ml-cpp-${ES_VERSION}-nodeps.zip" "${ML_CPP_DOWNLOADS}/ml-cpp-${ES_VERSION}-SNAPSHOT-nodeps.zip"
curl_with_retry "${ML_IVY_REPO}/maven/org/elasticsearch/ml/ml-cpp/${ES_VERSION}/ml-cpp-${ES_VERSION}.zip" "${ML_CPP_DOWNLOADS}/ml-cpp-${ES_VERSION}-SNAPSHOT.zip"

.ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dbuild.snapshot=false -Dbuild.ml_cpp.repo=file://${ML_IVY_REPO} \
  -Dtests.jvm.argline=-Dbuild.snapshot=false -Dlicense.key=${WORKSPACE}/x-pack/license-tools/src/test/resources/public.key -Dbuild.id=deadbeef ${@:-functionalTests}
