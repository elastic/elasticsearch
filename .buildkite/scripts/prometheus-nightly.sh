#!/bin/bash

# Runs one pinned corpus against this checkout and records a time series of compliance results.
set -euo pipefail
# Credentials must not appear in traced commands, including when invoked with bash -x.
set +x

source "$(dirname "${BASH_SOURCE[0]}")/promcheck-common.sh"

for var in PROMCHECK_RESULTS_NAMESPACE BUILDKITE_BUILD_ID; do
  [[ -n "${!var:-}" ]] || { echo "$var must be set" >&2; exit 1; }
done
promcheck_configure
[[ "$PROMCHECK_RESULTS_NAMESPACE" =~ ^[a-z0-9_]+$ && ${#PROMCHECK_RESULTS_NAMESPACE} -le 100 ]] \
  || { echo "Invalid results namespace" >&2; exit 1; }
# The namespace selects both the destination and its dashboard filter, keeping experimental runs isolated.
readonly PROMCHECK_RESULTS_INDEX="synthetics-promql.support-$PROMCHECK_RESULTS_NAMESPACE"
# CI provides Vault access. Environment overrides also allow an explicitly configured destination.
if [[ -z "${PROMCHECK_RESULTS_URL:-}" || -z "${PROMCHECK_RESULTS_API_KEY:-}" ]]; then
  : "${PROMCHECK_RESULTS_SECRET:?Set the results URL and API key, or a Vault secret path}"
  secret=$(vault read -format=json "$PROMCHECK_RESULTS_SECRET")
  PROMCHECK_RESULTS_URL=${PROMCHECK_RESULTS_URL:-$(jq -er '.data.url' <<< "$secret")}
  PROMCHECK_RESULTS_API_KEY=${PROMCHECK_RESULTS_API_KEY:-$(jq -er '.data.api_key' <<< "$secret")}
  unset secret
fi
[[ "$PROMCHECK_RESULTS_URL" == https://* ]] || { echo "Results URL must use HTTPS" >&2; exit 1; }

readonly OUTPUT_DIR="${PROMCHECK_OUTPUT_DIR:-/tmp/prometheus-nightly-output}"
mkdir -p "$OUTPUT_DIR"
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
echo "--- Downloading promcheck v$PROMCHECK_VER"
promcheck_download "$work_dir/promcheck" "$work_dir/$PROMCHECK_CORPUS.tar.gz"

revision=$(git rev-parse HEAD)
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "--- Running promcheck"
promcheck_run "$PWD" "$work_dir/promcheck" "$work_dir/$PROMCHECK_CORPUS.tar.gz" "$OUTPUT_DIR/promcheck.log" \
  || { echo "promcheck failed to complete; see run artifacts" >&2; exit 1; }
promcheck_summary "$OUTPUT_DIR/promcheck.log" > "$OUTPUT_DIR/counts.json"
jq -n --slurpfile counts "$OUTPUT_DIR/counts.json" \
  --arg timestamp "$started_at" --arg commit "$revision" --arg branch "${BUILDKITE_BRANCH:-}" \
  --arg version "$PROMCHECK_VER" --arg corpus "$PROMCHECK_CORPUS" --arg namespace "$PROMCHECK_RESULTS_NAMESPACE" \
  --arg build_id "$BUILDKITE_BUILD_ID" --arg build_url "${BUILDKITE_BUILD_URL:-}" \
  --arg pipeline "${BUILDKITE_PIPELINE_SLUG:-elastic-prometheus-nightly}" \
  --argjson build_number "${BUILDKITE_BUILD_NUMBER:-0}" \
  '$counts[0] as $c | {
    "@timestamp": $timestamp, status: "ok", dataset: $corpus,
    data_stream: {type: "synthetics", dataset: "promql.support", namespace: $namespace},
    git: {commit: $commit, branch: $branch}, promcheck: {version: $version},
    buildkite: {build_id: $build_id, build_number: $build_number, build_url: $build_url, pipeline: $pipeline},
    compliance: {
      input_total: $c.total, executed_total: $c.inspected, executed_success: $c.match,
      executed_failure: $c.mismatch, executed_error: $c.errors,
      executed_skip: ($c.response_filtered + $c.flaky), success_pct: ((1000 * $c.match / $c.total | round) / 10)
    }}' > "$OUTPUT_DIR/result.json"

echo "--- Indexing promcheck results"
# Overview stores compliance history in a data stream, which requires create operations.
# A stable build/corpus ID lets an upload retry recognize an already-created result.
doc_id=$(jq -rn --arg id "$BUILDKITE_BUILD_ID-$PROMCHECK_CORPUS" '$id | @uri')
http_status=$(curl -sS --retry 3 --connect-timeout 30 --max-time 120 -X PUT \
  -H "Authorization: ApiKey $PROMCHECK_RESULTS_API_KEY" -H 'Content-Type: application/json' \
  --data-binary "@$OUTPUT_DIR/result.json" -o "$OUTPUT_DIR/index-response.json" --write-out '%{http_code}' \
  "${PROMCHECK_RESULTS_URL%/}/$PROMCHECK_RESULTS_INDEX/_create/$doc_id")
case "$http_status" in
  201) jq -e '.result == "created"' "$OUTPUT_DIR/index-response.json" >/dev/null ;;
  409) jq -e '.error.type == "version_conflict_engine_exception"' "$OUTPUT_DIR/index-response.json" >/dev/null ;;
  *) echo "Results upload failed (HTTP $http_status); see index-response.json" >&2; exit 1 ;;
esac
echo "Indexed $PROMCHECK_CORPUS results for $revision"
