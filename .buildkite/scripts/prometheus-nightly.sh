#!/bin/bash

# Runs one pinned corpus against this checkout and records a time series of compliance results.
set -euo pipefail
# Credentials must not appear in traced commands, including when invoked with bash -x.
set +x

for var in PROMCHECK_VER PROMCHECK_TEST_INSTANCE_TIMEOUT PROMCHECK_CORPUS PROMCHECK_RESULTS_NAMESPACE BUILDKITE_BUILD_ID; do
  [[ -n "${!var:-}" ]] || { echo "$var must be set" >&2; exit 1; }
done
[[ "$PROMCHECK_TEST_INSTANCE_TIMEOUT" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid promcheck timeout" >&2; exit 1; }
[[ "$PROMCHECK_RESULTS_NAMESPACE" =~ ^[a-z0-9_]+$ && ${#PROMCHECK_RESULTS_NAMESPACE} -le 100 ]] \
  || { echo "Invalid results namespace" >&2; exit 1; }
# The namespace selects both the destination and its dashboard filter, keeping experimental runs isolated.
readonly PROMCHECK_RESULTS_INDEX="synthetics-promql.support-$PROMCHECK_RESULTS_NAMESPACE"
readonly GITHUB_API_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
[[ -n "$GITHUB_API_TOKEN" ]] || { echo "GH_TOKEN or GITHUB_TOKEN must be set" >&2; exit 1; }

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
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "Unsupported architecture" >&2; exit 1 ;;
esac

echo "--- Downloading promcheck v$PROMCHECK_VER"
release=$(curl -fsSL --retry 3 --connect-timeout 30 --max-time 300 \
  -H "Authorization: Bearer $GITHUB_API_TOKEN" -H 'Accept: application/vnd.github+json' \
  "https://api.github.com/repos/elastic/promcheck/releases/tags/v$PROMCHECK_VER")
for name in "promcheck-linux-$arch" "$PROMCHECK_CORPUS.tar.gz"; do
  url=$(jq -er --arg name "$name" '.assets[] | select(.name == $name) | .url' <<< "$release")
  curl -fsSL --retry 3 --connect-timeout 30 --max-time 300 \
    -H "Authorization: Bearer $GITHUB_API_TOKEN" -H 'Accept: application/octet-stream' \
    -o "$work_dir/$name" "$url"
done
chmod +x "$work_dir/promcheck-linux-$arch"

revision=$(git rev-parse HEAD)
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "--- Running promcheck"
set +e
NO_COLOR=1 "$work_dir/promcheck-linux-$arch" --elasticsearch "$PWD" \
  --elasticsearch-timeout "${PROMCHECK_TEST_INSTANCE_TIMEOUT}s" --verbose \
  < "$work_dir/$PROMCHECK_CORPUS.tar.gz" 2>&1 | tee "$OUTPUT_DIR/promcheck.log"
run_status=("${PIPESTATUS[@]}")
set -e
rc=${run_status[0]}
# Exit 1 represents query failures; other codes represent an incomplete run.
(( rc == 0 || rc == 1 )) || { echo "promcheck failed to complete (exit $rc)" >&2; exit 1; }
(( run_status[1] == 0 )) || { echo "Could not save promcheck log" >&2; exit 1; }

# Anchor the full verdict to reject partial output and validate the counts before indexing.
jq -Rn '
  [inputs | capture("^(?<total>[0-9]+) inspected: (?<match>[0-9]+) match(?: \\([0-9]+ bias-forgiven\\))?, (?<mismatch>[0-9]+) mismatch, (?<http4xx>[0-9]+) 4xx, (?<http5xx>[0-9]+) 5xx(?:, (?<error>[0-9]+) error)?(?:, (?<response_filtered>[0-9]+) response-filtered)?(?:, (?<flaky>[0-9]+) flaky)?$")]
  | last
  | if . == null then error("No complete promcheck verdict") else . end
  | with_entries(.value |= ((. // "0") | tonumber))
  | if .total > 0 and .total == (.match + .mismatch + .http4xx + .http5xx + .error + .response_filtered + .flaky)
    then . else error("Invalid promcheck counts") end
' < "$OUTPUT_DIR/promcheck.log" > "$OUTPUT_DIR/counts.json"
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
      input_total: $c.total, executed_total: $c.total, executed_success: $c.match,
      executed_failure: $c.mismatch, executed_error: ($c.http4xx + $c.http5xx + $c.error),
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
