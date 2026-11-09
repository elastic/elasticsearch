#!/bin/bash

# Runs one pinned corpus against this checkout and records a time series of compliance results.
set -euo pipefail
# Credentials must not appear in traced commands, including when invoked with bash -x.
set +x

for var in PROMCHECK_VER PROMCHECK_TEST_INSTANCE_TIMEOUT PROMCHECK_CORPUS PROMCHECK_RESULTS_INDEX BUILDKITE_BUILD_ID; do
  [[ -n "${!var:-}" ]] || { echo "$var must be set" >&2; exit 1; }
done
[[ "$PROMCHECK_TEST_INSTANCE_TIMEOUT" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid promcheck timeout" >&2; exit 1; }
[[ "$PROMCHECK_RESULTS_INDEX" =~ ^[a-z0-9][a-z0-9._-]*$ ]] || { echo "Invalid results index" >&2; exit 1; }
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
start_seconds=$SECONDS
echo "--- Running promcheck"
set +e
NO_COLOR=1 "$work_dir/promcheck-linux-$arch" --elasticsearch "$PWD" \
  --elasticsearch-timeout "${PROMCHECK_TEST_INSTANCE_TIMEOUT}s" --verbose \
  < "$work_dir/$PROMCHECK_CORPUS.tar.gz" 2>&1 | tee "$OUTPUT_DIR/promcheck.log"
run_status=("${PIPESTATUS[@]}")
set -e
duration=$((SECONDS - start_seconds))
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
' < "$OUTPUT_DIR/promcheck.log" > "$work_dir/counts.json"
jq -n --slurpfile counts "$work_dir/counts.json" \
  --arg timestamp "$started_at" --arg commit "$revision" --arg branch "${BUILDKITE_BRANCH:-}" \
  --arg version "$PROMCHECK_VER" --arg corpus "$PROMCHECK_CORPUS" \
  --arg build_id "$BUILDKITE_BUILD_ID" --arg build_url "${BUILDKITE_BUILD_URL:-}" \
  --arg job_id "${BUILDKITE_JOB_ID:-}" --argjson duration "$duration" --argjson exit_code "$rc" \
  '{"@timestamp": $timestamp, commit: $commit, branch: $branch, promcheck_version: $version,
    corpus: $corpus, build_id: $build_id, build_url: $build_url, job_id: $job_id,
    duration_seconds: $duration, exit_code: $exit_code, counts: $counts[0]}' > "$OUTPUT_DIR/result.json"

echo "--- Indexing promcheck results"
# A build/corpus ID makes retries of this upload replace the document rather than duplicate it.
doc_id=$(jq -rn --arg id "$BUILDKITE_BUILD_ID-$PROMCHECK_CORPUS" '$id | @uri')
curl -fsS --retry 3 --connect-timeout 30 --max-time 120 -X PUT \
  -H "Authorization: ApiKey $PROMCHECK_RESULTS_API_KEY" -H 'Content-Type: application/json' \
  --data-binary "@$OUTPUT_DIR/result.json" \
  "${PROMCHECK_RESULTS_URL%/}/$PROMCHECK_RESULTS_INDEX/_doc/$doc_id" > "$OUTPUT_DIR/index-response.json"
jq -e '.result == "created" or .result == "updated"' "$OUTPUT_DIR/index-response.json" >/dev/null
echo "Indexed $PROMCHECK_CORPUS results for $revision"
