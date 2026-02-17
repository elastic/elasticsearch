#!/bin/bash
#
# PromQL Query Coverage Benchmark
#
# Compares PromQL query translation coverage between the PR branch and base branch
# to detect regressions. Uses real-world queries extracted from
# https://github.com/elastic/grafana-dashboards-analysis
#

set -euo pipefail

GIT_QUERY_BENCH_REPO="elastic/grafana-dashboards-analysis"
QUERY_BENCH_DIR="/tmp/${GIT_QUERY_BENCH_REPO}"
SUMMARY_TXT="${QUERY_BENCH_DIR}/output/summary.txt"

cleanup() {
  rm -rf "${QUERY_BENCH_DIR}"
}
trap cleanup EXIT

echo "--- Setup"
mkdir -p "${QUERY_BENCH_DIR}/output"

if [ -z "${GH_TOKEN:-}" ]; then
  echo "Error: GH_TOKEN not set"
  exit 1
fi

echo "--- Cloning ${GIT_QUERY_BENCH_REPO}"
git clone --depth 1 --single-branch \
  "https://${GH_TOKEN}@github.com/${GIT_QUERY_BENCH_REPO}.git" \
  "$QUERY_BENCH_DIR"

mapfile -t QUERY_FILES < <(find "${QUERY_BENCH_DIR}/results" -name "*.csv" -type f | sort)
if [ ${#QUERY_FILES[@]} -eq 0 ]; then
  echo "Error: No CSV files found in ${QUERY_BENCH_DIR}/results"
  exit 1
fi
echo "Found ${#QUERY_FILES[@]} query file(s)"

echo "--- Fetching base branch"
git fetch origin "$BUILDKITE_PULL_REQUEST_BASE_BRANCH" --depth=1

PR_COMMIT="$BUILDKITE_COMMIT"
BASE_REF="origin/$BUILDKITE_PULL_REQUEST_BASE_BRANCH"

TOTAL_REGRESSIONS=0
FAILED_FILES=()

for QUERIES_FILE in "${QUERY_FILES[@]}"; do
  FILENAME=$(basename "$QUERIES_FILE" .csv)
  BASE_REPORT="${QUERY_BENCH_DIR}/output/${FILENAME}_base.json"
  CURRENT_REPORT="${QUERY_BENCH_DIR}/output/${FILENAME}_current.json"

  echo "--- $FILENAME"

  # Run on base branch (minimal output)
  git checkout --quiet "$BASE_REF"
  if ! .ci/scripts/run-gradle.sh :x-pack:plugin:esql:analyzePromqlQueries \
    -PqueriesFile="$QUERIES_FILE" \
    -PoutputFile="$BASE_REPORT" \
    -Pverbose=false \
    --quiet 2>/dev/null; then
    echo "Skipping (analyzer unavailable on base branch)"
    continue
  fi

  # Run on PR branch (minimal output)
  git checkout --quiet "$PR_COMMIT"
  if ! .ci/scripts/run-gradle.sh :x-pack:plugin:esql:analyzePromqlQueries \
    -PqueriesFile="$QUERIES_FILE" \
    -PoutputFile="$CURRENT_REPORT" \
    -Pverbose=false \
    --quiet; then
    echo "Error: analyzer failed on PR branch"
    exit 1
  fi

  # Compare overall numbers
  BASE_SUCCESS=$(jq '.successful' "$BASE_REPORT")
  CURRENT_SUCCESS=$(jq '.successful' "$CURRENT_REPORT")
  BASE_TOTAL=$(jq '.total' "$BASE_REPORT")
  CURRENT_TOTAL=$(jq '.total' "$CURRENT_REPORT")

  DIFF=$((CURRENT_SUCCESS - BASE_SUCCESS))

  if [ "$DIFF" -lt 0 ]; then
    STATUS="REGRESSION"
    TOTAL_REGRESSIONS=$((TOTAL_REGRESSIONS + 1))
    FAILED_FILES+=("$FILENAME")
    echo "^^^ +++"
  elif [ "$DIFF" -gt 0 ]; then
    STATUS="IMPROVED (+$DIFF)"
  else
    STATUS="OK"
  fi

  echo "$STATUS: $BASE_SUCCESS/$BASE_TOTAL -> $CURRENT_SUCCESS/$CURRENT_TOTAL"
  echo "$FILENAME: $STATUS base=$BASE_SUCCESS/$BASE_TOTAL current=$CURRENT_SUCCESS/$CURRENT_TOTAL" >> "$SUMMARY_TXT"
done

echo ""
echo "--- Summary"
echo ""

if [ -f "$SUMMARY_TXT" ]; then
  cat "$SUMMARY_TXT"
  echo ""
fi

if [ "$TOTAL_REGRESSIONS" -gt 0 ]; then
  echo "FAILED: Regressions detected in: ${FAILED_FILES[*]}"
  exit 1
fi

echo "SUCCESS: No regressions detected"
