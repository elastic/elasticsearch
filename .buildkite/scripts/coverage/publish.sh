#!/bin/bash
#
# Aggregation: merges the per-layer legs' exec data into the union report. The legs upload only
# raw exec data; this script rebuilds everything else from it:
#
#   1. zero-gate the exec data (nothing broken gets published)
#   2. compile the measured projects' main classes (remote build cache makes this cheap)
#   3. per-layer reports + the merged (union) report
#   4. summary, Buildkite annotation, S3 publish
#
# The PR pipeline (test-coverage.yml) does not run this - each of its legs reports its own layer.
# Run it as a Buildkite step after the legs (it downloads their exec artifacts itself), or
# locally against an already-populated COVERAGE_OUTPUT.
#
# S3 publishing needs one-time ops setup: the bucket, and AWS keys at the Vault path below. Until
# that exists the step still produces reports and the annotation - it says so instead of the link.
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"
# shellcheck source=lib.sh
source "$ROOT/.buildkite/scripts/coverage/lib.sh"

PROJECTS="${COVERAGE_PROJECTS:-:x-pack:plugin:esql-datasource-*}"
INCLUDES="${COVERAGE_INCLUDES:-org.elasticsearch.xpack.esql.*:org.elasticsearch.compute.*:org.elasticsearch.arrow.*}"
OUT="${COVERAGE_OUTPUT:-$ROOT/build/coverage}"
BUCKET="${COVERAGE_BUCKET:-esql-coverage-reports}"
VAULT_PATH="${COVERAGE_VAULT_PATH:-secret/ci/elastic-elasticsearch/esql-coverage-s3}"
SHA="${BUILDKITE_COMMIT:-local}"
GRADLE="${GRADLEW:-./gradlew}"

LIB="$OUT/lib"
EXEC="$OUT/exec"
REPORT="$OUT/report"
mkdir -p "$LIB" "$EXEC" "$REPORT"

coverage_fetch_tools "$LIB"

# On a fresh agent nothing is checked out under $EXEC yet - the legs' exec data lives in their
# Buildkite artifacts. Fetch it before gating; a local run that already has exec data skips this.
if [[ -n "${BUILDKITE:-}" && -z "$(find "$EXEC" -name '*.exec' 2>/dev/null | head -1)" ]]; then
  echo "--- downloading exec artifacts from the test legs"
  buildkite-agent artifact download "build/coverage/exec/**/*.exec" .
fi

# --- gate first: nothing broken gets published ---------------------------------------------------

"$ROOT/.buildkite/scripts/coverage/check-nonzero.sh" "$EXEC" "$CLI"

# --- classfiles ----------------------------------------------------------------------------------
#
# This agent ran no tests, so the measured projects have to be compiled here. The legs populated
# the remote build cache, so this is mostly cache hits.

compile_tasks=()
while IFS= read -r p; do
  [[ -n "$p" ]] && compile_tasks+=("$p:compileJava")
done < <(coverage_projects "$ROOT" "$PROJECTS")
if [[ ${#compile_tasks[@]} -eq 0 ]]; then
  echo "no projects match $PROJECTS" >&2
  exit 1
fi
echo "--- compiling ${#compile_tasks[@]} projects for report classfiles"
# shellcheck disable=SC2086
$GRADLE --continue "${compile_tasks[@]}" || true

PATH_ARGS=()
while IFS= read -r a; do PATH_ARGS+=("$a"); done < <(coverage_report_path_args "$ROOT" "$PROJECTS" "$INCLUDES")
if [[ ${#PATH_ARGS[@]} -eq 0 ]]; then
  echo "no compiled classes found for pattern $PROJECTS - cannot build a report" >&2
  exit 1
fi

# --- reports: per layer, and the union across all legs -------------------------------------------

report_one() {
  local name="$1"; shift
  [[ $# -eq 0 ]] && return 0
  mkdir -p "$REPORT/$name"
  java -jar "$CLI" report "$@" \
    "${PATH_ARGS[@]}" \
    --html "$REPORT/$name/html" \
    --xml "$REPORT/$name/coverage.xml" \
    --csv "$REPORT/$name/coverage.csv" \
    --name "$name" >/dev/null
}

all_execs=()
for layer_dir in "$EXEC"/*/; do
  [[ -d "$layer_dir" ]] || continue
  layer=$(basename "$layer_dir")
  execs=()
  while IFS= read -r f; do execs+=("$f"); all_execs+=("$f"); done < <(find "$layer_dir" -name '*.exec' 2>/dev/null)
  report_one "$layer" ${execs[@]+"${execs[@]}"}
done

if [[ ${#all_execs[@]} -eq 0 ]]; then
  echo "no exec data downloaded - nothing to publish" >&2
  exit 1
fi
java -jar "$CLI" merge "${all_execs[@]}" --destfile "$OUT/merged.exec" >/dev/null
report_one merged "$OUT/merged.exec"

SUMMARY=$("$ROOT/.buildkite/scripts/coverage/summarise.sh" "$REPORT" | tee "$OUT/summary.txt")
echo "$SUMMARY"

# --- publish + annotate --------------------------------------------------------------------------

LINK="report is in this step's artifacts (build/coverage/report/)"
if [[ "${COVERAGE_SKIP_PUBLISH:-}" == "1" ]]; then
  echo "--- S3 publish skipped (COVERAGE_SKIP_PUBLISH=1)"
elif [[ -n "${BUILDKITE:-}" ]]; then
  if creds=$(vault read -format=json "$VAULT_PATH" 2>/dev/null); then
    AWS_ACCESS_KEY_ID=$(jq -r '.data.access_key' <<< "$creds")
    AWS_SECRET_ACCESS_KEY=$(jq -r '.data.secret_key' <<< "$creds")
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    aws s3 sync "$REPORT" "s3://$BUCKET/$SHA/" --only-show-errors
    aws s3 sync "$REPORT" "s3://$BUCKET/latest/" --only-show-errors
    LINK="[Browse the report](https://$BUCKET.s3.amazonaws.com/$SHA/merged/html/index.html)"
  else
    echo "--- S3 publish skipped: no credentials at $VAULT_PATH (one-time ops setup, see README)"
  fi
fi

if [[ -n "${BUILDKITE:-}" ]] && command -v buildkite-agent >/dev/null; then
  printf '### Coverage (%s @ %.12s)\n\n```\n%s\n```\n\n%s\n' "$PROJECTS" "$SHA" "$SUMMARY" "$LINK" \
    | buildkite-agent annotate --style info --context coverage
fi
