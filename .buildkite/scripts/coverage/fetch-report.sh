#!/bin/bash
#
# Rebuild the merged coverage report on your own machine from a finished CI build.
#
# The CI legs upload raw execution data as Buildkite artifacts. This pulls those artifacts down and
# hands them to publish.sh, which does the merge and the reports. Use it when the merge step did not
# run, when you want the report for a build that has aged out of the agent, or when you want the
# numbers without opening Buildkite at all.
#
#   fetch-report.sh --pr 155367                 # newest coverage build on that PR
#   fetch-report.sh 167811                      # a specific build number
#   fetch-report.sh --pr 155367 ~/cov           # somewhere other than build/coverage
#
# Needs a Buildkite API token with read_builds + read_artifacts, from $BUILDKITE_API_TOKEN or
# ~/.config/bk.yaml. It does NOT need the graphql scope: `bk artifacts download` wants that and we
# do not have it, so this goes through the REST API and fetches each artifact's own download_url.
#
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

ORG=${BUILDKITE_ORG:-elastic}
PIPELINE=${BUILDKITE_PIPELINE_SLUG:-elasticsearch-pull-request}
API="https://api.buildkite.com/v2/organizations/$ORG/pipelines/$PIPELINE"

# `\?` is a GNU sed extension and this is meant to run on a laptop as much as on an agent, so the
# comment prefix comes off with two portable expressions instead.
usage() { sed -n '3,17p' "${BASH_SOURCE[0]}" | sed -e 's/^# //' -e 's/^#$//'; exit "${1:-1}"; }

PR="" BUILD=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --pr) PR="${2:?--pr needs a number}"; shift 2 ;;
    -h|--help) usage 0 ;;
    -*) echo "unknown option $1" >&2; usage ;;
    *) if [[ -z "$BUILD" ]]; then BUILD="$1"; else OUT="$1"; fi; shift ;;
  esac
done
OUT="${OUT:-$ROOT/build/coverage}"
[[ -n "$PR" || -n "$BUILD" ]] || usage

# The bk CLI keeps a token in two places and they are not interchangeable: `bk configure` writes an
# API token to ~/.config/bk.yaml, while `bk auth login` stores an OAuth token in the keychain that
# the REST API does not accept as a bearer. Rather than encode which one is right - it has changed
# between bk versions - try each and keep the first that actually authenticates.
TOKEN=""
for candidate in \
  "${BUILDKITE_API_TOKEN:-}" \
  "$(grep -oE '[A-Za-z0-9_-]{30,}' "$HOME/.config/bk.yaml" 2>/dev/null | head -1)" \
  "$(security find-generic-password -s buildkite-cli -w 2>/dev/null || true)"
do
  [[ -n "$candidate" ]] || continue
  if [[ "$(curl -sS -o /dev/null -w '%{http_code}' \
        -H "Authorization: Bearer $candidate" https://api.buildkite.com/v2/user)" == "200" ]]; then
    TOKEN="$candidate"
    break
  fi
done
if [[ -z "$TOKEN" ]]; then
  echo "no working Buildkite API token." >&2
  echo "Re-authenticate with:" >&2
  echo "  bk auth logout && bk auth login --scopes \"read_user read_organizations read_pipelines read_builds read_build_logs read_artifacts write_builds read_agents\"" >&2
  exit 1
fi
api() { curl -sSfL -H "Authorization: Bearer $TOKEN" "$@"; }

# --- resolve the build ---------------------------------------------------------------------------

if [[ -z "$BUILD" ]]; then
  echo "--- finding the newest build on PR $PR"
  # Buildkite has no server-side filter for pull_request_id, so scan recent builds newest-first and
  # take the first that both belongs to the PR and actually ran coverage. A PR has many builds and
  # only the ones where the label was applied carry coverage jobs.
  BUILD=$(api "$API/builds?per_page=100" | python3 -c '
import json,sys
pr=sys.argv[1]
for b in json.load(sys.stdin):
    if str((b.get("pull_request") or {}).get("id")) != pr: continue
    if any("test-coverage" in (j.get("name") or "") for j in b.get("jobs") or []):
        print(b["number"]); break
' "$PR")
  [[ -n "$BUILD" ]] || { echo "no recent build on PR $PR ran the coverage pipeline" >&2; exit 1; }
  echo "    build $BUILD"
fi

# --- download the execution data -----------------------------------------------------------------

EXEC="$OUT/exec"
mkdir -p "$EXEC"

# Artifacts are listed per job, never per build: a build's artifact list is paginated and a JaCoCo
# HTML report is thousands of files, so the .exec files get lost in the paging. Per job the sets are
# small enough to page through reliably.
jobs=$(api "$API/builds/$BUILD" | python3 -c '
import json,sys
for j in json.load(sys.stdin).get("jobs") or []:
    if "test-coverage" in (j.get("name") or "") and j.get("id"):
        print(j["id"], (j.get("name") or "").replace(" ",""))
')
[[ -n "$jobs" ]] || { echo "build $BUILD has no test-coverage jobs" >&2; exit 1; }

n=0 failed=0
while read -r job_id job_name; do
  [[ -n "$job_id" ]] || continue
  echo "--- $job_name"
  page=1
  while :; do
    # Emits a count line first, then the .exec artifacts. Paging has to stop on "the API returned
    # nothing", not on "this page held no .exec": a leg uploads its HTML report too, so a page can
    # legitimately be all report files with more .exec data on the page after it.
    listing=$(api "$API/builds/$BUILD/jobs/$job_id/artifacts?per_page=100&page=$page" | python3 -c '
import json,sys
arts=json.load(sys.stdin)
print(len(arts))
for a in arts:
    if a.get("path","").endswith(".exec"):
        print(a["path"], a["download_url"])
')
    total=$(head -1 <<< "$listing")
    batch=$(tail -n +2 <<< "$listing")
    [[ "$total" =~ ^[0-9]+$ ]] || { echo "unreadable artifact listing for $job_name" >&2; exit 1; }
    (( total > 0 )) || break
    expected=()
    while read -r path url; do
      [[ -n "$path" ]] || continue
      # Artifacts keep the path they were uploaded under, which already carries the layer:
      # build/coverage/exec/<layer>/<task>.exec. Preserve it so publish.sh sees the layers.
      dest="$OUT/${path#build/coverage/}"
      mkdir -p "$(dirname "$dest")"
      curl -sSL -H "Authorization: Bearer $TOKEN" "$url" -o "$dest" &
      expected+=("$dest")
    done <<< "$batch"
    wait
    # Downloads run in parallel, so a failure shows up as a missing or empty file rather than a
    # non-zero exit. Check for it here: an empty .exec sails through the merge and produces a
    # report that is quietly missing a whole suite.
    for dest in ${expected[@]+"${expected[@]}"}; do
      if [[ ! -s "$dest" ]]; then
        echo "download produced nothing for $dest - is the token still valid?" >&2
        rm -f "$dest"
        failed=$((failed+1))
      else
        n=$((n+1))
      fi
    done
    page=$((page+1))
  done
done <<< "$jobs"

echo "--- downloaded $n exec files to $EXEC"
if [[ $failed -gt 0 ]]; then
  echo "$failed artifact(s) failed to download; the report would be incomplete" >&2
  exit 1
fi
[[ $n -gt 0 ]] || { echo "no execution data on build $BUILD - did the legs finish?" >&2; exit 1; }

# --- merge and report ----------------------------------------------------------------------------
#
# publish.sh does the rest: it gates the exec data, compiles the measured projects for classfiles,
# and writes the per-layer and merged reports. Told not to publish, since there is nowhere to
# publish to from a laptop.
#
# The scope has to come from the pipeline, not from publish.sh's own defaults. Those defaults are
# narrower than what this pipeline measures, so taking them would rebuild a different report from
# the same execution data and quietly report different numbers than CI did.
PIPELINE_YML="$ROOT/.buildkite/pipelines/pull-request/test-coverage.yml"
scope_from_pipeline() {
  local key="$1"
  [[ -f "$PIPELINE_YML" ]] || return 0
  python3 - "$PIPELINE_YML" "$key" <<'PY'
import re, sys
key = sys.argv[2]
# Deliberately a line match rather than a YAML parse: pyyaml is not guaranteed on a dev machine and
# this is a flat `KEY: "value"` under the top-level env block.
for line in open(sys.argv[1]):
    m = re.match(rf'\s+{re.escape(key)}:\s*"?([^"\n]+)"?\s*$', line)
    if m:
        print(m.group(1).strip()); break
PY
}
export COVERAGE_PROJECTS="${COVERAGE_PROJECTS:-$(scope_from_pipeline COVERAGE_PROJECTS)}"
export COVERAGE_INCLUDES="${COVERAGE_INCLUDES:-$(scope_from_pipeline COVERAGE_INCLUDES)}"
if [[ -z "$COVERAGE_PROJECTS" || -z "$COVERAGE_INCLUDES" ]]; then
  echo "could not read the measured scope from $PIPELINE_YML; set COVERAGE_PROJECTS and COVERAGE_INCLUDES" >&2
  exit 1
fi
echo "--- scope: $COVERAGE_PROJECTS"

COVERAGE_OUTPUT="$OUT" COVERAGE_SKIP_PUBLISH=1 BUILDKITE= \
  "$ROOT/.buildkite/scripts/coverage/publish.sh"

echo
echo "browse it: open $OUT/report/merged/html/index.html"
