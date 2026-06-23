#!/bin/bash
#
# Classifies failed tests from the most recent elasticsearch-periodic-java-ea
# run into "EA-specific candidates" (no matching failures on non-EA lanes) vs
# "inherited from main" (also failing on regular CI lanes).
#
# Outputs a markdown report. Posts a Buildkite annotation when invoked from a
# Buildkite job ($BUILDKITE_BUILD_ID set), otherwise prints to stdout.
#
# Local prototype: data fetched via the `estc ci-stats` CLI so it runs on a
# developer machine without ci-stats credentials. The in-CI variant should
# replace fetch_ea_failures and count_baseline_failures with curl calls
# against the ci-stats cluster (auth via vault, mirroring USE_PERF_CREDENTIALS
# in pre-command). Those two functions are the only data-fetching seams.
#
# Usage:
#   .buildkite/scripts/ea-classify-failures.sh
#
# Tunables (env, with defaults):
#   EA_CLASSIFY_RECENT_HOURS=26          # window for "this run's" EA failures
#   EA_CLASSIFY_BASELINE_DAYS=14         # baseline window for non-EA failures
#   EA_CLASSIFY_INHERITED_THRESHOLD=1    # >=N non-EA failures => "inherited"

set -euo pipefail

EA_TAG="elasticsearch-periodic-java-ea"

log() { echo "$@" >&2; }

# Returns the raw `estc ci-stats list-failures` output for failures tagged as
# EA in the recent window. Override in tests.
fetch_ea_failures() {
  local recent_hours="$1"
  estc ci-stats list-failures \
    --tag "$EA_TAG" \
    --from "now-${recent_hours}h" \
    --size 200
}

# Returns a single integer: count of non-EA failures of the given test in the
# baseline window. Override in tests.
count_baseline_failures() {
  local class="$1" name="$2" baseline_days="$3"
  local esc_name=${name//\"/\\\"}
  estc ci-stats list-failures \
    --class "$class" \
    --query-string "name.keyword:\"$esc_name\" AND NOT build.tags:\"$EA_TAG\"" \
    --from "now-${baseline_days}d" --size 1 2>&1 \
    | awk '/^Failures:[[:space:]]+[0-9]+/ { print $2; exit }'
}

# Reads `list-failures` output on stdin and emits unique tab-separated
# "class<TAB>name" lines, sorted.
parse_test_pairs() {
  awk '
    /^  Class:[[:space:]]/ { sub(/^  Class:[[:space:]]+/, ""); c = $0 }
    /^  Test:[[:space:]]/  { sub(/^  Test:[[:space:]]+/, "");  print c "\t" $0 }
  ' | sort -u
}

# Emits the markdown report. Args: recent_hours baseline_days threshold
# candidates_block inherited_block
render_report() {
  local recent_hours="$1" baseline_days="$2" inherited_threshold="$3"
  local candidates="$4" inherited="$5"
  local cand_count inh_count
  cand_count=$(printf '%s' "$candidates" | grep -c '^- ' || true)
  inh_count=$(printf '%s' "$inherited" | grep -c '^- ' || true)

  cat <<MARKDOWN
## EA failure classification

Window: last ${recent_hours}h of \`${EA_TAG}\` failures vs ${baseline_days}d baseline of non-EA failures on \`main\`.
Inherited threshold: ≥${inherited_threshold} non-EA failure(s).

### EA-specific candidates (${cand_count})

Failed on EA but **no** non-EA failures in the baseline window — investigate these.

${candidates:-_(none)_}

<details>
<summary>Inherited from main (${inh_count}) — click to expand</summary>

These also fail on regular CI lanes; likely existing flaky/broken tests, not EA-specific.

${inherited:-_(none)_}

</details>
MARKDOWN
}

emit_annotation() {
  local style="$1" body="$2"
  if [[ -n "${BUILDKITE_BUILD_ID:-}" ]]; then
    printf '%s\n' "$body" \
      | buildkite-agent annotate \
          --context "ea-failure-classification" \
          --style "$style"
  else
    printf '%s\n' "$body"
  fi
}

main() {
  local recent_hours="${EA_CLASSIFY_RECENT_HOURS:-26}"
  local baseline_days="${EA_CLASSIFY_BASELINE_DAYS:-14}"
  local inherited_threshold="${EA_CLASSIFY_INHERITED_THRESHOLD:-1}"

  # Graceful fallback when running in CI before ci-stats credentials are wired
  # up. Lets us validate the pipeline integration end-to-end without full
  # functionality.
  if ! command -v estc >/dev/null 2>&1; then
    log "estc CLI not available; emitting placeholder annotation."
    emit_annotation info "## EA failure classification

_Pipeline integration in place. Awaiting ci-stats read credentials in CI to enable classification — see \`.buildkite/scripts/ea-classify-failures.sh\` header._"
    return 0
  fi

  log "[1/3] Fetching EA failures from the last ${recent_hours}h..."
  local ea_out
  ea_out=$(fetch_ea_failures "$recent_hours")

  log "[2/3] Parsing failures..."
  local tests
  tests=$(printf '%s\n' "$ea_out" | parse_test_pairs)

  if [[ -z "$tests" ]]; then
    log "No EA failures found in the last ${recent_hours}h."
    return 0
  fi

  local total
  total=$(printf '%s\n' "$tests" | wc -l | tr -d ' ')
  log "[3/3] Classifying $total unique tests against ${baseline_days}d baseline..."

  local candidates="" inherited="" i=0
  while IFS=$'\t' read -r class name; do
    i=$((i+1))
    log "  [$i/$total] $class.$name"
    local count
    count=$(count_baseline_failures "$class" "$name" "$baseline_days")
    count=${count:-0}
    if [[ "$count" -ge "$inherited_threshold" ]]; then
      inherited+="- \`${class}.${name}\` — ${count} non-EA fails / ${baseline_days}d"$'\n'
    else
      candidates+="- \`${class}.${name}\` — 0 non-EA fails / ${baseline_days}d"$'\n'
    fi
  done <<< "$tests"

  local body style="info"
  body=$(render_report "$recent_hours" "$baseline_days" "$inherited_threshold" "$candidates" "$inherited")
  if [[ -n "$candidates" ]]; then
    style="warning"
  fi
  emit_annotation "$style" "$body"
}

# Run main only when executed directly, so the test file can source this and
# override fetch_ea_failures / count_baseline_failures with stubs.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
