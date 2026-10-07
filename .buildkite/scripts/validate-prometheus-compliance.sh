#!/bin/bash

# Compares PromQL compliance with Prometheus between the PR and its merge base, using promcheck
# (https://github.com/elastic/promcheck), and reports the queries whose result changed on the build and the PR.
# The elasticsearch-pull-request-validate-prometheus-compliance pipeline (catalog-info.yaml) runs it with the PR's
# GITHUB_PR_* variables, so that the build bot also posts the report as a PR comment.
#
# The step never fails on the result. Buildkite's GitHub commit status mirrors the step state and ignores
# soft_fail (see flakiness-detection/runners/never-fail.sh), so a regression, or a comparison that could not
# run, is annotated and the step exits 0. Only a misconfigured pipeline fails it.
#
# Usage:
#   validate-prometheus-compliance.sh <dataset>

set -euo pipefail

readonly DATASET="${1:?Usage: validate-prometheus-compliance.sh <dataset>}"

for var in PROMCHECK_VER PROMCHECK_TEST_INSTANCE_TIMEOUT GITHUB_PR_TARGET_BRANCH; do
  [[ -n "${!var:-}" ]] || { echo "$var must be set" >&2; exit 1; }
done
for cmd in awk curl find git jq tar tee uv; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "$cmd must be on the PATH" >&2; exit 1; }
done
if [[ ! "$PROMCHECK_TEST_INSTANCE_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
  echo "PROMCHECK_TEST_INSTANCE_TIMEOUT must be a positive integer: $PROMCHECK_TEST_INSTANCE_TIMEOUT" >&2
  exit 1
fi

readonly GITHUB_API_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
if [[ -z "$GITHUB_API_TOKEN" ]]; then
  echo "GH_TOKEN or GITHUB_TOKEN must be set" >&2
  exit 1
fi

readonly PYTHON_VERSION=3.14
readonly RELEASE_URL="https://github.com/elastic/promcheck/releases/tag/v$PROMCHECK_VER"
readonly ANNOTATION_CONTEXT="ctx-validate-prometheus-compliance"
readonly PR_COMMENT_KEY="pr_comment:validate-prometheus-compliance:body"
# Changed queries listed per section; the full list is the changes artifact.
readonly MAX_ROWS=50

# The pipeline uploads OUTPUT_DIR as artifacts. Tests point TMP_ROOT at a scratch directory.
readonly TMP_ROOT="${PROMETHEUS_COMPLIANCE_TMPDIR:-/tmp}"
readonly WORK_DIR="$TMP_ROOT/validate-prometheus-compliance"
readonly OUTPUT_DIR="$TMP_ROOT/validate-prometheus-compliance-output"
readonly CACHE_DIR="$WORK_DIR/.cache"
readonly DATA_DIR="$WORK_DIR/data"
readonly INPUT_DIR="$WORK_DIR/input"
readonly DATA_ARCHIVE="$WORK_DIR/promcheck-data.tar.gz"
readonly PACKAGE="$WORK_DIR/promcheck.tar.gz"
readonly FIXTURE="$INPUT_DIR/$DATASET.jsonl"
readonly CONTROL_LOG="$OUTPUT_DIR/@$DATASET-control.log"
readonly TEST_LOG="$OUTPUT_DIR/@$DATASET-test.log"
readonly CHANGES_FILE="$OUTPUT_DIR/@$DATASET-changes.tsv"
readonly REPORT_FILE="$OUTPUT_DIR/@$DATASET-report.md"
# Artifacts keep their absolute path without the leading slash.
readonly CHANGES_LINK="<a href=\"artifact://${CHANGES_FILE#/}\">${CHANGES_FILE##*/}</a>"

# The exact job this comparison ran in.
build_url="${BUILDKITE_BUILD_URL:-}"
if [[ -n "$build_url" && -n "${BUILDKITE_JOB_ID:-}" ]]; then
  build_url+="#$BUILDKITE_JOB_ID"
fi
readonly BUILD_URL="$build_url"

control_dir=""

# Shows the report on the build and sets it as pr_comment meta-data, which the Buildkite build bot posts in its PR
# comment for pipelines that enable it (ELASTIC_PR_COMMENTS_ENABLED).
annotate() {
  local style="$1"
  local annotation="$2"
  if command -v buildkite-agent >/dev/null 2>&1; then
    printf '%s\n' "$annotation" | buildkite-agent annotate --context "$ANNOTATION_CONTEXT" --style "$style" \
      || echo "Failed to annotate the build"
    printf '%s\n' "$annotation" | buildkite-agent meta-data set "$PR_COMMENT_KEY" \
      || echo "Failed to set the PR comment"
  fi
}

# Wraps a report body read from stdin: first the marker that lets the PR comment be found and replaced, last the
# links to the run and the promcheck release.
report() {
  printf '<!-- promcheck-pr-report -->\n## Promcheck\n\n'
  cat
  if [[ -n "$BUILD_URL" ]]; then
    printf '[Buildkite run](%s) | ' "$BUILD_URL"
  fi
  printf '[promcheck v%s](%s)\n' "$PROMCHECK_VER" "$RELEASE_URL"
}

# The comparison could not run. It is reported like a result rather than failing the PR.
not_compared() {
  local reason="$1"
  echo "Not compared: $reason"
  annotate warning "$(printf '**Not compared**\n\n%s.\n\n' "$reason" | report)"
  exit 0
}

cleanup() {
  if [[ -n "$control_dir" ]]; then
    git worktree remove --force "$control_dir" >/dev/null 2>&1 || true
    rm -rf "$control_dir"
  fi
}

# Downloads a promcheck release asset by name.
download_asset() {
  local release="$1"
  local name="$2"
  local target="$3"
  local url
  url=$(jq -er --arg name "$name" '.assets[] | select(.name == $name) | .url' <<< "$release") \
    || not_compared "The promcheck v$PROMCHECK_VER release has no $name"
  curl -fsSL --retry 3 --retry-delay 2 \
    -H "Authorization: Bearer $GITHUB_API_TOKEN" \
    -H "Accept: application/octet-stream" \
    -o "$target" "$url" \
    || not_compared "Could not download $name"
}

# Runs promcheck against Elasticsearch built from the given checkout. promcheck exits 1 when queries fail, which
# is a result; any other exit code means the run itself failed.
run_promcheck() {
  local workspace="$1"
  local log="$2"
  local rc
  set +e
  NO_COLOR=1 uv run --cache-dir "$CACHE_DIR" --python "$PYTHON_VERSION" --with "$PACKAGE" promcheck \
    --granularity 15s \
    --test-instance-timeout "$PROMCHECK_TEST_INSTANCE_TIMEOUT" \
    --workspace "$workspace" \
    "$FIXTURE" 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  set -e
  (( rc == 0 || rc == 1 ))
}

# The promcheck summary of a run, or a failure when the run did not get as far as printing it.
summary() {
  awk -F'|' '
    function trim(s) { gsub(/[[:space:]]/, "", s); return s }
    /^\|[[:space:]]*executed\.success[[:space:]]*\|/ { ok = trim($3) }
    /^\|[[:space:]]*executed\.failure[[:space:]]*\|/ { fail = trim($3) }
    /^\|[[:space:]]*executed\.error[[:space:]]*\|/   { err = trim($3) }
    /^\|[[:space:]]*executed\.total[[:space:]]*\|/   { total = trim($3) }
    END {
      if (ok !~ /^[0-9]+$/ || fail !~ /^[0-9]+$/ || err !~ /^[0-9]+$/ || total !~ /^[0-9]+$/)
        exit 1
      skip = total - ok - fail - err
      if (skip < 0)
        exit 2
      printf "ok=%d fail=%d err=%d skip=%d total=%d\n", ok, fail, err, skip, total
    }
  ' "$1"
}

# Every query of a promcheck log as "<case>\t<status>\t<expression>", colour codes stripped.
outcomes() {
  awk '
    { gsub(/\033\[[0-9;]*m/, "") }
    /^\[[0-9]+\] [A-Z]+/ { id = substr($1, 2, length($1) - 2); status[id] = $2; order[++n] = id; next }
    n && /^[[:space:]]+expr:/ { e = $0; sub(/^[[:space:]]+expr:[[:space:]]*/, "", e); expr[order[n]] = e }
    END { for (i = 1; i <= n; i++) printf "%s\t%s\t%s\n", order[i], status[order[i]], expr[order[i]] }
  ' "$1"
}

# Queries that pass in one run and not the other, by case id: "<case>\t<before>\t<after>\t<expression>".
changes() {
  awk -F'\t' '
    NR == FNR { before[$1] = $2; next }
    ($1 in before) && ((before[$1] == "OK") != ($2 == "OK")) { printf "%s\t%s\t%s\t%s\n", $1, before[$1], $2, $3 }
  ' <(outcomes "$1") <(outcomes "$2") | sort -t "$(printf '\t')" -k1,1n
}

# The report body, in GitHub markdown: the result first, the changed queries collapsed, the comparison last.
# Regressions and fixes are told apart by the transition, so a new promcheck state only needs a label. Same input,
# same output.
render_changes() {
  local base="$1"
  local revision="$2"
  awk -F'\t' -v base="$base" -v revision="$revision" -v max="$MAX_ROWS" -v full="$CHANGES_LINK" '
    function passing(s) { return s == "OK" }
    function label(s) { return s == "OK" ? "PASS" : s == "ERR" ? "ERROR" : s }
    # n + 0: a section with no rows never set its counter, which awk would print as "".
    function count(n, one, many) { return (n + 0) " " (n == 1 ? one : many) }
    # Inline code: pipes escaped for the table, a backtick in the query widens the fence.
    function code(s) {
      gsub(/\|/, "\\|", s)
      return index(s, "`") ? "`` " s " ``" : "`" s "`"
    }
    function section(title, rows, n,   i, f) {
      if (!n) return
      printf "### %s\n\n| Case | Result | Query |\n|---:|:---:|---|\n", title
      for (i = 1; i <= n && i <= max; i++) {
        split(rows[i], f, "\t")
        printf "| %s | `%s -> %s` | %s |\n", f[1], label(f[2]), label(f[3]), code(f[4])
      }
      if (n > max) printf "\n_%d more in %s._\n", n - max, full
      printf "\n"
    }
    passing($2) { regressions[++nr] = $0; next }
    { fixes[++nf] = $0 }
    END {
      printf "**%s**\n\n", nr ? "Regression detected" : "No regressions"
      if (nr + nf == 0) {
        printf "No query compatibility changes detected.\n\n"
      } else {
        printf "%s, %s, %s\n\n", count(nr, "regression", "regressions"), count(nf, "fix", "fixes"),
          count(nr + nf, "changed case", "changed cases")
        printf "<details>\n<summary><strong>Show changed queries</strong></summary>\n\n"
        section("Regressions", regressions, nr)
        section("Fixed", fixes, nf)
        printf "</details>\n\n"
      }
      printf "**Compared** `%s` -> `%s`  \n", substr(base, 1, 10), substr(revision, 1, 10)
    }
  ' "$CHANGES_FILE"
}

rm -rf "$WORK_DIR" "$OUTPUT_DIR"
mkdir -p "$DATA_DIR" "$INPUT_DIR" "$OUTPUT_DIR"

echo "--- Downloading promcheck v$PROMCHECK_VER"
release=$(
  curl -fsSL --retry 3 --retry-delay 2 \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $GITHUB_API_TOKEN" \
    "https://api.github.com/repos/elastic/promcheck/releases/tags/v$PROMCHECK_VER"
) || not_compared "Could not fetch the promcheck v$PROMCHECK_VER release"
download_asset "$release" "promcheck-data-$PROMCHECK_VER.tar.gz" "$DATA_ARCHIVE"
download_asset "$release" "promcheck-$PROMCHECK_VER.tar.gz" "$PACKAGE"

tar -xzf "$DATA_ARCHIVE" -C "$DATA_DIR" || not_compared "Could not extract promcheck-data-$PROMCHECK_VER.tar.gz"
corpus_files=0
while IFS= read -r -d '' file; do
  [[ ! -e "$INPUT_DIR/${file##*/}" ]] || not_compared "promcheck-data-$PROMCHECK_VER.tar.gz has two ${file##*/} query corpora"
  cp "$file" "$INPUT_DIR/"
  corpus_files=$((corpus_files + 1))
done < <(find "$DATA_DIR" -type f -path '*/data/results/*' -name '*.jsonl' -print0)
(( corpus_files > 0 )) || not_compared "promcheck-data-$PROMCHECK_VER.tar.gz has no query corpus under data/results"
[[ -f "$FIXTURE" ]] || not_compared "promcheck-data-$PROMCHECK_VER.tar.gz has no $DATASET query corpus"

echo "--- Checking out the merge base with $GITHUB_PR_TARGET_BRANCH"
git fetch --no-tags origin "$GITHUB_PR_TARGET_BRANCH" \
  || not_compared "Could not fetch $GITHUB_PR_TARGET_BRANCH"
# The control is where this PR branched off, not the tip of the target branch: a PromQL change that landed there
# since would otherwise count as this PR's regression or fix.
base=$(git merge-base "origin/$GITHUB_PR_TARGET_BRANCH" HEAD) \
  || not_compared "This PR has no merge base with $GITHUB_PR_TARGET_BRANCH"
revision=$(git rev-parse HEAD)
control_dir=$(mktemp -d "$TMP_ROOT/validate-prometheus-compliance-control-XXXXXX")
trap cleanup EXIT
git worktree add --detach "$control_dir" "$base" || not_compared "Could not check out the merge base $base"

uv python install "$PYTHON_VERSION" || not_compared "Could not install Python $PYTHON_VERSION"

echo "--- Running promcheck on the merge base $base"
run_promcheck "$control_dir" "$CONTROL_LOG" || not_compared "The control run failed"
control_summary=$(summary "$CONTROL_LOG") || not_compared "The control run printed no promcheck summary"

echo "--- Running promcheck on the PR $revision"
run_promcheck "$PWD" "$TEST_LOG" || not_compared "The test run failed"
test_summary=$(summary "$TEST_LOG") || not_compared "The test run printed no promcheck summary"

echo "--- Reporting"
changes "$CONTROL_LOG" "$TEST_LOG" > "$CHANGES_FILE"
render_changes "$base" "$revision" | report > "$REPORT_FILE"

# A query that passed on the merge base and changed is a regression.
regressions=$(awk -F'\t' '$2 == "OK" { n++ } END { print n + 0 }' "$CHANGES_FILE")
fixes=$(awk -F'\t' '$2 != "OK" { n++ } END { print n + 0 }' "$CHANGES_FILE")
if (( regressions > 0 )); then
  annotate error "$(cat "$REPORT_FILE")"
else
  annotate success "$(cat "$REPORT_FILE")"
fi

echo "Merge base: $control_summary"
echo "PR: $test_summary"
echo "Regressions: $regressions"
echo "Fixes: $fixes"
echo "Report: $REPORT_FILE"
