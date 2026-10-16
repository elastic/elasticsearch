#!/bin/bash

# Compares PromQL compliance with Prometheus between the PR and its merge base, using the promcheck Go binary
# (https://github.com/elastic/promcheck), and reports the queries whose result changed on the build and the PR.
# The elasticsearch-pull-request-validate-prometheus-compliance pipeline (catalog-info.yaml) runs it with the PR's
# GITHUB_PR_* variables, so that the build bot also posts the report as a PR comment.
#
# Regressions fail the step after publishing the report. Flaky cases are reported separately and do not
# count as regressions or improvements. A comparison that could not run is annotated as a warning.
#
# The testcase bundle is fixed: grafanalabs-top100.tar.gz from the promcheck release carries the seeded
# dataset plus the queries, piped whole into the binary. No Python, no Docker: Prometheus runs in-process.

set -euo pipefail

readonly CORPUS=grafanalabs-top100

for var in PROMCHECK_VER PROMCHECK_TEST_INSTANCE_TIMEOUT GITHUB_PR_TARGET_BRANCH; do
  [[ -n "${!var:-}" ]] || { echo "$var must be set" >&2; exit 1; }
done
for cmd in awk curl git jq tee; do
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

readonly RELEASE_URL="https://github.com/elastic/promcheck/releases/tag/v$PROMCHECK_VER"
readonly ANNOTATION_CONTEXT="ctx-validate-prometheus-compliance"
readonly PR_COMMENT_KEY="pr_comment:validate-prometheus-compliance:body"
# Changed queries listed per section; the full list is the changes artifact.
readonly MAX_ROWS=50

# The pipeline uploads OUTPUT_DIR as artifacts. Tests point TMP_ROOT at a scratch directory.
readonly TMP_ROOT="${PROMETHEUS_COMPLIANCE_TMPDIR:-/tmp}"
readonly WORK_DIR="$TMP_ROOT/validate-prometheus-compliance"
readonly OUTPUT_DIR="$TMP_ROOT/validate-prometheus-compliance-output"
readonly PROMCHECK_BIN="$WORK_DIR/promcheck"
readonly BUNDLE="$WORK_DIR/$CORPUS.tar.gz"
readonly CONTROL_LOG="$OUTPUT_DIR/@$CORPUS-control.log"
readonly TEST_LOG="$OUTPUT_DIR/@$CORPUS-test.log"
readonly CHANGES_FILE="$OUTPUT_DIR/@$CORPUS-changes.tsv"
readonly REPORT_FILE="$OUTPUT_DIR/@$CORPUS-report.md"
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
  NO_COLOR=1 "$PROMCHECK_BIN" \
    --elasticsearch "$workspace" \
    --elasticsearch-timeout "${PROMCHECK_TEST_INSTANCE_TIMEOUT}s" \
    --output json < "$BUNDLE" | tee "$log"
  rc=${PIPESTATUS[0]}
  set -e
  (( rc == 0 || rc == 1 ))
}

# Trust promcheck's summary; combine the error buckets for the console output.
summary() {
  jq -ec 'select(.type == "summary") | . + {errors: (.es_4xx + .es_5xx + .error)}' "$1"
}

# Every executed query as "<case>\t<status>\t<expression>". Flatten whitespace
# for the Markdown table while preserving literal backslashes in PromQL.
outcomes() {
  jq -r '
    select(.type == "query") |
    (if .status == "skipped" then
       if .skip_reason == "flaky" then "flaky" else "filtered" end
     elif .status == "error" and .es_status >= 500 then "error5xx"
     elif .status == "error" and .es_status >= 400 then "error4xx"
     else .status end) as $status |
    [.name, $status, (.query | gsub("[\t\r\n]"; " "))] | join("\t")
  ' "$1"
}

# Compare by case ID. Flaky on either side takes precedence; filtered cases
# do not participate. The last column records the comparison category.
changes() {
  awk -F'\t' '
    function severity(s) {
      return s == "match" ? 0 : s == "mismatch" ? 1 : s == "error4xx" ? 2 : 3
    }
    NR == FNR { before[$1] = $2; next }
    $1 in before {
      old = before[$1]
      if (old == "flaky" || $2 == "flaky") category = "flaky"
      else if (old == "filtered" || $2 == "filtered") next
      else if (severity(old) < severity($2)) category = "regression"
      else if (severity(old) > severity($2)) category = "improvement"
      else next
      printf "%s\t%s\t%s\t%s\t%s\n", $1, old, $2, $3, category
    }
  ' <(outcomes "$1") <(outcomes "$2") | sort -t "$(printf '\t')" -k1,1n
}

# The report body, in GitHub markdown: the result first, the changed queries collapsed, the comparison last.
# Stable cases are omitted. Flaky cases are shown separately, even when flaky in both runs.
render_changes() {
  local base="$1"
  local revision="$2"
  awk -F'\t' -v base="$base" -v revision="$revision" -v max="$MAX_ROWS" -v full="$CHANGES_LINK" '
    function label(s) { return s == "match" ? "PASS" : s == "mismatch" ? "FAIL" : s == "error4xx" ? "ERROR 4xx" : s == "error5xx" ? "ERROR 5xx" : s == "flaky" ? "FLAKY" : "ERROR" }
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
    $5 == "regression" { regressions[++nr] = $0 }
    $5 == "improvement" { improvements[++ni] = $0 }
    $5 == "flaky" { flaky[++nk] = $0 }
    END {
      printf "**%s**\n\n", nr ? "Regression detected" : "No regressions"
      if (nr + ni + nk == 0) {
        printf "No query compatibility changes detected.\n\n"
      } else {
        printf "%s, %s, %s\n\n", count(nr, "regression", "regressions"), count(ni, "improvement", "improvements"),
          count(nk, "flaky case", "flaky cases")
        printf "<details>\n<summary><strong>Show changed queries</strong></summary>\n\n"
        section("Regressions", regressions, nr)
        section("Improvements", improvements, ni)
        section("Flaky", flaky, nk)
        printf "</details>\n\n"
      }
      printf "**Compared** `%s` -> `%s`  \n", substr(base, 1, 10), substr(revision, 1, 10)
    }
  ' "$CHANGES_FILE"
}

rm -rf "$WORK_DIR" "$OUTPUT_DIR"
mkdir -p "$WORK_DIR" "$OUTPUT_DIR"

# Show up on the build as soon as the comparison starts. The final
# report reuses the same annotation context and comment key, so it
# replaces this note instead of adding a second one.
annotate info "$(printf '**Promcheck running**\n\nComparing PromQL coverage between the merge base and this PR with promcheck v%s. The verdict replaces this note when both runs finish.\n\n' "$PROMCHECK_VER" | report)"

echo "--- Downloading promcheck v$PROMCHECK_VER"
release=$(
  curl -fsSL --retry 3 --retry-delay 2 \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $GITHUB_API_TOKEN" \
    "https://api.github.com/repos/elastic/promcheck/releases/tags/v$PROMCHECK_VER"
) || not_compared "Could not fetch the promcheck v$PROMCHECK_VER release"
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) not_compared "Unsupported architecture $(uname -m)" ;;
esac
download_asset "$release" "promcheck-linux-$arch" "$PROMCHECK_BIN"
download_asset "$release" "$CORPUS.tar.gz" "$BUNDLE"
chmod +x "$PROMCHECK_BIN"

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

echo "--- Running promcheck on the merge base $base"
run_promcheck "$control_dir" "$CONTROL_LOG" || not_compared "The control run failed"
control_summary=$(summary "$CONTROL_LOG") || not_compared "The control run printed no promcheck summary"

echo "--- Running promcheck on the PR $revision"
run_promcheck "$PWD" "$TEST_LOG" || not_compared "The test run failed"
test_summary=$(summary "$TEST_LOG") || not_compared "The test run printed no promcheck summary"

echo "--- Reporting"
changes "$CONTROL_LOG" "$TEST_LOG" > "$CHANGES_FILE"
render_changes "$base" "$revision" | report > "$REPORT_FILE"

regressions=$(awk -F'\t' '$5 == "regression" { n++ } END { print n + 0 }' "$CHANGES_FILE")
if (( regressions > 0 )); then
  annotate error "$(cat "$REPORT_FILE")"
else
  annotate success "$(cat "$REPORT_FILE")"
fi

echo "Merge base: $control_summary"
echo "PR: $test_summary"
echo "Regressions: $regressions"
echo "Report: $REPORT_FILE"

if (( regressions > 0 )); then
  exit 1
fi
