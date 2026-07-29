#!/bin/bash
#
# Commit the coverage numbers into elastic/elasticsearch-code-coverage.
#
# The reports live inside one build's artifacts, so reading them costs a trip into Buildkite. The
# numbers themselves are small, so they go into a repo instead, where they need no credentials to
# read and a coverage change shows up as a diff someone can review.
#
#   publish-to-repo.sh <report-dir>
#
# Where things land. A main build writes <area>/latest/; a PR build writes <area>/pr/<n>/, so a
# PR's numbers are at a stable path someone can link to and a PR can never overwrite what main
# measured.
#
# What is NOT committed here: the HTML report. Coverage numbers and the browsable report have
# different shapes - a few KB of text that wants a diff, against ~9,000 files that want replacing
# wholesale - so the report goes to its own branch, not this one.
#
# Auth: needs a token with push access to ANOTHER repo. The ambient GH_TOKEN in these jobs is a
# GitHub App token scoped to elasticsearch that cannot write at all (status.sh documents the same
# 403), so the Vault token is preferred and the ambient one is only a fallback for laptop runs.
# Unlike status.sh this cannot retry-on-failure: an attempt costs a clone and a commit.
#
# Never fails the caller. Coverage is measured and uploaded before this runs, so a push problem is
# a reporting problem, not a reason to lose the build.
#
# NOTE: `pipefail` below is load-bearing - it is what makes `if git push ... | redact` test the
# push's status rather than sed's. Do not drop it without rewriting those checks.
set -uo pipefail

REPORT="${1:?usage: publish-to-repo.sh <report-dir>}"
REPO="${COVERAGE_REPO:-elastic/elasticsearch-code-coverage}"
BRANCH_OUT="${COVERAGE_BRANCH:-data}"
AREA="${COVERAGE_AREA:-esql}"

say() { echo "    $*"; }

# Belt and braces with the gate in publish.sh. fetch-report.sh rebuilds a report on a laptop and
# runs publish.sh with both of these set; without this check, anyone with GH_TOKEN exported - or a
# logged-in vault - would push their local rebuild into the shared branch just by browsing an old
# build. COVERAGE_FORCE_PUBLISH=1 is the deliberate escape hatch for a hand-run backfill.
if [[ "${COVERAGE_FORCE_PUBLISH:-}" != "1" ]]; then
  if [[ "${COVERAGE_SKIP_PUBLISH:-}" == "1" || -z "${BUILDKITE:-}" ]]; then
    echo "--- not committing coverage: not a CI publish (set COVERAGE_FORCE_PUBLISH=1 to override)"
    exit 0
  fi
fi

if [[ ! -d "$REPORT" ]]; then
  echo "--- not committing coverage: no report at $REPORT"; exit 0
fi
# Absolute, because everything below runs after a cd into a temp clone.
REPORT="$(cd "$REPORT" && pwd)" || exit 0
SUMMARY="${2:-$(dirname "$REPORT")/summary.txt}"

# --- auth ----------------------------------------------------------------------------------------

TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
if command -v vault >/dev/null 2>&1 \
   && vault_token=$(vault read -field=gh_admin_token secret/ci/elastic-elasticsearch/agentic-workflows 2>/dev/null) \
   && [[ -n "$vault_token" ]]; then
  TOKEN="$vault_token"
fi
if [[ -z "$TOKEN" ]]; then
  echo "--- not committing coverage: no token with push access to $REPO"; exit 0
fi

# Preflight, so a missing repo or an unprivileged token says so plainly instead of turning into
# "the branch does not exist" three steps later.
if command -v gh >/dev/null 2>&1; then
  perm=$(GH_TOKEN="$TOKEN" gh api "repos/$REPO" --jq '.permissions.push' 2>/dev/null)
  if [[ "$perm" != "true" ]]; then
    echo "--- not committing coverage: this token cannot push to $REPO (permissions.push=${perm:-unknown})"
    exit 0
  fi
fi

SHA="${BUILDKITE_COMMIT:-$(git rev-parse HEAD 2>/dev/null || echo unknown)}"
SHORT="${SHA:0:12}"
BRANCH="${BUILDKITE_BRANCH:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)}"
PR="${BUILDKITE_PULL_REQUEST:-}"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if [[ -n "$PR" && "$PR" != "false" ]]; then
  DEST="$AREA/pr/$PR"
else
  DEST="$AREA/latest"
fi

WORK=$(mktemp -d) || { echo "--- not committing coverage: mktemp failed"; exit 0; }
# Buildkite cancels a step with a signal, and bash runs no EXIT trap for an untrapped one - which
# would leave the tokenised remote in $WORK/repo/.git/config on the agent.
trap 'rm -rf "$WORK"' EXIT INT TERM HUP

ORIGIN="https://x-access-token:$TOKEN@github.com/$REPO.git"
redact() { sed 's/x-access-token:[^@]*@/x-access-token:***@/g'; }

# --- get the branch ------------------------------------------------------------------------------
#
# Data goes on its own branch, never on main: the org applies a ruleset to every repo's DEFAULT
# branch requiring changes to arrive by pull request, and a CI token cannot bypass it. Non-default
# branches carry no such rule, so a machine-written branch pushes cleanly and needs nothing from
# org admins.
#
# Branch existence is decided by asking, not inferred from a clone failing - otherwise a bad token
# or a network blip masquerades as a first run and the real cause never reaches the log.
echo "--- committing coverage to $REPO on branch $BRANCH_OUT under $DEST"
if git ls-remote --exit-code --heads "$ORIGIN" "$BRANCH_OUT" >/dev/null 2>&1; then
  if ! clone_err=$(git clone --quiet --depth 1 --branch "$BRANCH_OUT" "$ORIGIN" "$WORK/repo" 2>&1); then
    say "could not clone $BRANCH_OUT (non-fatal):"; printf '%s\n' "$clone_err" | head -3 | redact
    exit 0
  fi
  cd "$WORK/repo" || exit 0
else
  say "$BRANCH_OUT does not exist yet, creating it"
  mkdir -p "$WORK/repo" || exit 0
  cd "$WORK/repo" || exit 0
  git init --quiet -b "$BRANCH_OUT" . || exit 0
  git remote add origin "$ORIGIN" || exit 0
fi

# --- write the numbers ---------------------------------------------------------------------------
#
# Cleared first: this run must not republish a layer it did not measure. Without this, a leg that
# did not run leaves the previous commit's CSV in place while metadata.json is rewritten with this
# commit - the repo would assert that stale numbers belong to a commit that never produced them.
rm -rf "$DEST" && mkdir -p "$DEST" || exit 0

# JaCoCo's CSV carries a row per CLASS - ~700KB per layer. Aggregated to per-PACKAGE it is a couple
# of hundred rows and a few KB, which is both the granularity anyone reasons about and the
# difference between a repo that stays small and one that is gigabytes inside a year. The per-class
# detail stays in the build artifacts, reachable with fetch-report.sh.
layers=0
for csv in "$REPORT"/*/coverage.csv; do
  [[ -f "$csv" ]] || continue
  layer=$(basename "$(dirname "$csv")")
  # Into a temp name first: `>` truncates before python runs, so a failure would otherwise commit
  # and publish a zero-byte CSV and still report success.
  if ! python3 - "$csv" > "$DEST/$layer.csv.tmp" <<'PY'
import csv, sys
from collections import defaultdict
cols = ['INSTRUCTION_MISSED', 'INSTRUCTION_COVERED', 'BRANCH_MISSED', 'BRANCH_COVERED',
        'LINE_MISSED', 'LINE_COVERED', 'COMPLEXITY_MISSED', 'COMPLEXITY_COVERED',
        'METHOD_MISSED', 'METHOD_COVERED']
agg = defaultdict(lambda: defaultdict(int))
with open(sys.argv[1], newline='') as fh:
    for r in csv.DictReader(fh):
        bucket = agg[r['PACKAGE']]
        for c in cols:
            bucket[c] += int(r.get(c) or 0)
w = csv.writer(sys.stdout, lineterminator='\n')
w.writerow(['PACKAGE'] + cols + ['LINE_PCT', 'BRANCH_PCT'])
for pkg, v in sorted(agg.items()):
    lt = v['LINE_COVERED'] + v['LINE_MISSED']
    bt = v['BRANCH_COVERED'] + v['BRANCH_MISSED']
    w.writerow([pkg] + [v[c] for c in cols]
               + [f"{100 * v['LINE_COVERED'] / lt:.2f}" if lt else '',
                  f"{100 * v['BRANCH_COVERED'] / bt:.2f}" if bt else ''])
PY
  then
    say "aggregation failed for $layer - not publishing this run"; exit 0
  fi
  mv "$DEST/$layer.csv.tmp" "$DEST/$layer.csv" || exit 0
  layers=$((layers + 1))
done
if (( layers == 0 )); then
  say "no layer CSVs under $REPORT - nothing to publish"; exit 0
fi
[[ -f "$SUMMARY" ]] && cp "$SUMMARY" "$DEST/summary.txt"

if ! python3 - "$SHA" "$BRANCH" "$NOW" > "$DEST/metadata.json.tmp" <<'PY'
import json, os, sys
sha, branch, now = sys.argv[1:4]
env = os.environ.get
meta = {'commit': sha, 'branch': branch, 'measured_at': now,
        'scope': env('COVERAGE_PROJECTS', ''), 'includes': env('COVERAGE_INCLUDES', '')}
for k, v in (('pipeline', env('BUILDKITE_PIPELINE_SLUG', '')),
             ('build', env('BUILDKITE_BUILD_NUMBER', '')),
             ('build_url', env('BUILDKITE_BUILD_URL', '')),
             ('pull_request', env('BUILDKITE_PULL_REQUEST', ''))):
    if v and v != 'false':
        meta[k] = v
json.dump(meta, sys.stdout, indent=2)
print()
PY
then
  say "could not write metadata - not publishing this run"; exit 0
fi
mv "$DEST/metadata.json.tmp" "$DEST/metadata.json" || exit 0

HEADLINE=$(grep -m1 '^merged' "$DEST/summary.txt" 2>/dev/null || echo "coverage measured")

# --- commit and push -----------------------------------------------------------------------------

git -c user.name="elasticsearch-ci" -c user.email="noreply@elastic.co" add -A || exit 0
# metadata.json carries a fresh timestamp every run, so it must be excluded from the "did anything
# change" test - otherwise the guard can never fire and every re-run commits a new timestamp over
# identical numbers.
if git diff --cached --quiet -- . ":(exclude)$DEST/metadata.json"; then
  say "coverage unchanged since the last run"; exit 0
fi
git -c user.name="elasticsearch-ci" -c user.email="noreply@elastic.co" \
  commit --quiet -m "$AREA $SHORT on $BRANCH" -m "$HEADLINE" || exit 0

# Coverage steps on parallel PRs land on the same branch, so losing the race is the common path,
# not the exotic one. Rebase onto whatever arrived and try again rather than dropping the numbers.
for attempt in 1 2 3; do
  if git push --quiet origin "HEAD:$BRANCH_OUT" 2>&1 | redact; then
    say "pushed https://github.com/$REPO/tree/$BRANCH_OUT/$DEST"
    exit 0
  fi
  (( attempt == 3 )) && break
  say "push rejected, rebasing onto $BRANCH_OUT and retrying ($attempt/3)"
  git fetch --quiet origin "$BRANCH_OUT" 2>&1 | redact || break
  git rebase --quiet FETCH_HEAD 2>&1 | redact || { git rebase --abort >/dev/null 2>&1; break; }
done
say "could not push to $REPO (non-fatal)"
exit 0
