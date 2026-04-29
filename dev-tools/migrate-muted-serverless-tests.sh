#!/usr/bin/env bash
#
# Migrate muted test issues from elastic/elasticsearch-serverless to
# elastic/elasticsearch.
#
# For every issue URL in muted-tests.yml that points to
# elastic/elasticsearch-serverless, this script:
#
#   1. Reads the source issue (title, body, labels) via `gh`.
#   2. Opens an equivalent issue in elastic/elasticsearch with the original
#      body plus a "Migrated from <source-url>" footer.
#   3. Replaces the URL in muted-tests.yml with the new issue URL.
#   4. Posts a comment on the source issue linking to the new one and closes
#      the source issue.
#
# Re-running the script is safe: only URLs still pointing at
# elasticsearch-serverless are processed.
#
# Requirements: gh (authenticated for both repos), jq, sed, awk.
#
# Usage:
#   dev-tools/migrate-muted-serverless-tests.sh [--dry-run]
#
# Environment overrides:
#   MUTED_FILE   path to muted-tests.yml (default: muted-tests.yml at repo root)
#   SOURCE_REPO  default: elastic/elasticsearch-serverless
#   TARGET_REPO  default: elastic/elasticsearch
#   EXTRA_LABELS comma-separated extra labels to apply on the new issue
#                (in addition to ">test-failure")

set -euo pipefail

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
fi

MUTED_FILE="${MUTED_FILE:-muted-tests.yml}"
SOURCE_REPO="${SOURCE_REPO:-elastic/elasticsearch-serverless}"
TARGET_REPO="${TARGET_REPO:-elastic/elasticsearch}"
EXTRA_LABELS="${EXTRA_LABELS:-}"
# Roots to scan for migrated test classes. Defaults cover the usual suspects;
# override if you only want to look in a subtree.
SEARCH_ROOTS="${SEARCH_ROOTS:-x-pack modules server libs qa}"

if [[ ! -f "$MUTED_FILE" ]]; then
  echo "error: $MUTED_FILE not found (run from repo root or set MUTED_FILE)" >&2
  exit 1
fi
read -r -a SEARCH_ROOT_DIRS <<<"$SEARCH_ROOTS"
for dir in "${SEARCH_ROOT_DIRS[@]}"; do
  if [[ ! -d "$dir" ]]; then
    echo "error: search root '$dir' not found (run from repo root or override SEARCH_ROOTS)" >&2
    exit 1
  fi
done

for tool in gh jq sed awk; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool '$tool' not on PATH" >&2
    exit 1
  fi
done

# Cache labels that exist in the target repo so we can drop unknown labels
# rather than failing the issue creation.
TARGET_LABELS_FILE="$(mktemp)"
trap 'rm -f "$TARGET_LABELS_FILE"' EXIT
gh label list --repo "$TARGET_REPO" --limit 2000 --json name --jq '.[].name' \
  >"$TARGET_LABELS_FILE"

label_exists_in_target() {
  grep -Fxq -- "$1" "$TARGET_LABELS_FILE"
}

# Walk the YAML and collect (class, issue-url) for every entry whose `issue:`
# field points at elasticsearch-serverless. Each entry is the most recent
# `class:` line seen above the `issue:` line.
PAIRS_FILE="$(mktemp)"
trap 'rm -f "$TARGET_LABELS_FILE" "$PAIRS_FILE"' EXIT
awk '
  /^[[:space:]]*-?[[:space:]]*class:[[:space:]]*/ {
    line = $0
    sub(/^[[:space:]]*-?[[:space:]]*class:[[:space:]]*/, "", line)
    gsub(/"/, "", line)
    sub(/[[:space:]]+#.*$/, "", line)
    sub(/[[:space:]]+$/, "", line)
    cls = line
    next
  }
  /^[[:space:]]*issue:[[:space:]]*"?https:\/\/github\.com\/elastic\/elasticsearch-serverless\/issues\/[0-9]+/ {
    line = $0
    sub(/^[[:space:]]*issue:[[:space:]]*/, "", line)
    gsub(/"/, "", line)
    sub(/[[:space:]]+#.*$/, "", line)
    sub(/[[:space:]]+$/, "", line)
    if (cls != "") print cls "\t" line
  }
' "$MUTED_FILE" >"$PAIRS_FILE"

PAIR_COUNT=$(wc -l <"$PAIRS_FILE" | tr -d ' ')
if [[ "$PAIR_COUNT" -eq 0 ]]; then
  echo "No elasticsearch-serverless issue URLs found in $MUTED_FILE."
  exit 0
fi

echo "Found $PAIR_COUNT muted entry/entries pointing at $SOURCE_REPO."
[[ "$DRY_RUN" == "1" ]] && echo "(dry-run mode: no changes will be made)"

# Pre-build a lookup of fully-qualified class names that live under the
# elasticsearch source tree, so we only migrate issues for tests that have
# actually been migrated to this repo.
CLASSES_FILE="$(mktemp)"
trap 'rm -f "$TARGET_LABELS_FILE" "$PAIRS_FILE" "$CLASSES_FILE"' EXIT
while IFS= read -r f; do
  # Convert .../<srcset>/java/org/elasticsearch/.../Foo.java -> org.elasticsearch...Foo
  rel="${f#*/java/}"
  rel="${rel%.java}"
  printf '%s\n' "${rel//\//.}"
done < <(find "${SEARCH_ROOT_DIRS[@]}" -type f -name "*.java" -not -path "*/build/*") \
  | sort -u >"$CLASSES_FILE"

class_is_migrated() {
  grep -Fxq -- "$1" "$CLASSES_FILE"
}

migrate_one() {
  local cls="$1"
  local src_url="$2"
  local src_num="${src_url##*/}"

  echo
  echo "==> $cls — $src_url"

  if ! class_is_migrated "$cls"; then
    echo "    skip: $cls not found under [${SEARCH_ROOTS}]"
    echo "          (test has not been migrated to elasticsearch yet)"
    return 0
  fi

  local issue_json
  if ! issue_json=$(gh issue view "$src_num" --repo "$SOURCE_REPO" \
        --json title,body,labels,state 2>/dev/null); then
    echo "    skip: cannot read source issue (deleted or no access)"
    return 0
  fi

  local state title body
  state=$(jq -r '.state' <<<"$issue_json")
  title=$(jq -r '.title' <<<"$issue_json")
  body=$(jq -r '.body // ""' <<<"$issue_json")

  if [[ "$state" != "OPEN" ]]; then
    echo "    skip: source issue is $state"
    return 0
  fi

  # Build label set: source issue's labels (filtered to ones that exist in
  # the target repo), plus >test-failure, plus any EXTRA_LABELS.
  local -a labels=()
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    if label_exists_in_target "$name"; then
      labels+=("$name")
    else
      echo "    note: dropping label '$name' (not present in $TARGET_REPO)"
    fi
  done < <(jq -r '.labels[].name' <<<"$issue_json")

  if label_exists_in_target ">test-failure"; then
    labels+=(">test-failure")
  fi
  if [[ -n "$EXTRA_LABELS" ]]; then
    IFS=',' read -r -a extras <<<"$EXTRA_LABELS"
    for l in "${extras[@]}"; do
      labels+=("$l")
    done
  fi

  # De-duplicate.
  local -a uniq_labels=()
  local seen=""
  for l in "${labels[@]}"; do
    case "$seen" in
      *"|$l|"*) ;;
      *) uniq_labels+=("$l"); seen="$seen|$l|" ;;
    esac
  done

  local new_body
  new_body=$(printf '%s\n\n---\n\nMigrated from %s\n' "$body" "$src_url")

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "    [dry-run] would create issue in $TARGET_REPO"
    echo "    [dry-run]   title : $title"
    echo "    [dry-run]   labels: ${uniq_labels[*]:-(none)}"
    return 0
  fi

  # Create new issue.
  local -a label_args=()
  for l in "${uniq_labels[@]}"; do
    label_args+=(--label "$l")
  done

  local new_url
  new_url=$(gh issue create \
    --repo "$TARGET_REPO" \
    --title "$title" \
    --body "$new_body" \
    "${label_args[@]}")
  echo "    created: $new_url"

  # Replace the URL in muted-tests.yml. Use awk for safe literal replacement
  # (avoids needing to escape regex/sed metacharacters).
  awk -v from="$src_url" -v to="$new_url" '
    {
      out = ""
      line = $0
      while ((idx = index(line, from)) > 0) {
        out = out substr(line, 1, idx - 1) to
        line = substr(line, idx + length(from))
      }
      print out line
    }
  ' "$MUTED_FILE" >"${MUTED_FILE}.tmp"
  mv "${MUTED_FILE}.tmp" "$MUTED_FILE"
  echo "    updated: $MUTED_FILE"

  # Comment + close the source issue.
  gh issue comment "$src_num" --repo "$SOURCE_REPO" \
    --body "Migrated to $new_url — closing this issue, please follow up there." \
    >/dev/null
  gh issue close "$src_num" --repo "$SOURCE_REPO" >/dev/null
  echo "    closed source issue"
}

while IFS=$'\t' read -r cls url; do
  [[ -z "$cls" || -z "$url" ]] && continue
  migrate_one "$cls" "$url"
done <"$PAIRS_FILE"

echo
echo "Done."
