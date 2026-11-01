#!/bin/bash

set -euo pipefail

GENERATED_DIRS=(
  "x-pack/plugin/esql/src/main/generated"
  "x-pack/plugin/esql/src/main/generated-src"
  "x-pack/plugin/esql/compute/src/main/generated"
  "x-pack/plugin/esql/compute/src/main/generated-src"
  "x-pack/plugin/esql/function/math/src/main/generated"
)

echo "--- Regenerating ES|QL sources"
# --rerun is required, otherwise these come back from the build cache without running the generators
.ci/scripts/run-gradle.sh -Dignore.tests.seed \
  :x-pack:plugin:esql:stringTemplates --rerun \
  :x-pack:plugin:esql:compute:stringTemplates --rerun \
  :x-pack:plugin:esql:compileJava --rerun \
  :x-pack:plugin:esql:compute:compileJava --rerun \
  :x-pack:plugin:esql:function:math:compileJava --rerun

echo "--- Checking generated sources"
CHANGES=$(git status --porcelain -- "${GENERATED_DIRS[@]}")
if [[ -n "$CHANGES" ]]; then
  echo "$CHANGES"
  git diff -- "${GENERATED_DIRS[@]}"
  echo ""
  echo "Generated ES|QL sources don't match their generators. Regenerate and commit the result:"
  echo "  ./gradlew :x-pack:plugin:esql:compileJava :x-pack:plugin:esql:compute:compileJava --rerun"
  exit 1
fi

echo "Generated ES|QL sources are up to date"
