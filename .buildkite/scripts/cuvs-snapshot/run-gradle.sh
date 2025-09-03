#!/bin/bash

set -euo pipefail

source .buildkite/scripts/cuvs-snapshot/configure.sh

cd "$WORKSPACE"

.ci/scripts/run-gradle.sh "$@"
