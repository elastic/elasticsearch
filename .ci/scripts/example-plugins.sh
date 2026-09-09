#!/bin/bash

set -euo pipefail

# Oracle OpenJDK 17.0.2 cannot read cgroup v2 on newer kernels
# (NPE: CgroupInfo.getMountPoint() / anyController is null).
# Adoptium 17 still can. JAVA_HOME alone is not enough: run-gradle.sh
# launches Gradle via `java` on PATH.
resolve_java_home() {
  local adoptium_home="$HOME/.java/adoptopenjdk17"
  if [[ "${ES_BUILD_JAVA:-}" == *"openjdk17"* ]]; then
    if [[ ! -d "$adoptium_home" ]]; then
      echo "Missing $adoptium_home (required when ES_BUILD_JAVA=$ES_BUILD_JAVA)" >&2
      exit 1
    fi
    echo "$adoptium_home"
    return
  fi
  echo "$JAVA_HOME"
}

cd "$WORKSPACE/plugins/examples"

JAVA_HOME="$(resolve_java_home)"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

echo "--- Using JAVA_HOME=$JAVA_HOME"
"$JAVA_HOME/bin/java" -version

"$WORKSPACE/.ci/scripts/run-gradle.sh" "$@"
