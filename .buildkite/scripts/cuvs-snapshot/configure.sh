#!/bin/bash

set -euo pipefail

if [[ -f /etc/profile.d/elastic-nvidia.sh ]]; then
  export JAVA_HOME="$HOME/.java/openjdk24"
  export PATH="$JAVA_HOME/bin:$PATH"

  # Setup LD_LIBRARY_PATH, PATH

  export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
  source /etc/profile.d/elastic-nvidia.sh
fi

# Not running this before the tests results in an error when running the tests
# No idea why...
nvidia-smi

CURRENT_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ELASTICSEARCH_REPO_DIR="$(cd "$CURRENT_SCRIPT_DIR/../../.." && pwd)"

CUVS_SNAPSHOT_VERSION="${CUVS_SNAPSHOT_VERSION:-$(cat "$CURRENT_SCRIPT_DIR"/current-snapshot-version)}"
CUVS_ARCHIVE="cuvs-$CUVS_SNAPSHOT_VERSION.tar.gz"
CUVS_URL="https://storage.googleapis.com/elasticsearch-cuvs-snapshots/$CUVS_ARCHIVE"

CUVS_WORKSPACE=${CUVS_WORKSPACE:-$(cd "$(mktemp -d)")}
CUVS_DIR="$(pwd)/cuvs-$CUVS_SNAPSHOT_VERSION"

curl -O "$CUVS_URL"
tar -xzf "$CUVS_ARCHIVE"

CUVS_VERSION=$(cd "$CUVS_DIR/cuvs-java/target" && mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

LD_LIBRARY_PATH=$(echo "$LD_LIBRARY_PATH" | tr ':' '\n' | grep -v "libcuvs/linux-x64" | tr '\n' ':' | sed 's/:$//')
LD_LIBRARY_PATH="$CUVS_DIR/libcuvs/linux-x64:$LD_LIBRARY_PATH"
export LD_LIBRARY_PATH

cd "$CUVS_DIR/cuvs-java/target"
mvn install:install-file -Dfile="cuvs-java-$CUVS_VERSION.jar" -DartifactId=elastic-cuvs-java -DgeneratePom=true

cd "$ELASTICSEARCH_REPO_DIR"
PLUGIN_GRADLE_FILE=x-pack/plugin/gpu/build.gradle
sed -i "s|implementation 'com.nvidia.cuvs:elastic-cuvs-java:.*'|implementation 'com.nvidia.cuvs:elastic-cuvs-java:$CUVS_VERSION'|" "$PLUGIN_GRADLE_FILE"
