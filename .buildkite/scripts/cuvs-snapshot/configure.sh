#!/bin/bash

set -euo pipefail

if [[ -f /etc/profile.d/elastic-nvidia.sh ]]; then
  export JAVA_HOME="$HOME/.java/openjdk24"
  export PATH="$JAVA_HOME/bin:$PATH"

  # Setup LD_LIBRARY_PATH, PATH
  source /etc/profile.d/elastic-nvidia.sh
fi

CUVS_WORKSPACE=/opt/elastic-cuvs
sudo rm -rf "$CUVS_WORKSPACE/cuvs"
sudo mkdir "$CUVS_WORKSPACE"
sudo chmod 777 "$CUVS_WORKSPACE"

CURRENT_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ELASTICSEARCH_REPO_DIR="$(cd "$CURRENT_SCRIPT_DIR/../../.." && pwd)"

CUVS_SNAPSHOT_VERSION="${CUVS_SNAPSHOT_VERSION:-$(cat "$CURRENT_SCRIPT_DIR"/current-snapshot-version)}"
CUVS_ARCHIVE="cuvs-$CUVS_SNAPSHOT_VERSION.tar.gz"
CUVS_URL="https://storage.googleapis.com/elasticsearch-cuvs-snapshots/$CUVS_ARCHIVE"

# CUVS_WORKSPACE=${CUVS_WORKSPACE:-$(cd "$(mktemp -d)" && pwd)}
cd "$CUVS_WORKSPACE"
CUVS_DIR="$CUVS_WORKSPACE"/cuvs
mkdir -p "$CUVS_DIR"

CUVS_SNAPSHOT_DIR="$CUVS_WORKSPACE/cuvs-$CUVS_SNAPSHOT_VERSION"

curl -O "$CUVS_URL"
tar -xzf "$CUVS_ARCHIVE"

mv "$CUVS_SNAPSHOT_DIR"/cuvs "$CUVS_WORKSPACE/"

CUVS_VERSION=$(cd "$CUVS_DIR/java/cuvs-java/target" && mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

LD_LIBRARY_PATH=$(echo "$LD_LIBRARY_PATH" | tr ':' '\n' | grep -v "cuvs/" | tr '\n' ':' | sed 's/:$//')
LD_LIBRARY_PATH="\
${CUVS_DIR}/cpp/build/install/lib:\
${CUVS_DIR}/cpp/build/_deps/rapids_logger-build:\
${CUVS_DIR}/cpp/build/_deps/rmm-build:\
$LD_LIBRARY_PATH"

export LD_LIBRARY_PATH

cd "$CUVS_DIR/java/cuvs-java/target"
mvn install:install-file -Dfile="cuvs-java-$CUVS_VERSION.jar"

cd "$ELASTICSEARCH_REPO_DIR"
PLUGIN_GRADLE_FILE=x-pack/plugin/gpu/build.gradle
sed -i "s|implementation 'com.nvidia.cuvs:cuvs-java:.*'|implementation 'com.nvidia.cuvs:cuvs-java:$CUVS_VERSION'|" "$PLUGIN_GRADLE_FILE"
