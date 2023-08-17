#!/bin/bash

set -euo pipefail

# If updating the jdk, you should only need to change these two variables
jdk=java11
catalogJdk=latest_oracle_11_windows

choco install jq

dest="/c/users/buildkite/.java/${jdk}"

echo "Using catalog ID $catalogJdk..."

json=$(curl --silent "https://jvm-catalog.elastic.co/jdk/$catalogJdk")

echo "Downloading $(echo "$json" | jq -r '.id')..."
echo "  to $dest"

mkdir -p "$dest"
cd "$dest"
curl --silent -o "${jdk}.zip" "$(echo "$json" | jq -r '.url')"

# unzip doesn't have a --strip-components, but this is the same effect
mkdir -p temp
unzip -d temp "${jdk}.zip"
mv temp/*/* .
rm -rf temp
rm -f "${jdk}.tar.gz"
./bin/java -version
cd -
