#!/bin/bash

set -euo pipefail

# If updating the jdk, you should only need to change these two variables
jdk=java8
catalogJdk=latest_oracle_8_windows

choco install jq

dest="/c/users/buildkite/.java/${jdk}"

echo "Using catalog ID $catalogJdk..."

json=$(curl --silent "https://jvm-catalog.elastic.co/jdk/$catalogJdk")

echo "Downloading $(echo "$json" | jq -r '.id')..."

curl -o "${jdk}.exe" "$(echo "$json" | jq -r '.url')"

./java8.exe /s INSTALLDIR=C:\\users\\buildkite\\.java\\java8

sleep 20
