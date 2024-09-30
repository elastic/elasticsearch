#!/bin/bash

set -euo pipefail

AGENT_VERSION="8.10.1"

ELASTIC_AGENT_URL=$(vault read -field=url secret/ci/elastic-elasticsearch/elastic-agent-token)
ELASTIC_AGENT_TOKEN=$(vault read -field=token secret/ci/elastic-elasticsearch/elastic-agent-token)

ELASTIC_AGENT_DIR=/opt/elastic-agent
IS_WINDOWS=""

# Windows
if uname -a | grep -q MING; then
  ELASTIC_AGENT_DIR=/c/elastic-agent
  IS_WINDOWS="true"

  # Make sudo a no-op on Windows
  sudo() {
    "$@"
  }
fi

if [[ ! -d $ELASTIC_AGENT_DIR ]]; then
  sudo mkdir $ELASTIC_AGENT_DIR

  if [[ "$IS_WINDOWS" != "true" ]]; then
    sudo chown -R buildkite-agent:buildkite-agent $ELASTIC_AGENT_DIR
  fi

  cd $ELASTIC_AGENT_DIR

  archive="elastic-agent-$AGENT_VERSION-linux-x86_64.tar.gz"
  if [[ "$IS_WINDOWS" == "true" ]]; then
    archive="elastic-agent-$AGENT_VERSION-windows-x86_64.zip"
  elif [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
    archive="elastic-agent-$AGENT_VERSION-linux-arm64.tar.gz"
  fi

  curl -L -O "https://artifacts.elastic.co/downloads/beats/elastic-agent/$archive"

  if [[ "$IS_WINDOWS" == "true" ]]; then
    unzip "$archive"
    mv elastic-agent-*/* .
  else
    tar xzf "$archive" --directory=. --strip-components=1
  fi
fi

cd $ELASTIC_AGENT_DIR
sudo ./elastic-agent install -f --url="$ELASTIC_AGENT_URL" --enrollment-token="$ELASTIC_AGENT_TOKEN"
