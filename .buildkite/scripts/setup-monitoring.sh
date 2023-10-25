#!/bin/bash

set -euo pipefail

ELASTIC_AGENT_URL=$(vault read -field=url secret/ci/elastic-elasticsearch/elastic-agent-token)
ELASTIC_AGENT_TOKEN=$(vault read -field=token secret/ci/elastic-elasticsearch/elastic-agent-token)

if [[ ! -d /opt/elastic-agent ]]; then
  sudo mkdir /opt/elastic-agent
  sudo chown -R buildkite-agent:buildkite-agent /opt/elastic-agent
  cd /opt/elastic-agent

  archive=elastic-agent-8.10.1-linux-x86_64.tar.gz
  if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
    archive=elastic-agent-8.10.1-linux-arm64.tar.gz
  fi

  curl -L -O "https://artifacts.elastic.co/downloads/beats/elastic-agent/$archive"

  tar xzf "$archive" --directory=. --strip-components=1
fi

cd /opt/elastic-agent
sudo ./elastic-agent install -f --url="$ELASTIC_AGENT_URL" --enrollment-token="$ELASTIC_AGENT_TOKEN"
