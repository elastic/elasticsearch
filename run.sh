#!/bin/bash

set -eo pipefail

AGENT_VERSION=$(awk '/apm_agent/ { print $3 }' build-tools-internal/version.properties)

# This is the path that `./gradlew localDistro` prints out at the end
cd build/distribution/local/elasticsearch-8.3.0-SNAPSHOT

# URL and token for sending traces
SERVER_URL="https://apm-testing.apm.us-west2.gcp.elastic-cloud.com"
SECRET_TOKEN="bNJCAZxDjQwIFKxdk2"

# Optional - override the agent jar
# OVERRIDE_AGENT_JAR="$HOME/.m2/repository/co/elastic/apm/elastic-apm-agent/1.30.1-SNAPSHOT/elastic-apm-agent-1.30.1-SNAPSHOT.jar"

# Clear this so that ES doesn't repeatedly complain about ignoring it
export JAVA_HOME=''

if [[ ! -f config/elasticsearch.keystore ]]; then
  ./bin/elasticsearch-keystore create
  # Use elastic:password for sending REST requests
  echo "password" | ./bin/elasticsearch-keystore add -x 'bootstrap.password'
fi

AGENT_JAR="modules/apm-integration/elastic-apm-agent-${AGENT_VERSION}.jar"

if [[ -n "$OVERRIDE_AGENT_JAR" ]]; then
  # Copy in WIP agent
  cp "$OVERRIDE_AGENT_JAR" "$AGENT_JAR"
fi

AGENT_OPTS=""
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.service_name=elasticsearch"
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.instrument=false"
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.server_url=$SERVER_URL"
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.secret_token=$SECRET_TOKEN"
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.service_version=8.2.0-SNAPSHOT"
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.environment=dev"
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.log_level=trace"
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.log_file=$PWD/apm.log"
AGENT_OPTS="$AGENT_OPTS -Delastic.apm.enable_experimental_instrumentations=true"

export ES_SERVER_OPTS="-ea -javaagent:$AGENT_JAR $AGENT_OPTS"

# export ES_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=*:5007 "
export ES_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5007 "

# export ES_JAVA_OPTS="-Djava.security.debug=failure"
# export ES_JAVA_OPTS="-Djava.security.debug=access,failure"

exec ./bin/elasticsearch -Expack.apm.tracing.enabled=true -Eingest.geoip.downloader.enabled=false
