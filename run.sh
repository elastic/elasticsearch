#!/bin/bash

set -eo pipefail

cd build/distribution/local/elasticsearch-8.2.0-SNAPSHOT

SERVER_URL=""
SECRET_TOKEN=""

export JAVA_HOME=''

if [[ ! -f config/elasticsearch.keystore ]]; then
  ./bin/elasticsearch-keystore create
#   echo "$SERVER_URL" | ./bin/elasticsearch-keystore add -x -f -v 'xpack.apm.endpoint'
#   echo "$SECRET_TOKEN" | ./bin/elasticsearch-keystore add -x -f -v 'xpack.apm.token'
  echo "password" | ./bin/elasticsearch-keystore add -x 'bootstrap.password'
fi

# AGENT="$PWD/modules/apm-integration/elastic-apm-agent-1.29.0.jar"
AGENT="$PWD/modules/apm-integration/elastic-apm-agent-1.29.0.jar"

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

# SUSPEND_JVM="n"

# export ES_SERVER_OPTS="-agentlib:jdwp=transport=dt_socket,server=n,suspend=n,address=*:5005 -ea -javaagent:$AGENT $AGENT_OPTS"
export ES_SERVER_OPTS="-ea -javaagent:$AGENT $AGENT_OPTS"

exec ./bin/elasticsearch -Expack.apm.tracing.enabled=true
