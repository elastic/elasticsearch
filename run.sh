#!/bin/bash

set -eo pipefail

# This is the path that `./gradlew localDistro` prints out at the end
cd build/distribution/local/elasticsearch-8.2.0-SNAPSHOT

# URL and token for sending traces
SERVER_URL=""
SECRET_TOKEN=""

# Clear this so that ES doesn't repeatedly complain about ignoring it
export JAVA_HOME=''

if [[ ! -f config/elasticsearch.keystore ]]; then
  ./bin/elasticsearch-keystore create
  echo "$SERVER_URL" | ./bin/elasticsearch-keystore add -x 'xpack.apm.endpoint'
  echo "$SECRET_TOKEN" | ./bin/elasticsearch-keystore add -x 'xpack.apm.token'
  # Use elastic:password for sending REST requests
  echo "password" | ./bin/elasticsearch-keystore add -x 'bootstrap.password'
fi

# export ES_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=n,suspend=n,address=*:5005 -ea "
# export ES_JAVA_OPTS="-Djava.security.debug=failure"
# export ES_JAVA_OPTS="-Djava.security.debug=access,failure"

exec ./bin/elasticsearch -Expack.apm.tracing.enabled=true
