#!/bin/bash

# Custom entrypoint to generate the `elasticapm.properties` file. This is a
# script instead of a static file so that the `service_version` can be set
# correctly, although for the purposes of this test, that may be pointless.

set -eo pipefail

cd /usr/share/elasticsearch/

cat > config/elasticapm.properties <<EOF
enabled: true
service_version: $STACK_VERSION
# ES does not use auto-instrumentation
instrument: false
# Required for OpenTelemetry support
enable_experimental_instrumentations: true
server_url: http://apmserver:8200
secret_token:
service_name: elasticsearch
log_level: error
log_file: _AGENT_HOME_/../../logs/apm.log
EOF

exec /usr/local/bin/docker-entrypoint.sh
