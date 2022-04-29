#!/bin/bash

# Custom entrypoint to generate the `elasticapm.properties` file. This is a
# script instead of a static file so that the `service_version` can be set
# correctly, although for the purposes of this test, that may be pointless.

set -eo pipefail

cd /usr/share/elasticsearch/

sed -i -e '
  s|enabled: .*|enabled: true|
  s|# server_url: .*|server_url: http://apmserver:8200|
  s|# secret_token:.*|secret_token: |
  s|metrics_interval:.*|metrics_interval: 1s|
' config/elasticapm.properties

exec /usr/local/bin/docker-entrypoint.sh
