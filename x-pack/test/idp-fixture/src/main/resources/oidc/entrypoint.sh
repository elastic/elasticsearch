#!/bin/bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.
#
# Run-time script: container entrypoint. Waits for override.properties (written
# by the test via copyFileToContainer after the container starts), then starts Tomcat.

set -e

until [ -f /config/c2id/override.properties ]; do
  echo "Waiting for properties file"
  sleep 0.5
done
echo "Properties file available. Starting server..."

exec /c2id-server/tomcat/bin/catalina.sh run
