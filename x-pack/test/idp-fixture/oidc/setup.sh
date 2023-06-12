#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.
#

#!/bin/bash

# HACK: we start serving on 8080 so that we can progress to the postProcessFixture step. That's the step during which
# we have access to the ephemeral port of the container, which we need to properly configure the issuer field in c2id
# config
python3 -m http.server 8080 &
PY_PID=$!

until [ -f /config/c2id/override.properties ]
do
  echo "Waiting for properties file"
  sleep 1
done
echo "Properties file available. Starting server..."

# now that the properties file is configured and available, stop our fake server and launch the real thing
kill $PY_PID
bash /c2id-server/tomcat/bin/catalina.sh run
