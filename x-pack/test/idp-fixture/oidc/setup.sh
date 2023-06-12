#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.
#

#!/bin/bash

# TODO HACK HACK HACK
python3 -m http.server 8080 &
PY_PID=$!

until [ -f /config/c2id/override.properties ]
do
  echo "Waiting"
  sleep 1
done
echo "File found"

kill $PY_PID

bash /c2id-server/tomcat/bin/catalina.sh run
