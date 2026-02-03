#!/bin/bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.
#
# Build-time script: runs during "docker build" to prepare the image.
# Run logic (wait for config, start Tomcat) lives in entrypoint.sh.

set -e

# Ensure config directory exists so the runtime can write override.properties there
mkdir -p /config/c2id
