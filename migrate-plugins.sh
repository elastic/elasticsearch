#!/usr/bin/env bash

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

set -euo pipefail

export SUCCESS=false

finish() {
    if [ $SUCCESS != "true" ]; then
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "Script did NOT complete successfully!"
        echo "It likely left the working tree in an unclean state."
        echo "Please clean up before re-running the script."
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        exit 1
    else
        echo "Migration finished successfully."
    fi
}
trap finish EXIT

echo "Migrating..."

# TODO: implement actual moving of plugins


# Migration has been successful
export SUCCESS=true
echo "Done."
