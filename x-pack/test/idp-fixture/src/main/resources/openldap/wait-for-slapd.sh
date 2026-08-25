#!/bin/bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.
#

# The readiness check in the base image only checks for a PID associated with slapd, not that slapd is actually ready to serve LDAP.
# This causes intermittent startup errors when `ldapadd` is run before slapd is ready to accept it.
# This script is intended to replace the faulty readiness check.

set -euo pipefail

last_error=""

for ((attempt = 1; attempt <= 300; attempt++)); do
    if last_error="$(
        ldapsearch \
            -Q \
            -Y EXTERNAL \
            -H ldapi:/// \
            -s base \
            -b cn=config \
            dn 2>&1
    )"; then
        exit 0
    fi

    sleep 0.1
done

printf 'OpenLDAP did not become ready within 30 seconds. Last ldapsearch error:\n%s\n' \
    "$last_error" >&2
exit 1
