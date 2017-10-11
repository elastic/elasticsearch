#!/bin/bash

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

verify_xpack_installation() {
    local user="$ESPLUGIN_COMMAND_USER"
    local group="$ESPLUGIN_COMMAND_USER"

    assert_file "$ESHOME/bin/x-pack" d $user $group 755
    assert_file "$ESHOME/bin/x-pack/certgen" f $user $group 755
    assert_file "$ESHOME/bin/x-pack/croneval" f $user $group 755
    assert_file "$ESHOME/bin/x-pack/extension" f $user $group 755
    assert_file "$ESHOME/bin/x-pack/migrate" f $user $group 755
    assert_file "$ESHOME/bin/x-pack/setup-passwords" f $user $group 755
    assert_file "$ESHOME/bin/x-pack/syskeygen" f $user $group 755
    assert_file "$ESHOME/bin/x-pack/users" f $user $group 755
    assert_file "$ESHOME/bin/x-pack/x-pack-env" f $user $group 755
    assert_number_of_files "$ESHOME/bin/x-pack/" 16

    assert_file "$ESCONFIG/x-pack" d $user elasticsearch 750
    assert_file "$ESCONFIG/x-pack/users" f $user elasticsearch 660
    assert_file "$ESCONFIG/x-pack/users_roles" f $user elasticsearch 660
    assert_file "$ESCONFIG/x-pack/roles.yml" f $user elasticsearch 660
    assert_file "$ESCONFIG/x-pack/role_mapping.yml" f $user elasticsearch 660
    assert_file "$ESCONFIG/x-pack/log4j2.properties" f $user elasticsearch 660
    assert_number_of_files "$ESCONFIG/x-pack" 5

    assert_file "$ESCONFIG/elasticsearch.keystore" f $user elasticsearch 660
}

assert_number_of_files() {
    local directory=$1
    local expected=$2

    local count=$(ls "$directory" | wc -l)
    [ "$count" -eq "$expected" ] || {
        echo "Expected $expected files in $directory but found: $count"
        false
    }
}

wait_for_xpack() {
    local host=${1:-localhost}
    local port=${2:-9200}
    for i in {1..30}; do
        echo "GET / HTTP/1.0" > /dev/tcp/$host/$port && break || sleep 1;
    done
}