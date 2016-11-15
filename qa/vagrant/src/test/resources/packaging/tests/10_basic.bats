#!/usr/bin/env bats

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

# This file is used to test the X-Pack package.

# WARNING: This testing file must be executed as root and can
# dramatically change your system. It removes the 'elasticsearch'
# user/group and also many directories. Do not execute this file
# unless you know exactly what you are doing.

load $BATS_UTILS/utils.bash
load $BATS_UTILS/tar.bash
load $BATS_UTILS/plugins.bash

setup() {
    skip_not_tar_gz
    export ESHOME=/tmp/elasticsearch
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=elasticsearch
}

@test "[X-PACK] install x-pack" {
    # Cleans everything for the 1st execution
    clean_before_test

    # Install the archive
    install_archive

    count=$(find . -type f -name 'x-pack*.zip' | wc -l)
    [ "$count" -eq 1 ]

    install_and_check_plugin x pack x-pack-*.jar
}

@test "[X-PACK] verify x-pack installation" {
    assert_file "$ESHOME/bin/x-pack" d elasticsearch elasticsearch 755
    assert_file "$ESHOME/bin/x-pack/certgen" f elasticsearch elasticsearch 755
    assert_file "$ESHOME/bin/x-pack/croneval" f elasticsearch elasticsearch 755
    assert_file "$ESHOME/bin/x-pack/extension" f elasticsearch elasticsearch 755
    assert_file "$ESHOME/bin/x-pack/migrate" f elasticsearch elasticsearch 755
    assert_file "$ESHOME/bin/x-pack/syskeygen" f elasticsearch elasticsearch 755
    assert_file "$ESHOME/bin/x-pack/users" f elasticsearch elasticsearch 755
    assert_file "$ESCONFIG/x-pack" d elasticsearch elasticsearch 750
    assert_file "$ESCONFIG/x-pack/users" f elasticsearch elasticsearch 660
    assert_file "$ESCONFIG/x-pack/users_roles" f elasticsearch elasticsearch 660
    assert_file "$ESCONFIG/x-pack/roles.yml" f elasticsearch elasticsearch 660
    assert_file "$ESCONFIG/x-pack/role_mapping.yml" f elasticsearch elasticsearch 660
    assert_file "$ESCONFIG/x-pack/log4j2.properties" f elasticsearch elasticsearch 660
}