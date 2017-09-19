#!/usr/bin/env bats

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

load $BATS_UTILS/utils.bash
load $BATS_UTILS/plugins.bash
load $BATS_UTILS/xpack.bash

setup() {
    if [ $BATS_TEST_NUMBER == 1 ]; then
        clean_before_test
        install
    fi
}

if [[ "$BATS_TEST_FILENAME" =~ 20_tar_keystore.bats$ ]]; then
    load $BATS_UTILS/tar.bash
    GROUP='TAR KEYSTORE'
    install() {
        install_archive
        verify_archive_installation
    }
    export ESHOME=/tmp/elasticsearch
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=elasticsearch
else
    load $BATS_UTILS/packages.bash
    if is_rpm; then
        GROUP='RPM KEYSTORE'
    elif is_dpkg; then
        GROUP='DEB KEYSTORE'
    fi
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=root
    install() {
        install_package
        verify_package_installation
    }
fi

@test "[$GROUP] keystore does not exist" {
    assert_file_not_exist /etc/elasticsearch/elasticsearch.keystore
}

@test "[$GROUP] keystore exists after install" {
    install_and_check_plugin x pack x-pack-*.jar
    verify_xpack_installation
}
