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
load $BATS_UTILS/xpack.bash

setup() {
    skip_not_tar_gz
    export ESHOME=/tmp/elasticsearch
    export PACKAGE_NAME="elasticsearch"
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=elasticsearch
}

@test "[X-PACK] install default distribution" {
    # Cleans everything for the 1st execution
    clean_before_test

    # Install the archive
    install_archive
    set_debug_logging
}

@test "[X-PACK] verify x-pack installation" {
    verify_xpack_installation
}

@test "[X-PACK] verify croneval works" {
    run $ESHOME/bin/elasticsearch-croneval "0 0 20 ? * MON-THU" -c 2
    [ "$status" -eq 0 ]
    [[ "$output" == *"Valid!"* ]] || {
      echo "Expected output message to contain [Valid!] but found: $output"
      false
    }
}
