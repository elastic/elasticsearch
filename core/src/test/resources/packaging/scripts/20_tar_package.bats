#!/usr/bin/env bats

# This file is used to test the tar gz package.

# WARNING: This testing file must be executed as root and can
# dramatically change your system. It removes the 'elasticsearch'
# user/group and also many directories. Do not execute this file
# unless you know exactly what you are doing.

# The test case can be executed with the Bash Automated
# Testing System tool available at https://github.com/sstephenson/bats
# Thanks to Sam Stephenson!

# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Load test utilities
load packaging_test_utils

# Cleans everything for the 1st execution
setup() {
    if [ "$BATS_TEST_NUMBER" -eq 1 ]; then
        clean_before_test
    fi
}

##################################
# Install TAR GZ package
##################################
@test "[TAR] tar command is available" {
    skip_not_tar_gz
    run tar --version
    [ "$status" -eq 0 ]
}

@test "[TAR] archive is available" {
    skip_not_tar_gz
    count=$(find . -type f -name 'elasticsearch*.tar.gz' | wc -l)
    [ "$count" -eq 1 ]
}

@test "[TAR] archive is not installed" {
    skip_not_tar_gz
    count=$(find /tmp -type d -name 'elasticsearch*' | wc -l)
    [ "$count" -eq 0 ]
}

@test "[TAR] install archive" {
    skip_not_tar_gz

    # Install the archive
    install_archive

    count=$(find /tmp -type d -name 'elasticsearch*' | wc -l)
    [ "$count" -eq 1 ]
}

##################################
# Check that the archive is correctly installed
##################################
@test "[TAR] verify archive installation" {
    skip_not_tar_gz

    verify_archive_installation "/tmp/elasticsearch"
}

##################################
# Check that Elasticsearch is working
##################################
@test "[TAR] test elasticsearch" {
    skip_not_tar_gz

    start_elasticsearch_service

    run_elasticsearch_tests

    stop_elasticsearch_service

    run rm -rf "/tmp/elasticsearch"
    [ "$status" -eq 0 ]
}
