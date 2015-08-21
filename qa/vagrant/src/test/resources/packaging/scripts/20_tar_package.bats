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
load tar

setup() {
    skip_not_tar_gz
}

##################################
# Install TAR GZ package
##################################
@test "[TAR] tar command is available" {
    # Cleans everything for the 1st execution
    clean_before_test
    run tar --version
    [ "$status" -eq 0 ]
}

@test "[TAR] archive is available" {
    count=$(find . -type f -name 'elasticsearch*.tar.gz' | wc -l)
    [ "$count" -eq 1 ]
}

@test "[TAR] archive is not installed" {
    count=$(find /tmp -type d -name 'elasticsearch*' | wc -l)
    [ "$count" -eq 0 ]
}

@test "[TAR] install archive" {
    # Install the archive
    install_archive

    count=$(find /tmp -type d -name 'elasticsearch*' | wc -l)
    [ "$count" -eq 1 ]

    # Its simpler to check that the install was correct in this test rather
    # than in another test because install_archive sets a number of path
    # variables that verify_archive_installation reads. To separate this into
    # another test you'd have to recreate the variables.
    verify_archive_installation
}

##################################
# Check that Elasticsearch is working
##################################
@test "[TAR] test elasticsearch" {
    start_elasticsearch_service

    run_elasticsearch_tests

    stop_elasticsearch_service

    rm -rf "/tmp/elasticsearch"
}
