#!/usr/bin/env bats

# This file is used to test the elasticsearch init.d scripts.

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

    # Installs a package before test
    if is_dpkg; then
        dpkg -i elasticsearch*.deb >&2 || true
    fi
    if is_rpm; then
        rpm -i elasticsearch*.rpm >&2 || true
    fi
}

@test "[INIT.D] start" {
    skip_not_sysvinit

    run service elasticsearch start
    [ "$status" -eq 0 ]

    wait_for_elasticsearch_status

    assert_file_exist "/var/run/elasticsearch/elasticsearch.pid"
}

@test "[INIT.D] status (running)" {
    skip_not_sysvinit

    run service elasticsearch status
    [ "$status" -eq 0 ]
}

##################################
# Check that Elasticsearch is working
##################################
@test "[INIT.D] test elasticsearch" {
    skip_not_sysvinit

    run_elasticsearch_tests
}

@test "[INIT.D] restart" {
    skip_not_sysvinit

    run service elasticsearch restart
    [ "$status" -eq 0 ]

    wait_for_elasticsearch_status

    run service elasticsearch status
    [ "$status" -eq 0 ]
}

@test "[INIT.D] stop (running)" {
    skip_not_sysvinit

    run service elasticsearch stop
    [ "$status" -eq 0 ]

}

@test "[INIT.D] status (stopped)" {
    skip_not_sysvinit

    run service elasticsearch status
    [ "$status" -eq 3 ]
}

# Simulates the behavior of a system restart:
# the PID directory is deleted by the operating system
# but it should not block ES from starting
# see https://github.com/elastic/elasticsearch/issues/11594
@test "[INIT.D] delete PID_DIR and restart" {
    skip_not_sysvinit

    run rm -rf /var/run/elasticsearch
    [ "$status" -eq 0 ]


    run service elasticsearch start
    [ "$status" -eq 0 ]

    wait_for_elasticsearch_status

    assert_file_exist "/var/run/elasticsearch/elasticsearch.pid"

    run service elasticsearch stop
    [ "$status" -eq 0 ]
}