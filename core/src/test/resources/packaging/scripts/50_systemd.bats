#!/usr/bin/env bats

# This file is used to test the elasticsearch Systemd setup.

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

@test "[SYSTEMD] daemon reload" {
    skip_not_systemd

    run systemctl daemon-reload
    [ "$status" -eq 0 ]
}

@test "[SYSTEMD] enable" {
    skip_not_systemd

    run systemctl enable elasticsearch.service
    [ "$status" -eq 0 ]

    run systemctl is-enabled elasticsearch.service
    [ "$status" -eq 0 ]
}

@test "[SYSTEMD] start" {
    skip_not_systemd

    run systemctl start elasticsearch.service
    [ "$status" -eq 0 ]

    wait_for_elasticsearch_status

    assert_file_exist "/var/run/elasticsearch/elasticsearch.pid"
}

@test "[SYSTEMD] start (running)" {
    skip_not_systemd

    run systemctl start elasticsearch.service
    [ "$status" -eq 0 ]
}

@test "[SYSTEMD] is active (running)" {
    skip_not_systemd

    run systemctl is-active elasticsearch.service
    [ "$status" -eq 0 ]
    [ "$output" = "active" ]
}

@test "[SYSTEMD] status (running)" {
    skip_not_systemd

    run systemctl status elasticsearch.service
    [ "$status" -eq 0 ]
}

##################################
# Check that Elasticsearch is working
##################################
@test "[SYSTEMD] test elasticsearch" {
    skip_not_systemd

    run_elasticsearch_tests
}

@test "[SYSTEMD] restart" {
    skip_not_systemd

    run systemctl restart elasticsearch.service
    [ "$status" -eq 0 ]

    wait_for_elasticsearch_status

    run service elasticsearch status
    [ "$status" -eq 0 ]
}

@test "[SYSTEMD] stop (running)" {
    skip_not_systemd

    run systemctl stop elasticsearch.service
    [ "$status" -eq 0 ]

    run systemctl status elasticsearch.service
    echo "$output" | grep "Active:" | grep "inactive"
}

@test "[SYSTEMD] stop (stopped)" {
    skip_not_systemd

    run systemctl stop elasticsearch.service
    [ "$status" -eq 0 ]

    run systemctl status elasticsearch.service
    echo "$output" | grep "Active:" | grep "inactive"
}

@test "[SYSTEMD] status (stopped)" {
    skip_not_systemd

    run systemctl status elasticsearch.service
    echo "$output" | grep "Active:" | grep "inactive"
}
