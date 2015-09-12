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
    skip_not_systemd
    skip_not_dpkg_or_rpm
}

@test "[SYSTEMD] install elasticsearch" {
    clean_before_test
    install_package
}

@test "[SYSTEMD] daemon reload after install" {
    systemctl daemon-reload
}

@test "[SYSTEMD] daemon isn't enabled on restart" {
    # Rather than restart the VM we just ask systemd if it plans on starting
    # elasticsearch on restart. Not as strong as a restart but much much
    # faster.
    run systemctl is-enabled elasticsearch.service
    [ "$output" = "disabled" ]
}

@test "[SYSTEMD] enable" {
    systemctl enable elasticsearch.service

    systemctl is-enabled elasticsearch.service
}

@test "[SYSTEMD] start" {
    systemctl start elasticsearch.service

    wait_for_elasticsearch_status

    assert_file_exist "/var/run/elasticsearch/elasticsearch.pid"
}

@test "[SYSTEMD] start (running)" {
    systemctl start elasticsearch.service
}

@test "[SYSTEMD] is active (running)" {
    run systemctl is-active elasticsearch.service
    [ "$status" -eq 0 ]
    [ "$output" = "active" ]
}

@test "[SYSTEMD] status (running)" {
    systemctl status elasticsearch.service
}

##################################
# Check that Elasticsearch is working
##################################
@test "[SYSTEMD] test elasticsearch" {
    run_elasticsearch_tests
}

@test "[SYSTEMD] restart" {
    systemctl restart elasticsearch.service

    wait_for_elasticsearch_status

    service elasticsearch status
}

@test "[SYSTEMD] stop (running)" {
    systemctl stop elasticsearch.service

    run systemctl status elasticsearch.service
    [ "$status" -eq 3 ] || "Expected exit code 3 meaning stopped"
    echo "$output" | grep "Active:" | grep "inactive"
}

@test "[SYSTEMD] stop (stopped)" {
    systemctl stop elasticsearch.service

    run systemctl status elasticsearch.service
    [ "$status" -eq 3 ] || "Expected exit code 3 meaning stopped"
    echo "$output" | grep "Active:" | grep "inactive"
}

@test "[SYSTEMD] status (stopped)" {
    run systemctl status elasticsearch.service
    [ "$status" -eq 3 ] || "Expected exit code 3 meaning stopped"
    echo "$output" | grep "Active:" | grep "inactive"
}

# Simulates the behavior of a system restart:
# the PID directory is deleted by the operating system
# but it should not block ES from starting
# see https://github.com/elastic/elasticsearch/issues/11594
@test "[SYSTEMD] delete PID_DIR and restart" {
    rm -rf /var/run/elasticsearch

    systemd-tmpfiles --create

    systemctl start elasticsearch.service

    wait_for_elasticsearch_status

    assert_file_exist "/var/run/elasticsearch/elasticsearch.pid"

    systemctl stop elasticsearch.service
}
