#!/usr/bin/env bats

# This file is used to test the installation of a RPM package.

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
# Install RPM package
##################################
@test "[RPM] rpm command is available" {
    skip_not_rpm
    run rpm --version
    [ "$status" -eq 0 ]
}

@test "[RPM] package is available" {
    skip_not_rpm
    count=$(find . -type f -name 'elastic*.rpm' | wc -l)
    [ "$count" -eq 1 ]
}

@test "[RPM] package is not installed" {
    skip_not_rpm
    run rpm -qe 'elasticsearch' >&2
    [ "$status" -eq 1 ]
}

@test "[RPM] install package" {
    skip_not_rpm
    run rpm -i elasticsearch*.rpm >&2
    [ "$status" -eq 0 ]
}

@test "[RPM] package is installed" {
    skip_not_rpm
    run rpm -qe 'elasticsearch' >&2
    [ "$status" -eq 0 ]
}

##################################
# Check that the package is correctly installed
##################################
@test "[RPM] verify package installation" {
    skip_not_rpm

    verify_package_installation
}

##################################
# Check that Elasticsearch is working
##################################
@test "[TEST] test elasticsearch" {
    skip_not_rpm

    start_elasticsearch_service

    run_elasticsearch_tests
}

##################################
# Uninstall RPM package
##################################
@test "[RPM] remove package" {
    skip_not_rpm
    run rpm -e 'elasticsearch' >&2
    [ "$status" -eq 0 ]
}

@test "[RPM] package has been removed" {
    skip_not_rpm
    run rpm -qe 'elasticsearch' >&2
    [ "$status" -eq 1 ]
}

@test "[RPM] verify package removal" {
    skip_not_rpm

    # The removal must stop the service
    count=$(ps | grep Elasticsearch | wc -l)
    [ "$count" -eq 0 ]

    # The removal must disable the service
    # see prerm file
    if is_systemd; then
        run systemctl status elasticsearch.service
        echo "$output" | grep "Active:" | grep 'inactive\|failed'

        run systemctl is-enabled elasticsearch.service
        [ "$status" -eq 1 ]
    fi

    # Those directories are deleted when removing the package
    # see postrm file
    assert_file_not_exist "/var/log/elasticsearch"
    assert_file_not_exist "/usr/share/elasticsearch/plugins"
    assert_file_not_exist "/var/run/elasticsearch"

    assert_file_not_exist "/etc/elasticsearch"
    assert_file_not_exist "/etc/elasticsearch/elasticsearch.yml"
    assert_file_not_exist "/etc/elasticsearch/logging.yml"

    assert_file_not_exist "/etc/init.d/elasticsearch"
    assert_file_not_exist "/usr/lib/systemd/system/elasticsearch.service"

    assert_file_not_exist "/etc/sysconfig/elasticsearch"
}
