#!/usr/bin/env bats

# This file is used to test the installation and removal
# of a Debian package.

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
# Install DEB package
##################################
@test "[DEB] dpkg command is available" {
    skip_not_dpkg
    run dpkg --version
    [ "$status" -eq 0 ]
}

@test "[DEB] package is available" {
    skip_not_dpkg
    count=$(find . -type f -name 'elastic*.deb' | wc -l)
    [ "$count" -eq 1 ]
}

@test "[DEB] package is not installed" {
    skip_not_dpkg
    run dpkg -s 'elasticsearch' >&2
    [ "$status" -eq 1 ]
}

@test "[DEB] install package" {
    skip_not_dpkg
    run dpkg -i elasticsearch*.deb >&2
    [ "$status" -eq 0 ]
}

@test "[DEB] package is installed" {
    skip_not_dpkg
    run dpkg -s 'elasticsearch' >&2
    [ "$status" -eq 0 ]
}

##################################
# Check that the package is correctly installed
##################################
@test "[DEB] verify package installation" {
    skip_not_dpkg

    verify_package_installation
}

##################################
# Check that Elasticsearch is working
##################################
@test "[TEST] test elasticsearch" {
    skip_not_dpkg

    start_elasticsearch_service

    run_elasticsearch_tests
}

##################################
# Uninstall DEB package
##################################
@test "[DEB] remove package" {
    skip_not_dpkg
    run dpkg -r 'elasticsearch' >&2
    [ "$status" -eq 0 ]
}

@test "[DEB] package has been removed" {
    skip_not_dpkg
    run dpkg -s 'elasticsearch' >&2
    [ "$status" -eq 0 ]
    echo "$output" | grep -i "status" | grep -i "deinstall ok"
}

@test "[DEB] verify package removal" {
    skip_not_dpkg

    # The removal must stop the service
    count=$(ps | grep Elasticsearch | wc -l)
    [ "$count" -eq 0 ]

    # The removal must disable the service
    # see prerm file
    if is_systemd; then
        run systemctl status elasticsearch.service
        [ "$status" -eq 3 ]

        run systemctl is-enabled elasticsearch.service
        [ "$status" -eq 1 ]
    fi

    # Those directories are deleted when removing the package
    # see postrm file
    assert_file_not_exist "/var/log/elasticsearch"
    assert_file_not_exist "/usr/share/elasticsearch/plugins"
    assert_file_not_exist "/var/run/elasticsearch"

    # The configuration files are still here
    assert_file_exist "/etc/elasticsearch"
    assert_file_exist "/etc/elasticsearch/elasticsearch.yml"
    assert_file_exist "/etc/elasticsearch/logging.yml"

    # The env file is still here
    assert_file_exist "/etc/default/elasticsearch"

    # The service files are still here
    assert_file_exist "/etc/init.d/elasticsearch"
    assert_file_exist "/usr/lib/systemd/system/elasticsearch.service"
}

@test "[DEB] purge package" {
    skip_not_dpkg
    run dpkg --purge 'elasticsearch' >&2
    [ "$status" -eq 0 ]
}

@test "[DEB] verify package purge" {
    skip_not_dpkg

    # all remaining files are deleted by the purge
    assert_file_not_exist "/etc/elasticsearch"
    assert_file_not_exist "/etc/elasticsearch/elasticsearch.yml"
    assert_file_not_exist "/etc/elasticsearch/logging.yml"

    assert_file_not_exist "/etc/default/elasticsearch"

    assert_file_not_exist "/etc/init.d/elasticsearch"
    assert_file_not_exist "/usr/lib/systemd/system/elasticsearch.service"

    assert_file_not_exist "/usr/share/elasticsearch"

    assert_file_not_exist "/usr/share/doc/elasticsearch"
    assert_file_not_exist "/usr/share/doc/elasticsearch/copyright"
}

@test "[DEB] package has been completly removed" {
    skip_not_dpkg
    run dpkg -s 'elasticsearch' >&2
    [ "$status" -eq 1 ]
}
