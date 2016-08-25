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
load os_package
load plugins

# Cleans everything for the 1st execution
setup() {
    skip_not_sysvinit
    skip_not_dpkg_or_rpm
    export_elasticsearch_paths
}

@test "[INIT.D] remove any leftover configuration to start elasticsearch on restart" {
    # This configuration can be added with a command like:
    # $ sudo update-rc.d elasticsearch defaults 95 10
    # but we want to test that the RPM and deb _don't_ add it on its own.
    # Note that it'd be incorrect to use:
    # $ sudo update-rc.d elasticsearch disable
    # here because that'd prevent elasticsearch from installing the symlinks
    # that cause it to be started on restart.
    sudo update-rc.d -f elasticsearch remove || true
    sudo chkconfig elasticsearch off || true
}

@test "[INIT.D] install elasticsearch" {
    clean_before_test
    install_package
}

@test "[INIT.D] elasticsearch fails if startup script is not executable" {
    local INIT="/etc/init.d/elasticsearch"
    local DAEMON="$ESHOME/bin/elasticsearch"
    
    sudo chmod -x "$DAEMON"
    run "$INIT"
    sudo chmod +x "$DAEMON"
    
    [ "$status" -eq 1 ]
    [[ "$output" == *"The elasticsearch startup script does not exists or it is not executable, tried: $DAEMON"* ]]
}

@test "[INIT.D] daemon isn't enabled on restart" {
    # Rather than restart the VM which would be slow we check for the symlinks
    # that init.d uses to restart the application on startup.
    ! find /etc/rc[0123456].d | grep elasticsearch
    # Note that we don't use -iname above because that'd have to look like:
    # [ $(find /etc/rc[0123456].d -iname "elasticsearch*" | wc -l) -eq 0 ]
    # Which isn't really clearer than what we do use.
}

@test "[INIT.D] start" {
    # Install scripts used to test script filters and search templates before
    # starting Elasticsearch so we don't have to wait for elasticsearch to scan for
    # them.
    install_elasticsearch_test_scripts
    service elasticsearch start
    wait_for_elasticsearch_status
    assert_file_exist "/var/run/elasticsearch/elasticsearch.pid"
}

@test "[INIT.D] status (running)" {
    service elasticsearch status
}

##################################
# Check that Elasticsearch is working
##################################
@test "[INIT.D] test elasticsearch" {
    run_elasticsearch_tests
}

@test "[INIT.D] restart" {
    service elasticsearch restart

    wait_for_elasticsearch_status

    service elasticsearch status
}

@test "[INIT.D] stop (running)" {
    service elasticsearch stop
}

@test "[INIT.D] status (stopped)" {
    run service elasticsearch status
    # precise returns 4, trusty 3
    [ "$status" -eq 3 ] || [ "$status" -eq 4 ]
}

@test "[INIT.D] don't mkdir when it contains a comma" {
    # Remove these just in case they exist beforehand
    rm -rf /tmp/aoeu,/tmp/asdf
    rm -rf /tmp/aoeu,
    # set DATA_DIR to DATA_DIR=/tmp/aoeu,/tmp/asdf
    sed -i 's/DATA_DIR=.*/DATA_DIR=\/tmp\/aoeu,\/tmp\/asdf/' /etc/init.d/elasticsearch
    cat /etc/init.d/elasticsearch | grep "DATA_DIR"
    service elasticsearch start
    wait_for_elasticsearch_status
    assert_file_not_exist /tmp/aoeu,/tmp/asdf
    assert_file_not_exist /tmp/aoeu,
    service elasticsearch stop
    run service elasticsearch status
    # precise returns 4, trusty 3
    [ "$status" -eq 3 ] || [ "$status" -eq 4 ]
}

# Simulates the behavior of a system restart:
# the PID directory is deleted by the operating system
# but it should not block ES from starting
# see https://github.com/elastic/elasticsearch/issues/11594
@test "[INIT.D] delete PID_DIR and restart" {
    rm -rf /var/run/elasticsearch

    service elasticsearch start

    wait_for_elasticsearch_status

    assert_file_exist "/var/run/elasticsearch/elasticsearch.pid"

    service elasticsearch stop
}
