#!/usr/bin/env bats

# This file is used to test the elasticsearch Systemd setup.

# WARNING: This testing file must be executed as root and can
# dramatically change your system. It should only be executed
# in a throw-away VM like those made by the Vagrantfile at
# the root of the Elasticsearch source code. This should
# cause the script to fail if it is executed any other way:
[ -f /etc/is_vagrant_vm ] || {
  >&2 echo "must be run on a vagrant VM"
  exit 1
}

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
load $BATS_UTILS/utils.bash
load $BATS_UTILS/packages.bash
load $BATS_UTILS/plugins.bash

# Cleans everything for the 1st execution
setup() {
    skip_not_systemd
    skip_not_dpkg_or_rpm
    export_elasticsearch_paths
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
    # Capture the current epoch in millis
    run date +%s
    epoch="$output"

    # The OpenJDK packaged for CentOS and OEL both override the default value (false) for the JVM option "AssumeMP".
    #
    # Because it is forced to "true" by default for these packages, the following warning message is printed to the
    # standard output when the Vagrant box has only 1 CPU:
    #       OpenJDK 64-Bit Server VM warning: If the number of processors is expected to increase from one, then you should configure
    #       the number of parallel GC threads appropriately using -XX:ParallelGCThreads=N
    #
    # This message will then fail the next test where we check if no entries have been added to the journal.
    #
    # This message appears since with java-1.8.0-openjdk-1.8.0.111-1.b15.el7_2.x86_64 because of the commit:
    #       2016-10-10  - Andrew Hughes <gnu.andrew@redhat.com> - 1:1.8.0.111-1.b15 - Turn debug builds on for all JIT architectures.
    #                     Always AssumeMP on RHEL.
    #                   - Resolves: rhbz#1381990
    #
    if [ -x "$(command -v lsb_release)" ]; then
        # Here we set the "-XX:-AssumeMP" option to false again:
        lsb_release=$(lsb_release -i)
        if [[ "$lsb_release" =~ "CentOS" ]] || [[ "$lsb_release" =~ "OracleServer" ]]; then
            echo "-XX:-AssumeMP" >> $ESCONFIG/jvm.options
        fi
    fi

    systemctl start elasticsearch.service
    wait_for_elasticsearch_status
    assert_file_exist "/var/run/elasticsearch/elasticsearch.pid"
    assert_file_exist "/var/log/elasticsearch/elasticsearch.log"

    # Converts the epoch back in a human readable format
    run date --date=@$epoch "+%Y-%m-%d %H:%M:%S"
    since="$output"

    # Verifies that no new entries in journald have been added
    # since the last start
    result="$(journalctl _SYSTEMD_UNIT=elasticsearch.service --since "$since" --output cat | wc -l)"
    [ "$result" -eq "0" ] || {
            echo "Expected no entries in journalctl for the Elasticsearch service but found:"
            journalctl _SYSTEMD_UNIT=elasticsearch.service --since "$since"
            false
        }
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
}

@test "[SYSTEMD] status (stopping)" {
    run systemctl status elasticsearch.service
    # I'm not sure why suse exits 0 here, but it does
    if [ ! -e /etc/SuSE-release ]; then
        [ "$status" -eq 3 ] || "Expected exit code 3 meaning stopped but got $status"
    fi
    echo "$output" | grep "Active:" | grep "inactive"
}

@test "[SYSTEMD] stop (stopped)" {
    systemctl stop elasticsearch.service
}

@test "[SYSTEMD] status (stopped)" {
    run systemctl status elasticsearch.service
    # I'm not sure why suse exits 0 here, but it does
    if [ ! -e /etc/SuSE-release ]; then
        [ "$status" -eq 3 ] || "Expected exit code 3 meaning stopped but got $status"
    fi
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

@test "[SYSTEMD] start Elasticsearch with custom JVM options" {
    assert_file_exist $ESENVFILE
    local temp=`mktemp -d`
    cp "$ESCONFIG"/elasticsearch.yml "$temp"
    cp "$ESCONFIG"/log4j2.properties "$temp"
    touch "$temp/jvm.options"
    chown -R elasticsearch:elasticsearch "$temp"
    echo "-Xms512m" >> "$temp/jvm.options"
    echo "-Xmx512m" >> "$temp/jvm.options"
    # we have to disable Log4j from using JMX lest it will hit a security
    # manager exception before we have configured logging; this will fail
    # startup since we detect usages of logging before it is configured
    echo "-Dlog4j2.disable.jmx=true" >> "$temp/jvm.options"
    cp $ESENVFILE "$temp/elasticsearch"
    echo "ES_PATH_CONF=\"$temp\"" >> $ESENVFILE
    echo "ES_JAVA_OPTS=\"-XX:-UseCompressedOops\"" >> $ESENVFILE
    service elasticsearch start
    wait_for_elasticsearch_status
    curl -s -XGET localhost:9200/_nodes | fgrep '"heap_init_in_bytes":536870912'
    curl -s -XGET localhost:9200/_nodes | fgrep '"using_compressed_ordinary_object_pointers":"false"'
    service elasticsearch stop
    cp "$temp/elasticsearch" $ESENVFILE
}

@test "[SYSTEMD] masking systemd-sysctl" {
    clean_before_test

    systemctl mask systemd-sysctl.service
    install_package

    systemctl unmask systemd-sysctl.service
}

@test "[SYSTEMD] service file sets limits" {
    clean_before_test
    install_package
    systemctl start elasticsearch.service
    wait_for_elasticsearch_status
    local pid=$(cat /var/run/elasticsearch/elasticsearch.pid)
    local max_file_size=$(cat /proc/$pid/limits | grep "Max file size" | awk '{ print $4 }')
    [ "$max_file_size" == "unlimited" ]
    local max_processes=$(cat /proc/$pid/limits | grep "Max processes" | awk '{ print $3 }')
    [ "$max_processes" == "4096" ]
    local max_open_files=$(cat /proc/$pid/limits | grep "Max open files" | awk '{ print $4 }')
    [ "$max_open_files" == "65536" ]
    local max_address_space=$(cat /proc/$pid/limits | grep "Max address space" | awk '{ print $4 }')
    [ "$max_address_space" == "unlimited" ]
    systemctl stop elasticsearch.service
}

@test "[SYSTEMD] test runtime directory" {
    clean_before_test
    install_package
    sudo rm -rf /var/run/elasticsearch
    systemctl start elasticsearch.service
    wait_for_elasticsearch_status
    [ -d /var/run/elasticsearch ]
    systemctl stop elasticsearch.service
}
