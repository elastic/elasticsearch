#!/usr/bin/env bats

# This file is used to test the tar gz package.

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
load $BATS_UTILS/tar.bash
load $BATS_UTILS/plugins.bash

setup() {
    skip_not_tar_gz
    export ESHOME=/tmp/elasticsearch
    export_elasticsearch_paths
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

@test "[TAR] verify elasticsearch-plugin list runs without any plugins installed" {
    # previously this would fail because the archive installations did
    # not create an empty plugins directory
    local plugins_list=`$ESHOME/bin/elasticsearch-plugin list`
    [[ -z $plugins_list ]]
}

@test "[TAR] elasticsearch fails if java executable is not found" {
  local JAVA=$(which java)

  sudo chmod -x $JAVA
  run "$ESHOME/bin/elasticsearch"
  sudo chmod +x $JAVA

  [ "$status" -eq 1 ]
  local expected="could not find java; set JAVA_HOME or ensure java is in PATH"
  [[ "$output" == *"$expected"* ]] || {
    echo "Expected error message [$expected] but found: $output"
    false
  }
}

##################################
# Check that Elasticsearch is working
##################################
@test "[TAR] test elasticsearch" {
    start_elasticsearch_service
    run_elasticsearch_tests
    stop_elasticsearch_service
}

@test "[TAR] start Elasticsearch with custom JVM options" {
    local es_java_opts=$ES_JAVA_OPTS
    local es_path_conf=$ES_PATH_CONF
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
    export ES_PATH_CONF="$temp"
    export ES_JAVA_OPTS="-XX:-UseCompressedOops"
    start_elasticsearch_service
    curl -s -XGET localhost:9200/_nodes | fgrep '"heap_init_in_bytes":536870912'
    curl -s -XGET localhost:9200/_nodes | fgrep '"using_compressed_ordinary_object_pointers":"false"'
    stop_elasticsearch_service
    export ES_PATH_CONF=$es_path_conf
    export ES_JAVA_OPTS=$es_java_opts
}

@test "[TAR] remove tar" {
    rm -rf "/tmp/elasticsearch"
}
