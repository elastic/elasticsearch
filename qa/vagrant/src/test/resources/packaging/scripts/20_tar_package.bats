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
load plugins

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

@test "[TAR] elasticsearch fails if java executable is not found" {
  local JAVA=$(which java)

  sudo chmod -x $JAVA
  run "$ESHOME/bin/elasticsearch"
  sudo chmod +x $JAVA

  [ "$status" -eq 1 ]
  [[ "$output" == *"Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME"* ]]
}

##################################
# Check that Elasticsearch is working
##################################
@test "[TAR] test elasticsearch" {
    # Install scripts used to test script filters and search templates before
    # starting Elasticsearch so we don't have to wait for elasticsearch to scan for
    # them.
    install_elasticsearch_test_scripts
    start_elasticsearch_service
    run_elasticsearch_tests
    stop_elasticsearch_service
}

@test "[TAR]" start Elasticsearch with custom JVM options {
    local es_java_opts=$ES_JAVA_OPTS
    local es_jvm_options=$ES_JVM_OPTIONS
    local temp=`mktemp -d`
    touch "$temp/jvm.options"
    chown -R elasticsearch:elasticsearch "$temp"
    echo "-Xms264m" >> "$temp/jvm.options"
    echo "-Xmx264m" >> "$temp/jvm.options"
    export ES_JVM_OPTIONS="$temp/jvm.options"
    export ES_JAVA_OPTS="-XX:-UseCompressedOops"
    start_elasticsearch_service
    curl -s -XGET localhost:9200/_nodes | fgrep '"heap_init_in_bytes":276824064'
    curl -s -XGET localhost:9200/_nodes | fgrep '"using_compressed_ordinary_object_pointers":"false"'
    stop_elasticsearch_service
    export ES_JVM_OPTIONS=$es_jvm_options
    export ES_JAVA_OPTS=$es_java_opts
}

@test "[TAR]" start Elasticsearch with unquoted JSON option {
    local es_java_opts=$ES_JAVA_OPTS
    local es_jvm_options=$ES_JVM_OPTIONS
    local temp=`mktemp -d`
    touch "$temp/jvm.options"
    chown -R elasticsearch:elasticsearch "$temp"
    echo "-Delasticsearch.json.allow_unquoted_field_names=true" >> "$temp/jvm.options"
    export ES_JVM_OPTIONS="$temp/jvm.options"
    start_elasticsearch_service
    # unquoted field name
    curl -s -XPOST localhost:9200/i/d/1 -d'{foo: "bar"}'
    [ "$?" -eq 0 ]
    curl -s -XDELETE localhost:9200/i
    stop_elasticsearch_service
    export ES_JVM_OPTIONS=$es_jvm_options
    export ES_JAVA_OPTS=$es_java_opts
}

@test "[TAR]" remove tar {
    rm -rf "/tmp/elasticsearch"
}
