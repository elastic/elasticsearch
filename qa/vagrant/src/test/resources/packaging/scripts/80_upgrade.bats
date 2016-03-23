#!/usr/bin/env bats

# Tests upgrading elasticsearch from a previous version with the deb or rpm
# packages. Just uses a single node cluster on the current machine rather than
# fancy rolling restarts.

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

# Cleans everything for the 1st execution
setup() {
    skip_not_dpkg_or_rpm
}

@test "[UPGRADE] install old version" {
    clean_before_test
    install_package -v $(cat upgrade_from_version)
}

@test "[UPGRADE] start old version" {
    start_elasticsearch_service
}

@test "[UPGRADE] check elasticsearch version is old version" {
    curl -s localhost:9200 | grep \"number\"\ :\ \"$(cat upgrade_from_version)\" || {
        echo "Installed an unexpected version:"
        curl -s localhost:9200
        false
    }
}

@test "[UPGRADE] index some documents into a few indexes" {
    curl -s -XPOST localhost:9200/library/book/1?pretty -d '{
      "title": "Elasticsearch - The Definitive Guide"
    }'
    curl -s -XPOST localhost:9200/library/book/2?pretty -d '{
      "title": "Brave New World"
    }'
    curl -s -XPOST localhost:9200/library2/book/1?pretty -d '{
      "title": "The Left Hand of Darkness"
    }'
}

@test "[UPGRADE] verify that the documents are there" {
    curl -s localhost:9200/library/book/1?pretty | grep Elasticsearch
    curl -s localhost:9200/library/book/2?pretty | grep World
    curl -s localhost:9200/library2/book/1?pretty | grep Darkness
}

@test "[UPGRADE] stop old version" {
    stop_elasticsearch_service
}

@test "[UPGRADE] install version under test" {
    install_package -u
}

@test "[UPGRADE] start version under test" {
    start_elasticsearch_service yellow library
    wait_for_elasticsearch_status yellow library2
}

@test "[UPGRADE] check elasticsearch version is version under test" {
    local versionToCheck=$(cat version | sed -e 's/-SNAPSHOT//')
    curl -s localhost:9200 | grep \"number\"\ :\ \"$versionToCheck\" || {
        echo "Installed an unexpected version:"
        curl -s localhost:9200
        false
    }
}

@test "[UPGRADE] verify that the documents are there after restart" {
    curl -s localhost:9200/library/book/1?pretty | grep Elasticsearch
    curl -s localhost:9200/library/book/2?pretty | grep World
    curl -s localhost:9200/library2/book/1?pretty | grep Darkness
}

@test "[UPGRADE] stop version under test" {
    stop_elasticsearch_service
}
