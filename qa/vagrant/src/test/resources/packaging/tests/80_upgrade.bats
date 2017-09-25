#!/usr/bin/env bats

# Tests upgrading elasticsearch from a previous version with the deb or rpm
# packages. Just uses a single node cluster on the current machine rather than
# fancy rolling restarts.

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

# Cleans everything for the 1st execution
setup() {
    skip_not_dpkg_or_rpm

    sameVersion="false"
    if [ "$(cat upgrade_from_version)" == "$(cat version)" ]; then
        sameVersion="true"
    fi
}

@test "[UPGRADE] install old version" {
    clean_before_test
    install_package -v $(cat upgrade_from_version)
}

@test "[UPGRADE] start old version" {
    start_elasticsearch_service
}

@test "[UPGRADE] check elasticsearch version is old version" {
    check_elasticsearch_version "$(cat upgrade_from_version)"
}

@test "[UPGRADE] index some documents into a few indexes" {
    curl -s -H "Content-Type: application/json" -XPOST localhost:9200/library/book/1?pretty -d '{
      "title": "Elasticsearch - The Definitive Guide"
    }'
    curl -s -H "Content-Type: application/json" -XPOST localhost:9200/library/book/2?pretty -d '{
      "title": "Brave New World"
    }'
    curl -s -H "Content-Type: application/json" -XPOST localhost:9200/library2/book/1?pretty -d '{
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
    if [ "$sameVersion" == "true" ]; then
        install_package -f
    else
        install_package -u
    fi
}

@test "[UPGRADE] start version under test" {
    start_elasticsearch_service yellow library
    wait_for_elasticsearch_status yellow library2
}

@test "[UPGRADE] check elasticsearch version is version under test" {
    check_elasticsearch_version "$(cat version)"
}

@test "[UPGRADE] verify that the documents are there after restart" {
    curl -s localhost:9200/library/book/1?pretty | grep Elasticsearch
    curl -s localhost:9200/library/book/2?pretty | grep World
    curl -s localhost:9200/library2/book/1?pretty | grep Darkness
}

@test "[UPGRADE] stop version under test" {
    stop_elasticsearch_service
}
