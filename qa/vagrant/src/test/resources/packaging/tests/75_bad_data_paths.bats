#!/usr/bin/env bats

# Tests data.path settings which in the past have misbehaving, leaking the
# default.data.path setting into the data.path even when it doesn't belong.

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
load $BATS_UTILS/packages.bash
load $BATS_UTILS/tar.bash
load $BATS_UTILS/utils.bash

@test "[BAD data.path] install package" {
    clean_before_test
    skip_not_dpkg_or_rpm
    install_package
}

@test "[BAD data.path] setup funny path.data in package install" {
    skip_not_dpkg_or_rpm
    local temp=`mktemp -d`
    chown elasticsearch:elasticsearch "$temp"
    echo "path.data: [$temp]" > "/etc/elasticsearch/elasticsearch.yml"
}

@test "[BAD data.path] start installed from package" {
    skip_not_dpkg_or_rpm
    start_elasticsearch_service green
}

@test "[BAD data.path] check for bad dir after starting from package" {
    skip_not_dpkg_or_rpm
    assert_file_not_exist /var/lib/elasticsearch/nodes
}

@test "[BAD data.path] install tar" {
    clean_before_test
    install_archive
}

@test "[BAD data.path] setup funny path.data in tar install" {
    local temp=`mktemp -d`
    chown elasticsearch:elasticsearch "$temp"
    echo "path.data: [$temp]" > "/tmp/elasticsearch/config/elasticsearch.yml"
}

@test "[BAD data.path] start installed from tar" {
    start_elasticsearch_service green "" "-Edefault.path.data=/tmp/elasticsearch/data"
}

@test "[BAD data.path] check for bad dir after starting from tar" {
    assert_file_not_exist "/tmp/elasticsearch/data/nodes"
}
