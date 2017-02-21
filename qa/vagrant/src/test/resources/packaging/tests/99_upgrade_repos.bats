#!/usr/bin/env bats

# Tests upgrading elasticsearch from a previous version with the deb or rpm
# packages. Just uses a single node cluster on the current machine rather than
# fancy rolling restarts.

# WARNING: This testing file must be executed as root and can
# dramatically change your system. It removes the 'elasticsearch'
# user/group and also many directories. Do not execute this file
# unless you know exactly what you are doing.

# NOTE: This test does not need any locally compiled version of elasticsearch
# Instead this test will use the latest stable version, and one version
# from a remote package repository that needs to be specified

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

# This is a special integration test, that ensures that our internal
# release candidates can be successfully installed

# Cleans everything for the 1st execution
setup() {
    skip_not_dpkg_or_rpm
    if [ ! -e repo_expected_version ] ; then
        skip
    fi

    if [ ! -e repo_url ] ; then
        skip
    fi

    if [ "$(cat upgrade_from_version)" == "$(cat repo_expected_version)" ]; then
        skip
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

@test "[UPGRADE] install version from repo (apt)" {
    skip_not_dpkg
    wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
    echo "deb $(cat repo_url)/apt stable main" >> /etc/apt/sources.list.d/elastic-5.x.list
    # needed for debian, but not for ubuntu
    apt-get install apt-transport-https || true
    apt-get -y update
    # this will upgrade only ES and prevent download of other packages
    apt-get -y install elasticsearch
}

@test "[UPGRADE] install version from repo (rpm)" {
    skip_not_rpm

    rpm --import https://artifacts.elastic.co/GPG-KEY-elasticsearch

    if [ -d "/etc/yum.repos.d/"  ] ; then
        file="/etc/yum.repos.d/elasticsearch.repo"
    else
        file="/etc/zypp/repos.d/elasticsearch.repo"
    fi

    cat > $file <<EOF
[elasticsearch-5.x]
name=Elasticsearch repository for 5.x packages
baseurl=$(cat repo_url)/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=1
autorefresh=1
type=rpm-md
EOF

    cat $file

    # one of those commands should upgrade ES, so lets try all three for simplicity
    yum -y install elasticsearch || true 
    dnf -y upgrade elasticsearch || true
    zypper --non-interactive install elasticsearch || true
}

@test "[UPGRADE] start version from repository" {
    start_elasticsearch_service yellow library
    wait_for_elasticsearch_status yellow library2
}

@test "[UPGRADE] check elasticsearch version is expected version from repository: $(cat repo_expected_version)" {
    check_elasticsearch_version "$(cat repo_expected_version)"
}

@test "[UPGRADE] verify that the documents are there after restart" {
    curl -s localhost:9200/library/book/1?pretty | grep Elasticsearch
    curl -s localhost:9200/library/book/2?pretty | grep World
    curl -s localhost:9200/library2/book/1?pretty | grep Darkness
}

@test "[UPGRADE] stop version from repository" {
    stop_elasticsearch_service
}
