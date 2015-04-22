#!/usr/bin/env bats

# This file is used to test the installation and removal
# of plugins with a tar gz archive.

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

setup() {
    # Cleans everything for every test execution
    clean_before_test

    # Download Marvel and Shield
    MARVEL_ZIP="$PWD/marvel.zip"
    SHIELD_ZIP="$PWD/shield.zip"

    if [ "$BATS_TEST_NUMBER" -eq 1 ]; then
        if [ ! -e "$MARVEL_ZIP" ]; then
            wget --quiet -O "$MARVEL_ZIP" "http://download.elasticsearch.org/elasticsearch/marvel/marvel-latest.zip"
        fi
        if [ ! -e "$SHIELD_ZIP" ]; then
            wget --quiet -O "$SHIELD_ZIP" "http://download.elasticsearch.org/elasticsearch/shield/shield-latest.zip"
        fi
    fi
}

##################################
# Install plugins with a tar archive
##################################
@test "[TAR] install marvel plugin" {

    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Checks that plugin archive is available
    [ -e "$MARVEL_ZIP" ]

    # Install Marvel
    run /tmp/elasticsearch/bin/plugin -i elasticsearch/marvel/latest -u "file://$MARVEL_ZIP"
    [ "$status" -eq 0 ]

    # Checks that Marvel is correctly installed
    assert_file_exist "/tmp/elasticsearch/plugins/marvel"

    start_elasticsearch_service

    run curl -XGET 'http://localhost:9200/_cat/plugins?v=false&h=component'
    [ "$status" -eq 0 ]
    echo "$output" | grep -w "marvel"

    stop_elasticsearch_service

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin -r elasticsearch/marvel/latest
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/plugins/marvel"
}

@test "[TAR] install marvel plugin with a custom path.plugins" {

    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Creates a temporary directory
    TEMP_PLUGINS_DIR=`mktemp -d 2>/dev/null || mktemp -d -t 'tmp'`

    # Modify the path.plugins setting in configuration file
    echo "path.plugins: $TEMP_PLUGINS_DIR" >> "/tmp/elasticsearch/config/elasticsearch.yml"

    run chown -R elasticsearch:elasticsearch "$TEMP_PLUGINS_DIR"
    [ "$status" -eq 0 ]

    # Checks that plugin archive is available
    [ -e "$MARVEL_ZIP" ]

    # Install Marvel
    run /tmp/elasticsearch/bin/plugin -i elasticsearch/marvel/latest -u "file://$MARVEL_ZIP"
    [ "$status" -eq 0 ]

    # Checks that Marvel is correctly installed
    assert_file_exist "$TEMP_PLUGINS_DIR/marvel"

    start_elasticsearch_service

    run curl -XGET 'http://localhost:9200/_cat/plugins?v=false&h=component'
    [ "$status" -eq 0 ]
    echo "$output" | grep -w "marvel"

    stop_elasticsearch_service

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin -r elasticsearch/marvel/latest
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "$TEMP_PLUGINS_DIR/marvel"

    # Delete the custom plugins directory
    run rm -rf "$TEMP_PLUGINS_DIR"
    [ "$status" -eq 0 ]
}

@test "[TAR] install shield plugin" {

    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Checks that plugin archive is available
    [ -e "$SHIELD_ZIP" ]

    # Install Shield
    run /tmp/elasticsearch/bin/plugin -i elasticsearch/shield/latest -u "file://$SHIELD_ZIP"
    [ "$status" -eq 0 ]

    # Checks that Shield is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/shield"
    assert_file_exist "/tmp/elasticsearch/bin/shield/esusers"
    assert_file_exist "/tmp/elasticsearch/bin/shield/syskeygen"
    assert_file_exist "/tmp/elasticsearch/config/shield"
    assert_file_exist "/tmp/elasticsearch/config/shield/role_mapping.yml"
    assert_file_exist "/tmp/elasticsearch/config/shield/roles.yml"
    assert_file_exist "/tmp/elasticsearch/config/shield/users"
    assert_file_exist "/tmp/elasticsearch/config/shield/users_roles"
    assert_file_exist "/tmp/elasticsearch/plugins/shield"

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin -r elasticsearch/shield/latest
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/shield"
    assert_file_exist "/tmp/elasticsearch/config/shield"
    assert_file_exist "/tmp/elasticsearch/config/shield/role_mapping.yml"
    assert_file_exist "/tmp/elasticsearch/config/shield/roles.yml"
    assert_file_exist "/tmp/elasticsearch/config/shield/users"
    assert_file_exist "/tmp/elasticsearch/config/shield/users_roles"
    assert_file_not_exist "/tmp/elasticsearch/plugins/shield"
}

@test "[TAR] install shield plugin with a custom path.plugins" {

    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Creates a temporary directory
    TEMP_PLUGINS_DIR=`mktemp -d 2>/dev/null || mktemp -d -t 'tmp'`

    # Modify the path.plugins setting in configuration file
    echo "path.plugins: $TEMP_PLUGINS_DIR" >> "/tmp/elasticsearch/config/elasticsearch.yml"

    run chown -R elasticsearch:elasticsearch "$TEMP_PLUGINS_DIR"
    [ "$status" -eq 0 ]

    # Checks that plugin archive is available
    [ -e "$SHIELD_ZIP" ]

    # Install Shield
    run /tmp/elasticsearch/bin/plugin -i elasticsearch/shield/latest -u "file://$SHIELD_ZIP"
    [ "$status" -eq 0 ]

    # Checks that Shield is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/shield"
    assert_file_exist "/tmp/elasticsearch/bin/shield/esusers"
    assert_file_exist "/tmp/elasticsearch/bin/shield/syskeygen"
    assert_file_exist "/tmp/elasticsearch/config/shield"
    assert_file_exist "/tmp/elasticsearch/config/shield/role_mapping.yml"
    assert_file_exist "/tmp/elasticsearch/config/shield/roles.yml"
    assert_file_exist "/tmp/elasticsearch/config/shield/users"
    assert_file_exist "/tmp/elasticsearch/config/shield/users_roles"
    assert_file_exist "$TEMP_PLUGINS_DIR/shield"

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin -r elasticsearch/shield/latest
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/shield"
    assert_file_exist "/tmp/elasticsearch/config/shield"
    assert_file_exist "/tmp/elasticsearch/config/shield/role_mapping.yml"
    assert_file_exist "/tmp/elasticsearch/config/shield/roles.yml"
    assert_file_exist "/tmp/elasticsearch/config/shield/users"
    assert_file_exist "/tmp/elasticsearch/config/shield/users_roles"
    assert_file_not_exist "$TEMP_PLUGINS_DIR/shield"

    # Delete the custom plugins directory
    run rm -rf "$TEMP_PLUGINS_DIR"
    [ "$status" -eq 0 ]
}

@test "[TAR] install shield plugin with a custom CONFIG_DIR" {

    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Creates a temporary directory
    TEMP_CONFIG_DIR=`mktemp -d 2>/dev/null || mktemp -d -t 'tmp'`

    # Move configuration files to the new configuration directory
    run mv /tmp/elasticsearch/config/* $TEMP_CONFIG_DIR
    [ "$status" -eq 0 ]

    run chown -R elasticsearch:elasticsearch "$TEMP_CONFIG_DIR"
    [ "$status" -eq 0 ]

    assert_file_exist "$TEMP_CONFIG_DIR/elasticsearch.yml"

    # Checks that plugin archive is available
    [ -e "$SHIELD_ZIP" ]

    # Install Shield with the CONF_DIR environment variable
    run env "CONF_DIR=$TEMP_CONFIG_DIR" /tmp/elasticsearch/bin/plugin -i "elasticsearch/shield/latest" -u "file://$SHIELD_ZIP"
    [ "$status" -eq 0 ]

    # Checks that Shield is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/shield"
    assert_file_exist "/tmp/elasticsearch/bin/shield/esusers"
    assert_file_exist "/tmp/elasticsearch/bin/shield/syskeygen"
    assert_file_exist "$TEMP_CONFIG_DIR/shield"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/role_mapping.yml"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/roles.yml"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/users"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/users_roles"
    assert_file_exist "/tmp/elasticsearch/plugins/shield"

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin -r elasticsearch/shield/latest
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/shield"
    assert_file_exist "$TEMP_CONFIG_DIR/shield"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/role_mapping.yml"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/roles.yml"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/users"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/users_roles"
    assert_file_not_exist "/tmp/elasticsearch/plugins/shield"

    # Delete the custom plugins directory
    run rm -rf "$TEMP_CONFIG_DIR"
    [ "$status" -eq 0 ]
}

@test "[TAR] install shield plugin with a custom ES_JAVA_OPTS" {

    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Creates a temporary directory
    TEMP_CONFIG_DIR=`mktemp -d 2>/dev/null || mktemp -d -t 'tmp'`

    # Move configuration files to the new configuration directory
    run mv /tmp/elasticsearch/config/* $TEMP_CONFIG_DIR
    [ "$status" -eq 0 ]

    run chown -R elasticsearch:elasticsearch "$TEMP_CONFIG_DIR"
    [ "$status" -eq 0 ]

    assert_file_exist "$TEMP_CONFIG_DIR/elasticsearch.yml"

    # Export ES_JAVA_OPTS
    export ES_JAVA_OPTS="-Des.path.conf=$TEMP_CONFIG_DIR"
    [ "$status" -eq 0 ]

    # Checks that plugin archive is available
    [ -e "$SHIELD_ZIP" ]

    # Install Shield
    run /tmp/elasticsearch/bin/plugin -i elasticsearch/shield/latest -u "file://$SHIELD_ZIP"
    [ "$status" -eq 0 ]

    # Checks that Shield is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/shield"
    assert_file_exist "/tmp/elasticsearch/bin/shield/esusers"
    assert_file_exist "/tmp/elasticsearch/bin/shield/syskeygen"
    assert_file_exist "$TEMP_CONFIG_DIR/shield"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/role_mapping.yml"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/roles.yml"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/users"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/users_roles"
    assert_file_exist "/tmp/elasticsearch/plugins/shield"

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin -r elasticsearch/shield/latest
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/shield"
    assert_file_exist "$TEMP_CONFIG_DIR/shield"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/role_mapping.yml"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/roles.yml"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/users"
    assert_file_exist "$TEMP_CONFIG_DIR/shield/users_roles"
    assert_file_not_exist "/tmp/elasticsearch/plugins/shield"

    # Delete the custom plugins directory
    run rm -rf "$TEMP_CONFIG_DIR"
    [ "$status" -eq 0 ]
}
