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
}

##################################
# Install plugins with a tar archive
##################################
@test "[TAR] install jvm-example plugin" {
    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Checks that plugin archive is available
    [ -e "$EXAMPLE_PLUGIN_ZIP" ]

    # Install jvm-example
    run /tmp/elasticsearch/bin/plugin install jvm-example -u "file://$EXAMPLE_PLUGIN_ZIP"
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example/test"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example/example.yaml"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example/plugin-descriptor.properties"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example/jvm-example-"*".jar"
    echo "Running jvm-example's bin script...."
    /tmp/elasticsearch/bin/jvm-example/test | grep test

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin remove jvm-example
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example/example.yaml"
    assert_file_not_exist "/tmp/elasticsearch/plugins/jvm-example"
}

@test "[TAR] install jvm-example plugin with a custom path.plugins" {
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
    [ -e "$EXAMPLE_PLUGIN_ZIP" ]

    # Install jvm-example
    run /tmp/elasticsearch/bin/plugin install jvm-example -u "file://$EXAMPLE_PLUGIN_ZIP"
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example/test"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example/example.yaml"
    assert_file_exist "$TEMP_PLUGINS_DIR/jvm-example"
    assert_file_exist "$TEMP_PLUGINS_DIR/jvm-example/plugin-descriptor.properties"
    assert_file_exist "$TEMP_PLUGINS_DIR/jvm-example/jvm-example-"*".jar"

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin remove jvm-example
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example/example.yaml"
    assert_file_not_exist "$TEMP_PLUGINS_DIR/jvm-example"

    # Delete the custom plugins directory
    run rm -rf "$TEMP_PLUGINS_DIR"
    [ "$status" -eq 0 ]
}

@test "[TAR] install jvm-example plugin with a custom CONFIG_DIR" {
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
    [ -e "$EXAMPLE_PLUGIN_ZIP" ]

    # Install jvm-example with the CONF_DIR environment variable
    run env "CONF_DIR=$TEMP_CONFIG_DIR" /tmp/elasticsearch/bin/plugin install jvm-example -u "file://$EXAMPLE_PLUGIN_ZIP"
    [ "$status" -eq 0 ]

    # Checks that jvm-example is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example/test"
    assert_file_exist "$TEMP_CONFIG_DIR/jvm-example"
    assert_file_exist "$TEMP_CONFIG_DIR/jvm-example/example.yaml"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example/plugin-descriptor.properties"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example/jvm-example-"*".jar"

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin remove jvm-example
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "$TEMP_CONFIG_DIR/jvm-example"
    assert_file_exist "$TEMP_CONFIG_DIR/jvm-example/example.yaml"
    assert_file_not_exist "/tmp/elasticsearch/plugins/jvm-example"

    # Delete the custom plugins directory
    run rm -rf "$TEMP_CONFIG_DIR"
    [ "$status" -eq 0 ]
}

@test "[TAR] install jvm-example plugin with a custom ES_JAVA_OPTS" {
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
    [ -e "$EXAMPLE_PLUGIN_ZIP" ]

    # Install jvm-example
    run /tmp/elasticsearch/bin/plugin install jvm-example -u "file://$EXAMPLE_PLUGIN_ZIP"
    [ "$status" -eq 0 ]

    # Checks that jvm-example is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example/test"
    assert_file_exist "$TEMP_CONFIG_DIR/jvm-example"
    assert_file_exist "$TEMP_CONFIG_DIR/jvm-example/example.yaml"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example/plugin-descriptor.properties"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example/jvm-example-"*".jar"

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin remove jvm-example
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "$TEMP_CONFIG_DIR/jvm-example"
    assert_file_exist "$TEMP_CONFIG_DIR/jvm-example/example.yaml"
    assert_file_not_exist "/tmp/elasticsearch/plugins/jvm-example"

    # Delete the custom plugins directory
    run rm -rf "$TEMP_CONFIG_DIR"
    [ "$status" -eq 0 ]
}

@test "[TAR] install jvm-example plugin to elasticsearch directory with a space" {
    export ES_DIR="/tmp/elastic search"

    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Move the Elasticsearch installation to a directory with a space in it
    rm -rf "$ES_DIR"
    mv /tmp/elasticsearch "$ES_DIR"

    # Checks that plugin archive is available
    [ -e "$EXAMPLE_PLUGIN_ZIP" ]

    # Install jvm-example
    run "$ES_DIR/bin/plugin" install jvm-example -u "file://$EXAMPLE_PLUGIN_ZIP"
    [ "$status" -eq 0 ]

    # Checks that jvm-example is correctly installed
    assert_file_exist "$ES_DIR/bin/jvm-example"
    assert_file_exist "$ES_DIR/bin/jvm-example/test"
    assert_file_exist "$ES_DIR/config/jvm-example"
    assert_file_exist "$ES_DIR/config/jvm-example/example.yaml"
    assert_file_exist "$ES_DIR/plugins/jvm-example"
    assert_file_exist "$ES_DIR/plugins/jvm-example/plugin-descriptor.properties"
    assert_file_exist "$ES_DIR/plugins/jvm-example/jvm-example-"*".jar"

    # Remove the plugin
    run "$ES_DIR/bin/plugin" remove jvm-example
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "$ES_DIR/bin/jvm-example"
    assert_file_exist "$ES_DIR/config/jvm-example"
    assert_file_exist "$ES_DIR/config/jvm-example/example.yaml"
    assert_file_not_exist "$ES_DIR/plugins/jvm-example"

    #Cleanup our temporary Elasticsearch installation
    rm -rf "$ES_DIR"
}

@test "[TAR] install jvm-example plugin from a directory with a space" {
    export EXAMPLE_PLUGIN_ZIP_WITH_SPACE="/tmp/plugins with space/jvm-example.zip"

    # Install the archive
    install_archive

    # Checks that the archive is correctly installed
    verify_archive_installation

    # Checks that plugin archive is available
    [ -e "$EXAMPLE_PLUGIN_ZIP" ]

    # Copy the jvm-example plugin to a directory with a space in it
    rm -f "$EXAMPLE_PLUGIN_ZIP_WITH_SPACE"
    mkdir -p "$(dirname "$EXAMPLE_PLUGIN_ZIP_WITH_SPACE")"
    cp $EXAMPLE_PLUGIN_ZIP "$EXAMPLE_PLUGIN_ZIP_WITH_SPACE"

    # Install jvm-example
    run /tmp/elasticsearch/bin/plugin install jvm-example -u "file://$EXAMPLE_PLUGIN_ZIP_WITH_SPACE"
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly installed
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "/tmp/elasticsearch/bin/jvm-example/test"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example/example.yaml"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example/plugin-descriptor.properties"
    assert_file_exist "/tmp/elasticsearch/plugins/jvm-example/jvm-example-"*".jar"

    # Remove the plugin
    run /tmp/elasticsearch/bin/plugin remove jvm-example
    [ "$status" -eq 0 ]

    # Checks that the plugin is correctly removed
    assert_file_not_exist "/tmp/elasticsearch/bin/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example"
    assert_file_exist "/tmp/elasticsearch/config/jvm-example/example.yaml"
    assert_file_not_exist "/tmp/elasticsearch/plugins/jvm-example"

    #Cleanup our plugin directory with a space
    rm -rf "$EXAMPLE_PLUGIN_ZIP_WITH_SPACE"
}
