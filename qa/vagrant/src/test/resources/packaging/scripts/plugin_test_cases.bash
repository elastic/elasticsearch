#!/usr/bin/env bats

# This file is used to test the installation and removal
# of plugins after Elasticsearch has been installed with tar.gz,
# rpm, and deb.

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

##################################
# Common test cases for both tar and rpm/deb based plugin tests
##################################
# This file is symlinked to both 25_tar_plugins.bats and 50_plugins.bats so its
# executed twice - once to test plugins using the tar distribution and once to
# test files using the rpm distribution or the deb distribution, whichever the
# system uses.

# Load test utilities
load packaging_test_utils
load plugins

setup() {
    # The rules on when we should clean an reinstall are complex - all the
    # jvm-example tests need cleaning because they are rough on the filesystem.
    # The first and any tests that find themselves without an ESHOME need to
    # clean as well.... this is going to mostly only happen on the first
    # non-jvm-example-plugin-test _and_ any first test if you comment out the
    # other tests. Commenting out lots of test cases seems like a reasonably
    # common workflow.
    if [ $BATS_TEST_NUMBER == 1 ] ||
            [[ $BATS_TEST_NAME =~ install_jvm.*example ]] ||
            [ ! -d "$ESHOME" ]; then
        clean_before_test
        install
    fi
}

if [[ "$BATS_TEST_FILENAME" =~ 25_tar_plugins.bats$ ]]; then
    load tar
    GROUP='TAR PLUGINS'
    install() {
        install_archive
        verify_archive_installation
    }
    export ESHOME=/tmp/elasticsearch
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=elasticsearch
else
    load os_package
    if is_rpm; then
        GROUP='RPM PLUGINS'
    elif is_dpkg; then
        GROUP='DEB PLUGINS'
    fi
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=root
    install() {
        install_package
        verify_package_installation
    }
fi

@test "[$GROUP] install jvm-example plugin with a custom path.plugins" {
    # Clean up after the last time this test was run
    rm -rf /tmp/plugins.*

    local oldPlugins="$ESPLUGINS"
    export ESPLUGINS=$(mktemp -d -t 'plugins.XXXX')

    # Modify the path.plugins setting in configuration file
    echo "path.plugins: $ESPLUGINS" >> "$ESCONFIG/elasticsearch.yml"
    chown -R elasticsearch:elasticsearch "$ESPLUGINS"

    install_jvm_example
    start_elasticsearch_service
    # check that configuration was actually picked up    
    curl -s localhost:9200/_cat/configured_example | sed 's/ *$//' > /tmp/installed
    echo "foo" > /tmp/expected
    diff /tmp/installed /tmp/expected
    stop_elasticsearch_service
    remove_jvm_example
}

@test "[$GROUP] install jvm-example plugin with a custom CONFIG_DIR" {
    # Clean up after the last time we ran this test
    rm -rf /tmp/config.*

    move_config

    CONF_DIR="$ESCONFIG" install_jvm_example
    CONF_DIR="$ESCONFIG" remove_jvm_example
}

@test "[$GROUP] install jvm-example plugin from a directory with a space" {
    rm -rf "/tmp/plugins with space"
    mkdir -p "/tmp/plugins with space"
    local zip=$(ls jvm-example-*.zip)
    cp $zip "/tmp/plugins with space"

    install_jvm_example "/tmp/plugins with space/$zip"
    remove_jvm_example
}

@test "[$GROUP] install jvm-example plugin to elasticsearch directory with a space" {
    [ "$GROUP" == "TAR PLUGINS" ] || skip "Test case only supported by TAR PLUGINS"

    move_elasticsearch "/tmp/elastic search"

    install_jvm_example
    remove_jvm_example
}

@test "[$GROUP] fail if java executable is not found" {
  [ "$GROUP" == "TAR PLUGINS" ] || skip "Test case only supported by TAR PLUGINS"
  local JAVA=$(which java)

  sudo chmod -x $JAVA
  run "$ESHOME/bin/plugin"
  sudo chmod +x $JAVA

  [ "$status" -eq 1 ]
  [[ "$output" == *"Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME"* ]]
}

# Note that all of the tests from here to the end of the file expect to be run
# in sequence and don't take well to being run one at a time.
@test "[$GROUP] install jvm-example plugin" {
    install_jvm_example
}

@test "[$GROUP] install icu plugin" {
    install_and_check_plugin analysis icu icu4j-*.jar
}

@test "[$GROUP] install kuromoji plugin" {
    install_and_check_plugin analysis kuromoji
}

@test "[$GROUP] install phonetic plugin" {
    install_and_check_plugin analysis phonetic commons-codec-*.jar
}

@test "[$GROUP] install smartcn plugin" {
    install_and_check_plugin analysis smartcn
}

@test "[$GROUP] install stempel plugin" {
    install_and_check_plugin analysis stempel
}

@test "[$GROUP] install aws plugin" {
    install_and_check_plugin cloud aws aws-java-sdk-core-*.jar
}

@test "[$GROUP] install azure plugin" {
    install_and_check_plugin cloud azure azure-core-*.jar
}

@test "[$GROUP] install gce plugin" {
    install_and_check_plugin cloud gce google-api-client-*.jar
}

@test "[$GROUP] install delete by query" {
    install_and_check_plugin - delete-by-query
}

@test "[$GROUP] install multicast discovery plugin" {
    install_and_check_plugin discovery multicast
}

@test "[$GROUP] install javascript plugin" {
    install_and_check_plugin lang javascript rhino-*.jar
}

@test "[$GROUP] install python plugin" {
    install_and_check_plugin lang python jython-standalone-*.jar
}

@test "[$GROUP] install murmur3 mapper" {
    install_and_check_plugin mapper murmur3
}

@test "[$GROUP] install size mapper" {
    install_and_check_plugin mapper size
}

@test "[$GROUP] install site example" {
    # Doesn't use install_and_check_plugin because this is a site plugin
    install_plugin site-example $(readlink -m site-example-*.zip)
    assert_file_exist "$ESHOME/plugins/site-example/_site/index.html"
}

@test "[$GROUP] check the installed plugins can be listed with 'plugins list' and result matches the list of plugins in plugins pom" {
    "$ESHOME/bin/plugin" list | tail -n +2 | sed 's/^......//' > /tmp/installed
    compare_plugins_list "/tmp/installed" "'plugins list'"
}

@test "[$GROUP] start elasticsearch with all plugins installed" {
    start_elasticsearch_service
}

@test "[$GROUP] check the installed plugins matches the list of build plugins" {
    curl -s localhost:9200/_cat/plugins?h=c | sed 's/ *$//' > /tmp/installed
    compare_plugins_list "/tmp/installed" "_cat/plugins"
}

@test "[$GROUP] stop elasticsearch" {
    stop_elasticsearch_service
}

@test "[$GROUP] remove jvm-example plugin" {
    remove_jvm_example
}

@test "[$GROUP] remove icu plugin" {
    remove_plugin analysis-icu
}

@test "[$GROUP] remove kuromoji plugin" {
    remove_plugin analysis-kuromoji
}

@test "[$GROUP] remove phonetic plugin" {
    remove_plugin analysis-phonetic
}

@test "[$GROUP] remove smartcn plugin" {
    remove_plugin analysis-smartcn
}

@test "[$GROUP] remove stempel plugin" {
    remove_plugin analysis-stempel
}

@test "[$GROUP] remove aws plugin" {
    remove_plugin cloud-aws
}

@test "[$GROUP] remove azure plugin" {
    remove_plugin cloud-azure
}

@test "[$GROUP] remove gce plugin" {
    remove_plugin cloud-gce
}

@test "[$GROUP] remove delete by query" {
    remove_plugin delete-by-query
}

@test "[$GROUP] remove multicast discovery plugin" {
    remove_plugin discovery-multicast
}

@test "[$GROUP] remove javascript plugin" {
    remove_plugin lang-javascript
}

@test "[$GROUP] remove python plugin" {
    remove_plugin lang-python
}

@test "[$GROUP] remove murmur3 mapper" {
    remove_plugin mapper-murmur3
}

@test "[$GROUP] remove size mapper" {
    remove_plugin mapper-size
}

@test "[$GROUP] remove site example" {
    remove_plugin site-example
}

@test "[$GROUP] start elasticsearch with all plugins removed" {
    start_elasticsearch_service
}

@test "[$GROUP] check that there are now no plugins installed" {
    curl -s localhost:9200/_cat/plugins > /tmp/installed
    local installedCount=$(cat /tmp/installed | wc -l)
    [ "$installedCount" == "0" ] || {
        echo "Expected all plugins to be removed but found $installedCount:"
        cat /tmp/installed
        false
    }
}

@test "[$GROUP] stop elasticsearch" {
    stop_elasticsearch_service
}
