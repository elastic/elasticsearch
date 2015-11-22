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

@test "[$GROUP] install jvm-example plugin with a custom CONFIG_FILE and check failure" {
    local relativePath=${1:-$(readlink -m jvm-example-*.zip)}
    CONF_FILE="$ESCONFIG/elasticsearch.yml" run sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/plugin" install "file://$relativePath"
    # this should fail because CONF_FILE is no longer supported
    [ $status = 1 ]
    CONF_FILE="$ESCONFIG/elasticsearch.yml" run sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/plugin" remove jvm-example
    echo "status is $status"
    [ $status = 1 ]
}

@test "[$GROUP] start elasticsearch with a custom CONFIG_FILE and check failure" {
    local CONF_FILE="$ESCONFIG/elasticsearch.yml"

    if is_dpkg; then
        echo "CONF_FILE=$CONF_FILE" >> /etc/default/elasticsearch;
    elif is_rpm; then
        echo "CONF_FILE=$CONF_FILE" >> /etc/sysconfig/elasticsearch;
    fi

    run_elasticsearch_service 1 -Des.default.config="$CONF_FILE"

    # remove settings again otherwise cleaning up before next testrun will fail
    if is_dpkg ; then
        sudo sed -i '/CONF_FILE/d' /etc/default/elasticsearch
    elif is_rpm; then
        sudo sed -i '/CONF_FILE/d' /etc/sysconfig/elasticsearch
    fi
}

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
    CONF_DIR="$ESCONFIG" start_elasticsearch_service
    diff  <(curl -s localhost:9200/_cat/configured_example | sed 's/ //g') <(echo "foo")
    stop_elasticsearch_service
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

@test "[$GROUP] install gce plugin" {
    install_and_check_plugin discovery gce google-api-client-*.jar
}

@test "[$GROUP] install delete by query plugin" {
    install_and_check_plugin - delete-by-query
}

@test "[$GROUP] install discovery-azure plugin" {
    install_and_check_plugin discovery azure azure-core-*.jar
}

@test "[$GROUP] install discovery-ec2 plugin" {
    install_and_check_plugin discovery ec2 aws-java-sdk-core-*.jar
}

@test "[$GROUP] install multicast discovery plugin" {
    install_and_check_plugin discovery multicast
}

@test "[$GROUP] install lang-expression plugin" {
    install_and_check_plugin lang expression
}

@test "[$GROUP] install lang-groovy plugin" {
    install_and_check_plugin lang groovy
}

@test "[$GROUP] install javascript plugin" {
    install_and_check_plugin lang javascript rhino-*.jar
}

@test "[$GROUP] install python plugin" {
    install_and_check_plugin lang python jython-standalone-*.jar
}

@test "[$GROUP] install mapper-attachments plugin" {
    install_and_check_plugin mapper attachments
}

@test "[$GROUP] install murmur3 mapper plugin" {
    install_and_check_plugin mapper murmur3
}

@test "[$GROUP] install size mapper plugin" {
    install_and_check_plugin mapper size
}

@test "[$GROUP] install repository-azure plugin" {
    install_and_check_plugin repository azure azure-storage-*.jar
}

@test "[$GROUP] install repository-s3 plugin" {
    install_and_check_plugin repository s3 aws-java-sdk-core-*.jar
}

@test "[$GROUP] install site example" {
    # Doesn't use install_and_check_plugin because this is a site plugin
    install_plugin site-example $(readlink -m site-example-*.zip)
    assert_file_exist "$ESHOME/plugins/site-example/_site/index.html"
}

@test "[$GROUP] install store-smb plugin" {
    install_and_check_plugin store smb
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

@test "[$GROUP] remove gce plugin" {
    remove_plugin discovery-gce
}

@test "[$GROUP] remove delete by query plugin" {
    remove_plugin delete-by-query
}

@test "[$GROUP] remove discovery-azure plugin" {
    remove_plugin discovery-azure
}

@test "[$GROUP] remove discovery-ec2 plugin" {
    remove_plugin discovery-ec2
}

@test "[$GROUP] remove multicast discovery plugin" {
    remove_plugin discovery-multicast
}

@test "[$GROUP] remove lang-expression plugin" {
    remove_plugin lang-expression
}

@test "[$GROUP] remove lang-groovy plugin" {
    remove_plugin lang-groovy
}

@test "[$GROUP] remove javascript plugin" {
    remove_plugin lang-javascript
}

@test "[$GROUP] remove python plugin" {
    remove_plugin lang-python
}

@test "[$GROUP] remove mapper-attachments plugin" {
    remove_plugin mapper-attachments
}

@test "[$GROUP] remove murmur3 mapper plugin" {
    remove_plugin mapper-murmur3
}

@test "[$GROUP] remove size mapper plugin" {
    remove_plugin mapper-size
}

@test "[$GROUP] remove repository-azure plugin" {
    remove_plugin repository-azure
}

@test "[$GROUP] remove repository-s3 plugin" {
    remove_plugin repository-s3
}

@test "[$GROUP] remove site example plugin" {
    remove_plugin site-example
}

@test "[$GROUP] remove store-smb plugin" {
    remove_plugin store-smb
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

@test "[$GROUP] install jvm-example with different logging modes and check output" {
    local relativePath=${1:-$(readlink -m jvm-example-*.zip)}
    sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/plugin" install "file://$relativePath" > /tmp/plugin-cli-output
    local loglines=$(cat /tmp/plugin-cli-output | wc -l)
    if [ "$GROUP" == "TAR PLUGINS" ]; then
    # tar extraction does not create the plugins directory so the plugin tool will print an additional line that the directory will be created
        [ "$loglines" -eq "7" ] || {
            echo "Expected 7 lines but the output was:"
            cat /tmp/plugin-cli-output
            false
        }
    else
        [ "$loglines" -eq "6" ] || {
            echo "Expected 6 lines but the output was:"
            cat /tmp/plugin-cli-output
            false
        }
    fi
    remove_jvm_example

    local relativePath=${1:-$(readlink -m jvm-example-*.zip)}
    sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/plugin" install "file://$relativePath" -Des.logger.level=DEBUG > /tmp/plugin-cli-output
    local loglines=$(cat /tmp/plugin-cli-output | wc -l)
    if [ "$GROUP" == "TAR PLUGINS" ]; then
        [ "$loglines" -gt "7" ] || {
            echo "Expected more than 7 lines but the output was:"
            cat /tmp/plugin-cli-output
            false
        }
    else
        [ "$loglines" -gt "6" ] || {
            echo "Expected more than 6 lines but the output was:"
            cat /tmp/plugin-cli-output
            false
        }
    fi
    remove_jvm_example
}
