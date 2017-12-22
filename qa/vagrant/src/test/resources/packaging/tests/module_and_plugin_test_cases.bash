#!/usr/bin/env bats

# This file is used to test the installation and removal
# of plugins after Elasticsearch has been installed with tar.gz,
# rpm, and deb.

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

##################################
# Common test cases for both tar and rpm/deb based plugin tests
##################################
# This file is symlinked to both 25_tar_plugins.bats and 50_modules_and_plugins.bats so its
# executed twice - once to test plugins using the tar distribution and once to
# test files using the rpm distribution or the deb distribution, whichever the
# system uses.

# Load test utilities
load $BATS_UTILS/utils.bash
load $BATS_UTILS/modules.bash
load $BATS_UTILS/plugins.bash

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
    load $BATS_UTILS/tar.bash
    GROUP='TAR PLUGINS'
    install() {
        install_archive
        verify_archive_installation
    }
    export ESHOME=/tmp/elasticsearch
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=elasticsearch
else
    load $BATS_UTILS/packages.bash
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

@test "[$GROUP] install jvm-example plugin with a symlinked plugins path" {
    # Clean up after the last time this test was run
    rm -rf /tmp/plugins.*
    rm -rf /tmp/old_plugins.*

    rm -rf "$ESPLUGINS"
    local es_plugins=$(mktemp -d -t 'plugins.XXXX')
    chown -R elasticsearch:elasticsearch "$es_plugins"
    ln -s "$es_plugins" "$ESPLUGINS"

    install_jvm_example
    start_elasticsearch_service
    # check that symlinked plugin was actually picked up
    curl -s localhost:9200/_cat/configured_example | sed 's/ *$//' > /tmp/installed
    echo "foo" > /tmp/expected
    diff /tmp/installed /tmp/expected
    stop_elasticsearch_service
    remove_jvm_example

    unlink "$ESPLUGINS"
}

@test "[$GROUP] install jvm-example plugin with a custom CONFIG_DIR" {
    # Clean up after the last time we ran this test
    rm -rf /tmp/config.*

    move_config

    ES_PATH_CONF="$ESCONFIG" install_jvm_example
    ES_PATH_CONF="$ESCONFIG" start_elasticsearch_service
    diff  <(curl -s localhost:9200/_cat/configured_example | sed 's/ //g') <(echo "foo")
    stop_elasticsearch_service
    ES_PATH_CONF="$ESCONFIG" remove_jvm_example
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
  run "$ESHOME/bin/elasticsearch-plugin"
  sudo chmod +x $JAVA

  [ "$status" -eq 1 ]
  local expected="could not find java; set JAVA_HOME or ensure java is in PATH"
  [[ "$output" == *"$expected"* ]] || {
    echo "Expected error message [$expected] but found: $output"
    false
  }
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

@test "[$GROUP] install ukrainian plugin" {
    install_and_check_plugin analysis ukrainian morfologik-fsa-*.jar morfologik-stemming-*.jar
}

@test "[$GROUP] install gce plugin" {
    install_and_check_plugin discovery gce google-api-client-*.jar
}

@test "[$GROUP] install discovery-azure-classic plugin" {
    install_and_check_plugin discovery azure-classic azure-core-*.jar
}

@test "[$GROUP] install discovery-ec2 plugin" {
    install_and_check_plugin discovery ec2 aws-java-sdk-core-*.jar
}

@test "[$GROUP] install discovery-file plugin" {
    install_and_check_plugin discovery file
}

@test "[$GROUP] install ingest-attachment plugin" {
    # we specify the version on the poi-3.17.jar so that the test does
    # not spuriously pass if the jar is missing but the other poi jars
    # are present
    install_and_check_plugin ingest attachment bcprov-jdk15on-*.jar tika-core-*.jar pdfbox-*.jar poi-3.17.jar poi-ooxml-3.17.jar poi-ooxml-schemas-*.jar poi-scratchpad-*.jar
}

@test "[$GROUP] install ingest-geoip plugin" {
    install_and_check_plugin ingest geoip geoip2-*.jar jackson-annotations-*.jar jackson-databind-*.jar maxmind-db-*.jar
}

@test "[$GROUP] install ingest-user-agent plugin" {
    install_and_check_plugin ingest user-agent
}

@test "[$GROUP] check ingest-common module" {
    check_module ingest-common jcodings-*.jar joni-*.jar
}

@test "[$GROUP] check lang-expression module" {
    # we specify the version on the asm-5.0.4.jar so that the test does
    # not spuriously pass if the jar is missing but the other asm jars
    # are present
    check_secure_module lang-expression antlr4-runtime-*.jar asm-5.0.4.jar asm-commons-*.jar asm-tree-*.jar lucene-expressions-*.jar
}

@test "[$GROUP] check lang-mustache module" {
    check_secure_module lang-mustache compiler-*.jar
}

@test "[$GROUP] check lang-painless module" {
    check_secure_module lang-painless antlr4-runtime-*.jar asm-debug-all-*.jar
}

@test "[$GROUP] install murmur3 mapper plugin" {
    install_and_check_plugin mapper murmur3
}

@test "[$GROUP] check reindex module" {
    check_module reindex
}

@test "[$GROUP] install repository-hdfs plugin" {
    install_and_check_plugin repository hdfs hadoop-client-*.jar hadoop-common-*.jar hadoop-annotations-*.jar hadoop-auth-*.jar hadoop-hdfs-client-*.jar htrace-core4-*.jar guava-*.jar protobuf-java-*.jar commons-logging-*.jar commons-cli-*.jar commons-collections-*.jar commons-configuration-*.jar commons-io-*.jar commons-lang-*.jar servlet-api-*.jar slf4j-api-*.jar
}

@test "[$GROUP] install size mapper plugin" {
    install_and_check_plugin mapper size
}

@test "[$GROUP] install repository-azure plugin" {
    install_and_check_plugin repository azure azure-storage-*.jar
}

@test "[$GROUP] install repository-gcs plugin" {
    install_and_check_plugin repository gcs google-api-services-storage-*.jar
}

@test "[$GROUP] install repository-s3 plugin" {
    install_and_check_plugin repository s3 aws-java-sdk-core-*.jar
}

@test "[$GROUP] install store-smb plugin" {
    install_and_check_plugin store smb
}

@test "[$GROUP] check the installed plugins can be listed with 'plugins list' and result matches the list of plugins in plugins pom" {
    "$ESHOME/bin/elasticsearch-plugin" list | cut -d'@' -f1 > /tmp/installed
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

@test "[$GROUP] remove ukrainian plugin" {
    remove_plugin analysis-ukrainian
}

@test "[$GROUP] remove gce plugin" {
    remove_plugin discovery-gce
}

@test "[$GROUP] remove discovery-azure-classic plugin" {
    remove_plugin discovery-azure-classic
}

@test "[$GROUP] remove discovery-ec2 plugin" {
    remove_plugin discovery-ec2
}

@test "[$GROUP] remove discovery-file plugin" {
    remove_plugin discovery-file
}

@test "[$GROUP] remove ingest-attachment plugin" {
    remove_plugin ingest-attachment
}

@test "[$GROUP] remove ingest-geoip plugin" {
    remove_plugin ingest-geoip
}

@test "[$GROUP] remove ingest-user-agent plugin" {
    remove_plugin ingest-user-agent
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

@test "[$GROUP] remove repository-gcs plugin" {
    remove_plugin repository-gcs
}

@test "[$GROUP] remove repository-hdfs plugin" {
    remove_plugin repository-hdfs
}

@test "[$GROUP] remove repository-s3 plugin" {
    remove_plugin repository-s3
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
    sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/elasticsearch-plugin" install "file://$relativePath" > /tmp/plugin-cli-output
    # exclude progress line
    local loglines=$(cat /tmp/plugin-cli-output | grep -v "^[[:cntrl:]]" | wc -l)
    [ "$loglines" -eq "2" ] || {
        echo "Expected 2 lines excluding progress bar but the output had $loglines lines and was:"
        cat /tmp/plugin-cli-output
        false
    }
    remove_jvm_example

    local relativePath=${1:-$(readlink -m jvm-example-*.zip)}
    sudo -E -u $ESPLUGIN_COMMAND_USER ES_JAVA_OPTS="-Des.logger.level=DEBUG" "$ESHOME/bin/elasticsearch-plugin" install "file://$relativePath" > /tmp/plugin-cli-output
    local loglines=$(cat /tmp/plugin-cli-output | grep -v "^[[:cntrl:]]" | wc -l)
    [ "$loglines" -gt "2" ] || {
        echo "Expected more than 2 lines excluding progress bar but the output had $loglines lines and was:"
        cat /tmp/plugin-cli-output
        false
    }
    remove_jvm_example
}

@test "[$GROUP] test java home with space" {
    # preserve JAVA_HOME
    local java_home=$JAVA_HOME

    # create a JAVA_HOME with a space
    local java=$(which java)
    local temp=`mktemp -d --suffix="java home"`
    mkdir -p "$temp/bin"
    ln -s "$java" "$temp/bin/java"
    export JAVA_HOME="$temp"

    # this will fail if the elasticsearch-plugin script does not
    # properly handle JAVA_HOME with spaces
    "$ESHOME/bin/elasticsearch-plugin" list

    rm -rf "$temp"

    # restore JAVA_HOME
    export JAVA_HOME=$java_home
}

@test "[$GROUP] test ES_JAVA_OPTS" {
    # preserve ES_JAVA_OPTS
    local es_java_opts=$ES_JAVA_OPTS

    export ES_JAVA_OPTS="-XX:+PrintFlagsFinal"
    # this will fail if ES_JAVA_OPTS is not passed through
    "$ESHOME/bin/elasticsearch-plugin" list | grep MaxHeapSize

    # restore ES_JAVA_OPTS
    export ES_JAVA_OPTS=$es_java_opts
}

@test "[$GROUP] test umask" {
    install_jvm_example $(readlink -m jvm-example-*.zip) 0077
}

@test "[$GROUP] hostname" {
    local temp=`mktemp -d`
    cp "$ESCONFIG"/elasticsearch.yml "$temp"
    echo 'node.name: ${HOSTNAME}' >> "$ESCONFIG"/elasticsearch.yml
    start_elasticsearch_service
    wait_for_elasticsearch_status
    [ "$(curl -XGET localhost:9200/_cat/nodes?h=name)" == "$HOSTNAME" ]
    stop_elasticsearch_service
    cp "$temp"/elasticsearch.yml "$ESCONFIG"/elasticsearch.yml
    rm -rf "$temp"
}
