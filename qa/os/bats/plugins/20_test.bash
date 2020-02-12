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
            [[ $BATS_TEST_NAME =~ install_a_sample_plugin ]] ||
            [ ! -d "$ESHOME" ]; then
        clean_before_test
        install
        set_debug_logging
    fi
}

load $BATS_UTILS/tar.bash
GROUP='TAR PLUGINS'
install() {
    install_archive
    verify_archive_installation
}
export ESHOME=/tmp/elasticsearch
export_elasticsearch_paths
export ESPLUGIN_COMMAND_USER=elasticsearch

@test "[$GROUP] do nothing" {

}

@test "[$GROUP] install a sample plugin with a symlinked plugins path" {
    # Clean up after the last time this test was run
    rm -rf /var/plugins.*
    rm -rf /var/old_plugins.*

    rm -rf "$ESPLUGINS"
    # The custom plugins directory is not under /tmp or /var/tmp because
    # systemd's private temp directory functionally means different
    # processes can have different views of what's in these directories
    local es_plugins=$(mktemp -p /var -d -t 'plugins.XXXX')
    chown -R elasticsearch:elasticsearch "$es_plugins"
    ln -s "$es_plugins" "$ESPLUGINS"

    install_plugin_example
    start_elasticsearch_service

    # check that symlinked plugin was actually picked up
    #curl -XGET -H 'Content-Type: application/json' 'http://localhost:9200/_cat/plugins?h=component' | sed 's/ *$//' > /tmp/installed
    #echo "custom-settings" > /tmp/expected
    #diff /tmp/installed /tmp/expected

    #curl -XGET -H 'Content-Type: application/json' 'http://localhost:9200/_cluster/settings?include_defaults&filter_path=defaults.custom.simple' > /tmp/installed
    #echo -n '{"defaults":{"custom":{"simple":"foo"}}}' > /tmp/expected
    #diff /tmp/installed /tmp/expected

    #stop_elasticsearch_service
    #remove_plugin_example

    unlink "$ESPLUGINS"
}
