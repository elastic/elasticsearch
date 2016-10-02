#!/bin/bash

# This file contains some utilities to test the elasticsearch scripts,
# the .deb/.rpm packages and the SysV/Systemd scripts.

# WARNING: This testing file must be executed as root and can
# dramatically change your system. It removes the 'elasticsearch'
# user/group and also many directories. Do not execute this file
# unless you know exactly what you are doing.

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

# Install a plugin an run all the common post installation tests.
install_plugin() {
    local name=$1
    local path="$2"

    assert_file_exist "$path"

    sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/elasticsearch-plugin" install -batch "file://$path"

    assert_file_exist "$ESPLUGINS/$name"
    assert_file_exist "$ESPLUGINS/$name/plugin-descriptor.properties"
    #check we did not accidentially create a log file as root as /usr/share/elasticsearch
    assert_file_not_exist "/usr/share/elasticsearch/logs"

    # At some point installing or removing plugins caused elasticsearch's logs
    # to be owned by root. This is bad so we want to make sure it doesn't
    # happen.
    if [ -e "$ESLOG" ] && [ $(stat "$ESLOG" --format "%U") == "root" ]; then
        echo "$ESLOG is now owned by root! That'll break logging when elasticsearch tries to start."
        false
    fi
}

install_jvm_plugin() {
    local name=$1
    local path="$2"
    install_plugin $name "$path"
    assert_file_exist "$ESPLUGINS/$name/$name"*".jar"
}

# Remove a plugin and make sure its plugin directory is removed.
remove_plugin() {
    local name=$1

    echo "Removing $name...."
    sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/elasticsearch-plugin" remove $name

    assert_file_not_exist "$ESPLUGINS/$name"

    # At some point installing or removing plugins caused elasticsearch's logs
    # to be owned by root. This is bad so we want to make sure it doesn't
    # happen.
    if [ -e "$ESLOG" ] && [ $(stat "$ESLOG" --format "%U") == "root" ]; then
        echo "$ESLOG is now owned by root! That'll break logging when elasticsearch tries to start."
        false
    fi
}

# Install the jvm-example plugin which fully exercises the special case file
# placements for non-site plugins.
install_jvm_example() {
    local relativePath=${1:-$(readlink -m jvm-example-*.zip)}
    install_jvm_plugin jvm-example "$relativePath"

    #owner group and permissions vary depending on how es was installed
    #just make sure that everything is the same as the parent bin dir, which was properly set up during install
    bin_user=$(find "$ESHOME/bin" -maxdepth 0 -printf "%u")
    bin_owner=$(find "$ESHOME/bin" -maxdepth 0 -printf "%g")
    bin_privileges=$(find "$ESHOME/bin" -maxdepth 0 -printf "%m")
    assert_file "$ESHOME/bin/jvm-example" d $bin_user $bin_owner 755
    assert_file "$ESHOME/bin/jvm-example/test" f $bin_user $bin_owner 755

    #owner group and permissions vary depending on how es was installed
    #just make sure that everything is the same as $CONFIG_DIR, which was properly set up during install
    config_user=$(find "$ESCONFIG" -maxdepth 0 -printf "%u")
    config_owner=$(find "$ESCONFIG" -maxdepth 0 -printf "%g")
    # directories should user the user file-creation mask
    assert_file "$ESCONFIG/jvm-example" d $config_user $config_owner 755
    assert_file "$ESCONFIG/jvm-example/example.yaml" f $config_user $config_owner 644

    echo "Running jvm-example's bin script...."
    "$ESHOME/bin/jvm-example/test" | grep test
}

# Remove the jvm-example plugin which fully exercises the special cases of
# removing bin and not removing config.
remove_jvm_example() {
    remove_plugin jvm-example

    assert_file_not_exist "$ESHOME/bin/jvm-example"
    assert_file_exist "$ESCONFIG/jvm-example"
    assert_file_exist "$ESCONFIG/jvm-example/example.yaml"
}

# Install a plugin with a special prefix. For the most part prefixes are just
# useful for grouping but the "analysis" prefix is special because all
# analysis plugins come with a corresponding lucene-analyzers jar.
# $1 - the prefix
# $2 - the plugin name
# $@ - all remaining arguments are jars that must exist in the plugin's
#      installation directory
install_and_check_plugin() {
    local prefix=$1
    shift
    local name=$1
    shift

    if [ "$prefix" == "-" ]; then
        local fullName="$name"
    else
        local fullName="$prefix-$name"
    fi

    install_jvm_plugin $fullName "$(readlink -m $fullName-*.zip)"

    assert_module_or_plugin_directory "$ESPLUGINS/$fullName"

    if [ $prefix == 'analysis' ]; then
        assert_module_or_plugin_file "$ESPLUGINS/$fullName/lucene-analyzers-$name-*.jar"
    fi
    for file in "$@"; do
        assert_module_or_plugin_file "$ESPLUGINS/$fullName/$file"
    done
}

# Compare a list of plugin names to the plugins in the plugins pom and see if they are the same
# $1 the file containing the list of plugins we want to compare to
# $2 description of the source of the plugin list
compare_plugins_list() {
    cat $1 | sort > /tmp/plugins
    ls /elasticsearch/plugins/*/build.gradle | cut -d '/' -f 4 |
        sort > /tmp/expected
    echo "Checking plugins from $2 (<) against expected plugins (>):"
    diff /tmp/expected /tmp/plugins
}
