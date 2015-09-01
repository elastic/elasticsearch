#!/bin/sh

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

    "$ESHOME/bin/plugin" install "file://$path"

    assert_file_exist "$ESPLUGINS/$name"
    assert_file_exist "$ESPLUGINS/$name/plugin-descriptor.properties"
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
    "$ESHOME/bin/plugin" remove $name

    assert_file_not_exist "$ESPLUGINS/$name"
}

# Install the jvm-example plugin which fully excercises the special case file
# placements for non-site plugins.
install_jvm_example() {
    local relativePath=${1:-$(readlink -m jvm-example-*.zip)}
    install_jvm_plugin jvm-example "$relativePath"

    assert_file_exist "$ESHOME/bin/jvm-example"
    assert_file_exist "$ESHOME/bin/jvm-example/test"
    assert_file_exist "$ESCONFIG/jvm-example"
    assert_file_exist "$ESCONFIG/jvm-example/example.yaml"

    echo "Running jvm-example's bin script...."
    "$ESHOME/bin/jvm-example/test" | grep test
}

# Remove the jvm-example plugin which fully excercises the special cases of
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
    if [ $prefix == 'analysis' ]; then
        assert_file_exist "$(readlink -m $ESPLUGINS/$fullName/lucene-analyzers-$name-*.jar)"
    fi
    for file in "$@"; do
        assert_file_exist "$(readlink -m $ESPLUGINS/$fullName/$file)"
    done
}
