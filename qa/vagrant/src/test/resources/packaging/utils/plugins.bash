#!/bin/bash

# This file contains some utilities to test the elasticsearch
# plugin installation and uninstallation process.

# WARNING: This testing file must be executed as root and can
# dramatically change your system. It should only be executed
# in a throw-away VM like those made by the Vagrantfile at
# the root of the Elasticsearch source code. This should
# cause the script to fail if it is executed any other way:
[ -f /etc/is_vagrant_vm ] || {
  >&2 echo "must be run on a vagrant VM"
  exit 1
}

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
    local umask="$3"

    assert_file_exist "$path"

    if [ ! -z "$ES_PATH_CONF" ] ; then
        if is_dpkg; then
            echo "ES_PATH_CONF=$ES_PATH_CONF" >> /etc/default/elasticsearch;
        elif is_rpm; then
            echo "ES_PATH_CONF=$ES_PATH_CONF" >> /etc/sysconfig/elasticsearch;
        fi
    fi

    if [ -z "$umask" ]; then
      sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/elasticsearch-plugin" install -batch "file://$path"
    else
      sudo -E -u $ESPLUGIN_COMMAND_USER bash -c "umask $umask && \"$ESHOME/bin/elasticsearch-plugin\" install -batch \"file://$path\""
    fi

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
    install_plugin $name "$path" $3
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
    install_jvm_plugin jvm-example "$relativePath" $2

    bin_user=$(find "$ESHOME/bin" -maxdepth 0 -printf "%u")
    bin_owner=$(find "$ESHOME/bin" -maxdepth 0 -printf "%g")

    assert_file "$ESHOME/plugins/jvm-example" d $bin_user $bin_owner 755
    assert_file "$ESHOME/plugins/jvm-example/jvm-example-$(cat version).jar" f $bin_user $bin_owner 644

    #owner group and permissions vary depending on how es was installed
    #just make sure that everything is the same as the parent bin dir, which was properly set up during install
    assert_file "$ESHOME/bin/jvm-example" d $bin_user $bin_owner 755
    assert_file "$ESHOME/bin/jvm-example/test" f $bin_user $bin_owner 755

    #owner group and permissions vary depending on how es was installed
    #just make sure that everything is the same as $CONFIG_DIR, which was properly set up during install
    config_user=$(find "$ESCONFIG" -maxdepth 0 -printf "%u")
    config_owner=$(find "$ESCONFIG" -maxdepth 0 -printf "%g")
    # directories should user the user file-creation mask
    assert_file "$ESCONFIG/jvm-example" d $config_user $config_owner 750
    assert_file "$ESCONFIG/jvm-example/example.yml" f $config_user $config_owner 660

    run sudo -E -u vagrant LANG="en_US.UTF-8" cat "$ESCONFIG/jvm-example/example.yml"
    [ $status = 1 ]
    [[ "$output" == *"Permission denied"* ]] || {
        echo "Expected permission denied but found $output:"
        false
    }

    echo "Running jvm-example's bin script...."
    "$ESHOME/bin/jvm-example/test" | grep test
}

# Remove the jvm-example plugin which fully exercises the special cases of
# removing bin and not removing config.
remove_jvm_example() {
    remove_plugin jvm-example

    assert_file_not_exist "$ESHOME/bin/jvm-example"
    assert_file_exist "$ESCONFIG/jvm-example"
    assert_file_exist "$ESCONFIG/jvm-example/example.yml"
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
        local full_name="$name"
    else
        local full_name="$prefix-$name"
    fi

    install_jvm_plugin $full_name "$(readlink -m $full_name-*.zip)"

    assert_module_or_plugin_directory "$ESPLUGINS/$full_name"

    # analysis plugins have a corresponding analyzers jar
    if [ $prefix == 'analysis' ]; then
        local analyzer_jar_suffix=$name
        # the name of the analyzer jar for the ukrainian plugin does
        # not match the name of the plugin, so we have to make an
        # exception
        if [ $name == 'ukrainian' ]; then
             analyzer_jar_suffix='morfologik'
        fi
        assert_module_or_plugin_file "$ESPLUGINS/$full_name/lucene-analyzers-$analyzer_jar_suffix-*.jar"
    fi
    for file in "$@"; do
        assert_module_or_plugin_file "$ESPLUGINS/$full_name/$file"
    done
}

# Compare a list of plugin names to the plugins in the plugins pom and see if they are the same
# $1 the file containing the list of plugins we want to compare to
# $2 description of the source of the plugin list
compare_plugins_list() {
    cat $1 | sort > /tmp/plugins
    echo "Checking plugins from $2 (<) against expected plugins (>):"
    diff -w /elasticsearch/qa/vagrant/build/plugins/expected /tmp/plugins
}
