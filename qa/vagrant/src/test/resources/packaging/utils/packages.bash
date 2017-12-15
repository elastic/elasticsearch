#!/bin/bash

# This file contains some utilities to test the elasticsearch scripts with
# the .deb/.rpm packages.

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


# Export some useful paths.
export_elasticsearch_paths() {
    export ESHOME="/usr/share/elasticsearch"
    export ESPLUGINS="$ESHOME/plugins"
    export ESMODULES="$ESHOME/modules"
    export ESCONFIG="/etc/elasticsearch"
    export ESDATA="/var/lib/elasticsearch"
    export ESLOG="/var/log/elasticsearch"
    export ESPIDDIR="/var/run/elasticsearch"
    if is_dpkg; then
        export ESENVFILE="/etc/default/elasticsearch"
    fi
    if is_rpm; then
        export ESENVFILE="/etc/sysconfig/elasticsearch"
    fi
}

# Install the rpm or deb package.
# -u upgrade rather than install. This only matters for rpm.
# -v the version to upgrade to. Defaults to the version under test.
install_package() {
    local version=$(cat version)
    local rpmCommand='-i'
    while getopts ":fuv:" opt; do
        case $opt in
            u)
                rpmCommand='-U'
                dpkgCommand='--force-confnew'
                ;;
            f)
                rpmCommand='-U --force'
                dpkgCommand='--force-conflicts'
                ;;
            v)
                version=$OPTARG
                ;;
            \?)
                echo "Invalid option: -$OPTARG" >&2
                ;;
        esac
    done
    if is_rpm; then
        rpm $rpmCommand elasticsearch-$version.rpm
    elif is_dpkg; then
        dpkg $dpkgCommand -i elasticsearch-$version.deb
    else
        skip "Only rpm or deb supported"
    fi
}

# Checks that all directories & files are correctly installed after a deb or
# rpm install.
verify_package_installation() {
    id elasticsearch

    getent group elasticsearch

    assert_file "$ESHOME" d root root 755
    assert_file "$ESHOME/bin" d root root 755
    assert_file "$ESHOME/bin/elasticsearch" f root root 755
    assert_file "$ESHOME/bin/elasticsearch-plugin" f root root 755
    assert_file "$ESHOME/bin/elasticsearch-translog" f root root 755
    assert_file "$ESHOME/lib" d root root 755
    assert_file "$ESCONFIG" d root elasticsearch 2750
    assert_file "$ESCONFIG/elasticsearch.yml" f root elasticsearch 660
    assert_file "$ESCONFIG/jvm.options" f root elasticsearch 660
    assert_file "$ESCONFIG/log4j2.properties" f root elasticsearch 660
    assert_file "$ESDATA" d elasticsearch elasticsearch 750
    assert_file "$ESLOG" d elasticsearch elasticsearch 750
    assert_file "$ESPLUGINS" d root root 755
    assert_file "$ESMODULES" d root root 755
    assert_file "$ESPIDDIR" d elasticsearch elasticsearch 755
    assert_file "$ESHOME/NOTICE.txt" f root root 644
    assert_file "$ESHOME/README.textile" f root root 644

    if is_dpkg; then
        # Env file
        assert_file "/etc/default/elasticsearch" f root elasticsearch 660

        # Doc files
        assert_file "/usr/share/doc/elasticsearch" d root root 755
        assert_file "/usr/share/doc/elasticsearch/copyright" f root root 644
    fi

    if is_rpm; then
        # Env file
        assert_file "/etc/sysconfig/elasticsearch" f root elasticsearch 660
        # License file
        assert_file "/usr/share/elasticsearch/LICENSE.txt" f root root 644
    fi

    if is_systemd; then
        assert_file "/usr/lib/systemd/system/elasticsearch.service" f root root 644
        assert_file "/usr/lib/tmpfiles.d/elasticsearch.conf" f root root 644
        assert_file "/usr/lib/sysctl.d/elasticsearch.conf" f root root 644
        if is_rpm; then
            [[ $(/usr/sbin/sysctl vm.max_map_count) =~ "vm.max_map_count = 262144" ]]
        else
            [[ $(/sbin/sysctl vm.max_map_count) =~ "vm.max_map_count = 262144" ]]
        fi
    fi

    if is_sysvinit; then
        assert_file "/etc/init.d/elasticsearch" f root root 750
    fi

    run sudo -E -u vagrant LANG="en_US.UTF-8" cat "$ESCONFIG/elasticsearch.yml"
    [ $status = 1 ]
    [[ "$output" == *"Permission denied"* ]] || {
        echo "Expected permission denied but found $output:"
        false
    }
}
