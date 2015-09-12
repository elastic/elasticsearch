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


# Install the tar.gz archive
install_archive() {
    export ESHOME=${1:-/tmp/elasticsearch}

    echo "Unpacking tarball to $ESHOME"
    rm -rf /tmp/untar
    mkdir -p /tmp/untar
    tar -xzf elasticsearch*.tar.gz -C /tmp/untar

    find /tmp/untar -depth -type d -name 'elasticsearch*' -exec mv {} "$ESHOME" \; > /dev/null

    # ES cannot run as root so create elasticsearch user & group if needed
    if ! getent group "elasticsearch" > /dev/null 2>&1 ; then
        if is_dpkg; then
            addgroup --system "elasticsearch"
        else
            groupadd -r "elasticsearch"
        fi
    fi
    if ! id "elasticsearch" > /dev/null 2>&1 ; then
        if is_dpkg; then
            adduser --quiet --system --no-create-home --ingroup "elasticsearch" --disabled-password --shell /bin/false "elasticsearch"
        else
            useradd --system -M --gid "elasticsearch" --shell /sbin/nologin --comment "elasticsearch user" "elasticsearch"
        fi
    fi

    chown -R elasticsearch:elasticsearch "$ESHOME"
    export_elasticsearch_paths
}

# Move the unzipped tarball to another location.
move_elasticsearch() {
    local oldhome="$ESHOME"
    export ESHOME="$1"
    rm -rf "$ESHOME"
    mv "$oldhome" "$ESHOME"
    export_elasticsearch_paths
}

# Export some useful paths.
export_elasticsearch_paths() {
    export ESPLUGINS="$ESHOME/plugins"
    export ESCONFIG="$ESHOME/config"
}
