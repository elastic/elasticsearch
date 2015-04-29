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


# Checks if necessary commands are available to run the tests

if [ ! -x /usr/bin/which ]; then
    echo "'which' command is mandatory to run the tests"
    exit 1
fi

if [ ! -x "`which wget 2>/dev/null`" ]; then
    echo "'wget' command is mandatory to run the tests"
    exit 1
fi

if [ ! -x "`which curl 2>/dev/null`" ]; then
    echo "'curl' command is mandatory to run the tests"
    exit 1
fi

if [ ! -x "`which pgrep 2>/dev/null`" ]; then
    echo "'pgrep' command is mandatory to run the tests"
    exit 1
fi

if [ ! -x "`which unzip 2>/dev/null`" ]; then
    echo "'unzip' command is mandatory to run the tests"
    exit 1
fi

if [ ! -x "`which tar 2>/dev/null`" ]; then
    echo "'tar' command is mandatory to run the tests"
    exit 1
fi

if [ ! -x "`which unzip 2>/dev/null`" ]; then
    echo "'unzip' command is mandatory to run the tests"
    exit 1
fi

if [ ! -x "`which java 2>/dev/null`" ]; then
    echo "'java' command is mandatory to run the tests"
    exit 1
fi

# Returns 0 if the 'dpkg' command is available
is_dpkg() {
    [ -x "`which dpkg 2>/dev/null`" ]
}

# Returns 0 if the 'rpm' command is available
is_rpm() {
    [ -x "`which rpm 2>/dev/null`" ]
}

# Skip test if the 'dpkg' command is not supported
skip_not_dpkg() {
    if [ ! -x "`which dpkg 2>/dev/null`" ]; then
        skip "dpkg is not supported"
    fi
}

# Skip test if the 'rpm' command is not supported
skip_not_rpm() {
    if [ ! -x "`which rpm 2>/dev/null`" ]; then
        skip "rpm is not supported"
    fi
}

# Returns 0 if the system supports Systemd
is_systemd() {
    [ -x /bin/systemctl ]
}

# Skip test if Systemd is not supported
skip_not_systemd() {
    if [ ! -x /bin/systemctl ]; then
        skip "systemd is not supported"
    fi
}

# Returns 0 if the system supports SysV
is_sysvinit() {
    [ -x "`which service 2>/dev/null`" ]
}

# Skip test if SysV is not supported
skip_not_sysvinit() {
    if [ -x "`which service 2>/dev/null`" ] && is_systemd; then
        skip "sysvinit is supported, but systemd too"
    fi
    if [ ! -x "`which service 2>/dev/null`" ]; then
        skip "sysvinit is not supported"
    fi
}

# Skip if tar is not supported
skip_not_tar_gz() {
    if [ ! -x "`which tar 2>/dev/null`" ]; then
        skip "tar is not supported"
    fi
}

# Skip if unzip is not supported
skip_not_zip() {
    if [ ! -x "`which unzip 2>/dev/null`" ]; then
        skip "unzip is not supported"
    fi
}

assert_file_exist() {
    [ -e "$1" ]
}

assert_file_not_exist() {
    [ ! -e "$1" ]
}

assert_file() {
    local file=$1
    local type=$2
    local user=$3
    local privileges=$4

    [ -n "$file" ] && [ -e "$file" ]

    if [ "$type" = "d" ]; then
        [ -d "$file" ]
    else
        [ -f "$file" ]
    fi

    if [ "x$user" != "x" ]; then
        realuser=$(ls -ld "$file" | awk '{print $3}')
        [ "$realuser" = "$user" ]
    fi

    if [ "x$privileges" != "x" ]; then
        realprivileges=$(find "$file" -maxdepth 0 -printf "%m")
        [ "$realprivileges" = "$privileges" ]
    fi
}

assert_output() {
    echo "$output" | grep -E "$1"
}

# Checks that all directories & files are correctly installed
verify_package_installation() {

    run id elasticsearch
    [ "$status" -eq 0 ]

    run getent group elasticsearch
    [ "$status" -eq 0 ]

    # Home dir
    assert_file "/usr/share/elasticsearch" d root 755
    # Bin dir
    assert_file "/usr/share/elasticsearch/bin" d root 755
    assert_file "/usr/share/elasticsearch/lib" d root 755
    # Conf dir
    assert_file "/etc/elasticsearch" d root 755
    assert_file "/etc/elasticsearch/elasticsearch.yml" f root 644
    assert_file "/etc/elasticsearch/logging.yml" f root 644
    # Data dir
    assert_file "/var/lib/elasticsearch" d elasticsearch 755
    # Log dir
    assert_file "/var/log/elasticsearch" d elasticsearch 755
    # Plugins dir
    assert_file "/usr/share/elasticsearch/plugins" d elasticsearch 755
    # PID dir
    assert_file "/var/run/elasticsearch" d elasticsearch 755
    # Readme files
    assert_file "/usr/share/elasticsearch/NOTICE.txt" f root 644
    assert_file "/usr/share/elasticsearch/README.textile" f root 644

    if is_dpkg; then
        # Env file
        assert_file "/etc/default/elasticsearch" f root 644

        # Doc files
        assert_file "/usr/share/doc/elasticsearch" d root 755
        assert_file "/usr/share/doc/elasticsearch/copyright" f root 644

    fi

    if is_rpm; then
        # Env file
        assert_file "/etc/sysconfig/elasticsearch" f root 644
        # License file
        assert_file "/usr/share/elasticsearch/LICENSE.txt" f root 644
    fi
}

# Deletes everything before running a test file
clean_before_test() {

    # List of files to be deleted
    ELASTICSEARCH_TEST_FILES=("/usr/share/elasticsearch" \
                            "/etc/elasticsearch" \
                            "/var/lib/elasticsearch" \
                            "/var/log/elasticsearch" \
                            "/tmp/elasticsearch" \
                            "/etc/default/elasticsearch" \
                            "/etc/sysconfig/elasticsearch"  \
                            "/var/run/elasticsearch"  \
                            "/usr/share/doc/elasticsearch")

    if [ "$ES_CLEAN_BEFORE_TEST" = "true" ]; then
        # Kills all processes of user elasticsearch
        if id elasticsearch > /dev/null 2>&1; then
            pkill -u elasticsearch 2>/dev/null || true
        fi

        # Removes RPM package
        if is_rpm; then
            rpm --quiet -e elasticsearch 2>/dev/null || true
        fi

        if [ -x "`which yum 2>/dev/null`" ]; then
            yum remove -y elasticsearch 2>/dev/null || true
        fi

        # Removes DEB package
        if is_dpkg; then
            dpkg --purge elasticsearch > /dev/null 2>&1 || true
        fi

        if [ -x "`which apt-get 2>/dev/null`" ]; then
            apt-get --quiet --yes purge elasticsearch > /dev/null 2>&1 || true
        fi

        # Removes user & group
        userdel elasticsearch > /dev/null 2>&1 || true
        groupdel elasticsearch > /dev/null 2>&1 || true


        # Removes all files
        for d in "${ELASTICSEARCH_TEST_FILES[@]}"; do
            if [ -e "$d" ]; then
                rm -rf "$d"
            fi
        done
    fi

    # Checks that all files are deleted
    for d in "${ELASTICSEARCH_TEST_FILES[@]}"; do
        if [ -e "$d" ]; then
            echo "$d should not exist before running the tests" >&2
            exit 1
        fi
    done
}

start_elasticsearch_service() {

    if is_systemd; then
        run systemctl daemon-reload
        [ "$status" -eq 0 ]

        run systemctl enable elasticsearch.service
        [ "$status" -eq 0 ]

        run systemctl is-enabled elasticsearch.service
        [ "$status" -eq 0 ]

        run systemctl start elasticsearch.service
        [ "$status" -eq 0 ]

    elif is_sysvinit; then
        run service elasticsearch start
        [ "$status" -eq 0 ]
    fi

    wait_for_elasticsearch_status

    if is_systemd; then
        run systemctl is-active elasticsearch.service
        [ "$status" -eq 0 ]

        run systemctl status elasticsearch.service
        [ "$status" -eq 0 ]

    elif is_sysvinit; then
        run service elasticsearch status
        [ "$status" -eq 0 ]
    fi
}

stop_elasticsearch_service() {

    if is_systemd; then
        run systemctl stop elasticsearch.service
        [ "$status" -eq 0 ]

        run systemctl is-active elasticsearch.service
        [ "$status" -eq 3 ]
        [ "$output" = "inactive" ]

    elif is_sysvinit; then
        run service elasticsearch stop
        [ "$status" -eq 0 ]

        run service elasticsearch status
        [ "$status" -ne 0 ]
    fi
}

# Waits for Elasticsearch to reach a given status (defaults to "green")
wait_for_elasticsearch_status() {
    local status="green"
    if [ "x$1" != "x" ]; then
        status="$1"
    fi

    # Try to connect to elasticsearch and wait for expected status
    wget --quiet --retry-connrefused --waitretry=1 --timeout=20 \
         --output-document=/dev/null "http://localhost:9200/_cluster/health?wait_for_status=$status&timeout=20s"

    # Checks the cluster health
    curl -XGET 'http://localhost:9200/_cat/health?h=status&v=false'
    if [ $? -ne 0 ]; then
        echo "error when checking cluster health" >&2
        exit 1
    fi
}

# Executes some very basic Elasticsearch tests
run_elasticsearch_tests() {
    run curl -XGET 'http://localhost:9200/_cat/health?h=status&v=false'
    [ "$status" -eq 0 ]
    echo "$output" | grep -w "green"

    run curl -XPOST 'http://localhost:9200/library/book/1?refresh=true' -d '{"title": "Elasticsearch - The Definitive Guide"}' 2>&1
    [ "$status" -eq 0 ]

    run curl -XGET 'http://localhost:9200/_cat/count?h=count&v=false'
    [ "$status" -eq 0 ]
    echo "$output" | grep -w "1"

    run curl -XDELETE 'http://localhost:9200/_all'
    [ "$status" -eq 0 ]
}
