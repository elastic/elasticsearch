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
    is_dpkg || skip "dpkg is not supported"
}

# Skip test if the 'rpm' command is not supported
skip_not_rpm() {
    is_rpm || skip "rpm is not supported"
}

skip_not_dpkg_or_rpm() {
    is_dpkg || is_rpm || skip "only dpkg or rpm systems are supported"
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
    local file="$1"
    echo "Should exist: ${file}"
    local file=$(readlink -m "${file}")
    [ -e "$file" ]
}

assert_file_not_exist() {
    local file="$1"
    echo "Should not exist: ${file}"
    local file=$(readlink -m "${file}")
    [ ! -e "$file" ]
}

assert_file() {
    local file="$1"
    local type=$2
    local user=$3
    local privileges=$4

    assert_file_exist "$file"

    if [ "$type" = "d" ]; then
        echo "And be a directory...."
        [ -d "$file" ]
    else
        echo "And be a regular file...."
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
# after a package (deb/rpm) install
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

    if is_systemd; then
        assert_file "/usr/lib/systemd/system/elasticsearch.service" f root 644
        assert_file "/usr/lib/tmpfiles.d/elasticsearch.conf" f root 644
        assert_file "/usr/lib/sysctl.d/elasticsearch.conf" f root 644
    fi
}

# Install the rpm or deb package.
# -u upgrade rather than install. This only matters for rpm.
# -v the version to upgrade to. Defaults to the version under test.
install_package() {
    local version=$(cat version)
    local rpmCommand='-i'
    while getopts ":uv:" opt; do
        case $opt in
            u)
                rpmCommand='-U'
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
        dpkg -i elasticsearch-$version.deb
    else
        skip "Only rpm or deb supported"
    fi
}

# Checks that all directories & files are correctly installed
# after a archive (tar.gz/zip) install
verify_archive_installation() {
    assert_file "$ESHOME" d
    assert_file "$ESHOME/bin" d
    assert_file "$ESHOME/bin/elasticsearch" f
    assert_file "$ESHOME/bin/elasticsearch.in.sh" f
    assert_file "$ESHOME/bin/plugin" f
    assert_file "$ESCONFIG" d
    assert_file "$ESCONFIG/elasticsearch.yml" f
    assert_file "$ESCONFIG/logging.yml" f
    assert_file "$ESHOME/lib" d
    assert_file "$ESHOME/NOTICE.txt" f
    assert_file "$ESHOME/LICENSE.txt" f
    assert_file "$ESHOME/README.textile" f
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
                            "/usr/share/doc/elasticsearch" \
                            "/tmp/elasticsearch" \
                            "/usr/lib/systemd/system/elasticsearch.conf" \
                            "/usr/lib/tmpfiles.d/elasticsearch.conf" \
                            "/usr/lib/sysctl.d/elasticsearch.conf")

    # Kills all processes of user elasticsearch
    if id elasticsearch > /dev/null 2>&1; then
        pkill -u elasticsearch 2>/dev/null || true
    fi

    # Kills all running Elasticsearch processes
    ps aux | grep -i "org.elasticsearch.bootstrap.Elasticsearch" | awk {'print $2'} | xargs kill -9 > /dev/null 2>&1 || true

    # Removes RPM package
    if is_rpm; then
        rpm --quiet -e elasticsearch > /dev/null 2>&1 || true
    fi

    if [ -x "`which yum 2>/dev/null`" ]; then
        yum remove -y elasticsearch > /dev/null 2>&1 || true
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
}

# Start elasticsearch and wait for it to come up with a status.
# $1 - expected status - defaults to green
start_elasticsearch_service() {
    local desiredStatus=${1:-green}

    if [ -f "/tmp/elasticsearch/bin/elasticsearch" ]; then
        # su and the Elasticsearch init script work together to break bats.
        # sudo isolates bats enough from the init script so everything continues
        # to tick along
        sudo -u elasticsearch /tmp/elasticsearch/bin/elasticsearch -d \
            -p /tmp/elasticsearch/elasticsearch.pid
    elif is_systemd; then
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

    wait_for_elasticsearch_status $desiredStatus

    if [ -r "/tmp/elasticsearch/elasticsearch.pid" ]; then
        pid=$(cat /tmp/elasticsearch/elasticsearch.pid)
        [ "x$pid" != "x" ] && [ "$pid" -gt 0 ]

        echo "Looking for elasticsearch pid...."
        ps $pid
    elif is_systemd; then
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
    if [ -r "/tmp/elasticsearch/elasticsearch.pid" ]; then
        pid=$(cat /tmp/elasticsearch/elasticsearch.pid)
        [ "x$pid" != "x" ] && [ "$pid" -gt 0 ]

        kill -SIGTERM $pid
    elif is_systemd; then
        run systemctl stop elasticsearch.service
        [ "$status" -eq 0 ]

        run systemctl is-active elasticsearch.service
        [ "$status" -eq 3 ]

        echo "$output" | grep -E 'inactive|failed'

    elif is_sysvinit; then
        run service elasticsearch stop
        [ "$status" -eq 0 ]

        run service elasticsearch status
        [ "$status" -ne 0 ]
    fi
}

# Waits for Elasticsearch to reach some status.
# $1 - expected status - defaults to green
wait_for_elasticsearch_status() {
    local desiredStatus=${1:-green}

    echo "Making sure elasticsearch is up..."
    wget -O - --retry-connrefused --waitretry=1 --timeout=60 http://localhost:9200 || {
          echo "Looks like elasticsearch never started. Here is its log:"
          if [ -r "/tmp/elasticsearch/elasticsearch.pid" ]; then
              cat /tmp/elasticsearch/log/elasticsearch.log
          else
              if [ -e '/var/log/elasticsearch/elasticsearch.log' ]; then
                  cat /var/log/elasticsearch/elasticsearch.log
              else
                  echo "The elasticsearch log doesn't exist. Maybe /vag/log/messages has something:"
                  tail -n20 /var/log/messages
              fi
          fi
          false
    }

    echo "Tring to connect to elasticsearch and wait for expected status..."
    curl -sS "http://localhost:9200/_cluster/health?wait_for_status=$desiredStatus&timeout=60s&pretty"
    if [ $? -eq 0 ]; then
        echo "Connected"
    else
        echo "Unable to connect to Elastisearch"
        false
    fi

    echo "Checking that the cluster health matches the waited for status..."
    run curl -sS -XGET 'http://localhost:9200/_cat/health?h=status&v=false'
    if [ "$status" -ne 0 ]; then
        echo "error when checking cluster health. code=$status output="
        echo $output
        false
    fi
    echo $output | grep $desiredStatus || {
        echo "unexpected status:  '$output' wanted '$desiredStatus'"
        false
    }
}

# Executes some very basic Elasticsearch tests
run_elasticsearch_tests() {
    # TODO this assertion is the same the one made when waiting for
    # elasticsearch to start
    run curl -XGET 'http://localhost:9200/_cat/health?h=status&v=false'
    [ "$status" -eq 0 ]
    echo "$output" | grep -w "green"

    curl -s -XPOST 'http://localhost:9200/library/book/1?refresh=true&pretty' -d '{
      "title": "Elasticsearch - The Definitive Guide"
    }'

    curl -s -XGET 'http://localhost:9200/_cat/count?h=count&v=false&pretty' |
      grep -w "1"

    curl -s -XDELETE 'http://localhost:9200/_all'
}

# Move the config directory to another directory and properly chown it.
move_config() {
    local oldConfig="$ESCONFIG"
    export ESCONFIG="${1:-$(mktemp -d -t 'config.XXXX')}"
    echo "Moving configuration directory from $oldConfig to $ESCONFIG"

    # Move configuration files to the new configuration directory
    mv "$oldConfig"/* "$ESCONFIG"
    chown -R elasticsearch:elasticsearch "$ESCONFIG"
    assert_file_exist "$ESCONFIG/elasticsearch.yml"
}
