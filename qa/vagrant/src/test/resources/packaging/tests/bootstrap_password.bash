#!/usr/bin/env bats

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

load $BATS_UTILS/utils.bash
load $BATS_UTILS/plugins.bash
load $BATS_UTILS/xpack.bash

setup() {
    if [ $BATS_TEST_NUMBER == 1 ]; then
        clean_before_test
        install

        install_and_check_plugin x pack x-pack-*.jar
        verify_xpack_installation
    fi
}

if [[ "$BATS_TEST_FILENAME" =~ 40_tar_bootstrap_password.bats$ ]]; then
    load $BATS_UTILS/tar.bash
    GROUP='TAR BOOTSTRAP PASSWORD'
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
        GROUP='RPM BOOTSTRAP PASSWORD'
    elif is_dpkg; then
        GROUP='DEB BOOTSTRAP PASSWORD'
    fi
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=root
    install() {
        install_package
        verify_package_installation
    }
fi

@test "[$GROUP] add bootstrap.password setting" {
    run sudo -E -u $ESPLUGIN_COMMAND_USER sh <<"NEW_PASS"
cat /dev/urandom | tr -dc "[a-zA-Z0-9]" | fold -w 20 | head -n 1 > /tmp/bootstrap.password
cat /tmp/bootstrap.password | $ESHOME/bin/elasticsearch-keystore add --stdin bootstrap.password
NEW_PASS
    [ "$status" -eq 0 ] || {
        echo "Expected elasticsearch-keystore tool exit code to be zero"
        echo "$output"
        false
    }
}

@test "[$GROUP] test bootstrap.password is in setting list" {
    run_elasticsearch_service 0
    wait_for_xpack

    sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/elasticsearch-keystore" list | grep "bootstrap.password"

    password=$(cat /tmp/bootstrap.password)
    curl -u "elastic:$password" -XGET localhost:9200 | grep "You Know, for Search"
}

@test "[$GROUP] test auto generated passwords with modified bootstrap.password" {
    run sudo -E -u $ESPLUGIN_COMMAND_USER sh <<"SETUP_OK"
echo 'y' | $ESHOME/bin/x-pack/setup-passwords auto
SETUP_OK
    echo "$output" > /tmp/setup-passwords-output-with-bootstrap
    [ "$status" -eq 0 ] || {
        echo "Expected x-pack setup-passwords tool exit code to be zero"
        cat /tmp/setup-passwords-output-with-bootstrap
        false
    }

    curl -s -XGET localhost:9200 | grep "missing authentication token for REST"

    # Disable bash history expansion because passwords can contain "!"
    set +H

    users=( elastic kibana logstash_system )
    for user in "${users[@]}"; do
        grep "Changed password for user $user" /tmp/setup-passwords-output-with-bootstrap || {
            echo "Expected x-pack setup-passwords tool to change password for user [$user]:"
            cat /tmp/setup-passwords-output-with-bootstrap
            false
        }

        password=$(grep "PASSWORD $user = " /tmp/setup-passwords-output-with-bootstrap | sed "s/PASSWORD $user = //")
        curl -u "$user:$password" -XGET localhost:9200 | grep "You Know, for Search"

        basic=$(echo -n "$user:$password" | base64)
        curl -H "Authorization: Basic $basic" -XGET localhost:9200 | grep "You Know, for Search"
    done
    set -H

    stop_elasticsearch_service
}
