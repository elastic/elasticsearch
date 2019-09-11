
#!/usr/bin/env bats

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

load $BATS_UTILS/utils.bash
load $BATS_UTILS/plugins.bash
load $BATS_UTILS/xpack.bash

setup() {
    if [ $BATS_TEST_NUMBER == 1 ]; then
        export PACKAGE_NAME="elasticsearch"
        clean_before_test
        install
        set_debug_logging

        generate_trial_license
        verify_xpack_installation
    fi
}


if [[ "$BATS_TEST_FILENAME" =~ 30_tar_setup_passwords.bats$ ]]; then
    load $BATS_UTILS/tar.bash
    GROUP='TAR SETUP PASSWORD'
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
        GROUP='RPM SETUP PASSWORD'
    elif is_dpkg; then
        GROUP='DEB SETUP PASSWORD'
    fi
    export_elasticsearch_paths
    export ESPLUGIN_COMMAND_USER=root
    install() {
        install_package
        verify_package_installation
    }
fi

@test "[$GROUP] test auto generated passwords" {
    assert_file_not_exist "/home/elasticsearch"
    run_elasticsearch_service 0
    wait_for_xpack

    run sudo -E -u $ESPLUGIN_COMMAND_USER bash <<"SETUP_AUTO"
echo 'y' | $ESHOME/bin/elasticsearch-setup-passwords auto
SETUP_AUTO
    echo "$output" > /tmp/setup-passwords-output
    [ "$status" -eq 0 ] || {
        echo "Expected x-pack elasticsearch-setup-passwords tool exit code to be zero but got $status"
        cat /tmp/setup-passwords-output
        debug_collect_logs
        false
    }

    curl -s -XGET localhost:9200 | grep "missing authentication credentials for REST"

    # Disable bash history expansion because passwords can contain "!"
    set +H

    users=( elastic kibana logstash_system )
    for user in "${users[@]}"; do
        grep "Changed password for user $user" /tmp/setup-passwords-output || {
            echo "Expected x-pack elasticsearch-setup-passwords tool to change password for user [$user]:"
            cat /tmp/setup-passwords-output
            false
        }

        password=$(grep "PASSWORD $user = " /tmp/setup-passwords-output | sed "s/PASSWORD $user = //")
        curl -u "$user:$password" -XGET localhost:9200 | grep "You Know, for Search"

        basic=$(echo -n "$user:$password" | base64)
        curl -H "Authorization: Basic $basic" -XGET localhost:9200 | grep "You Know, for Search"
    done
    set -H

    stop_elasticsearch_service
    assert_file_not_exist "/home/elasticsearch"
}

@test "[$GROUP] remove Elasticsearch" {
    # NOTE: this must be the last test, so that running oss tests does not already have the default distro still installed
    clean_before_test
}
