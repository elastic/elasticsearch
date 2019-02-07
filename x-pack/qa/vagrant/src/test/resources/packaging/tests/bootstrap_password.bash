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

if [[ "$BATS_TEST_FILENAME" =~ 20_tar_bootstrap_password.bats$ ]]; then
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
    if [[ -f /tmp/bootstrap.password ]]; then
	    sudo rm -f /tmp/bootstrap.password
    fi

    run sudo -E -u $ESPLUGIN_COMMAND_USER bash <<"NEW_PASS"
if [[ ! -f $ESCONFIG/elasticsearch.keystore ]]; then
    $ESHOME/bin/elasticsearch-keystore create
fi
cat /dev/urandom | tr -dc "[a-zA-Z0-9]" | fold -w 20 | head -n 1 > /tmp/bootstrap.password
cat /tmp/bootstrap.password | $ESHOME/bin/elasticsearch-keystore add --stdin bootstrap.password
NEW_PASS
    [ "$status" -eq 0 ] || {
        echo "Expected elasticsearch-keystore tool exit code to be zero but got [$status]"
        echo "$output"
        false
    }
    assert_file_exist "/tmp/bootstrap.password"
}

@test "[$GROUP] test bootstrap.password is in setting list" {
    run sudo -E -u $ESPLUGIN_COMMAND_USER bash <<"NODE_SETTINGS"
cat >> $ESCONFIG/elasticsearch.yml <<- EOF
network.host: 127.0.0.1
http.port: 9200
EOF
NODE_SETTINGS

    run_elasticsearch_service 0
    wait_for_xpack 127.0.0.1 9200

    sudo -E -u $ESPLUGIN_COMMAND_USER "$ESHOME/bin/elasticsearch-keystore" list | grep "bootstrap.password"

    password=$(cat /tmp/bootstrap.password)
    clusterHealth=$(sudo curl -u "elastic:$password" -H "Content-Type: application/json" \
	                          -XGET "http://127.0.0.1:9200/_cluster/health?wait_for_status=green&timeout=30s")
    echo "$clusterHealth" | grep '"status":"green"' || {
        echo "Expected cluster health to be green but got:"
        echo "$clusterHealth"
        false
    }
}

@test "[$GROUP] test auto generated passwords with modified bootstrap.password" {
    if [[ -f /tmp/setup-passwords-output-with-bootstrap ]]; then
	    sudo rm -f /tmp/setup-passwords-output-with-bootstrap
    fi

    run sudo -E -u $ESPLUGIN_COMMAND_USER bash <<"SETUP_OK"
echo 'y' | $ESHOME/bin/elasticsearch-setup-passwords auto
SETUP_OK
    echo "$output" > /tmp/setup-passwords-output-with-bootstrap
    [ "$status" -eq 0 ] || {
        echo "Expected x-pack elasticsearch-setup-passwords tool exit code to be zero but got [$status]"
        cat /tmp/setup-passwords-output-with-bootstrap
        debug_collect_logs
        false
    }

    curl -s -XGET 'http://127.0.0.1:9200' | grep "missing authentication credentials for REST"

    # Disable bash history expansion because passwords can contain "!"
    set +H

    users=( elastic kibana logstash_system )
    for user in "${users[@]}"; do
        grep "Changed password for user $user" /tmp/setup-passwords-output-with-bootstrap || {
            echo "Expected x-pack elasticsearch-setup-passwords tool to change password for user [$user]:"
            cat /tmp/setup-passwords-output-with-bootstrap
            false
        }

        password=$(grep "PASSWORD $user = " /tmp/setup-passwords-output-with-bootstrap | sed "s/PASSWORD $user = //")
        curl -u "$user:$password" -XGET 'http://127.0.0.1:9200' | grep "You Know, for Search"

        basic=$(echo -n "$user:$password" | base64)
        curl -H "Authorization: Basic $basic" -XGET 'http://127.0.0.1:9200' | grep "You Know, for Search"
    done
    set -H
}

@test "[$GROUP] test elasticsearch-sql-cli" {
    password=$(grep "PASSWORD elastic = " /tmp/setup-passwords-output-with-bootstrap | sed "s/PASSWORD elastic = //")
    curl -s -u "elastic:$password" -H "Content-Type: application/json" -XPUT 'localhost:9200/library/book/1?refresh&pretty' -d'{
            "name": "Ender'"'"'s Game",
            "author": "Orson Scott Card",
            "release_date": "1985-06-01",
            "page_count": 324
        }'

    password=$(grep "PASSWORD elastic = " /tmp/setup-passwords-output-with-bootstrap | sed "s/PASSWORD elastic = //")

    run $ESHOME/bin/elasticsearch-sql-cli --debug "http://elastic@127.0.0.1:9200" <<SQL
$password
SELECT * FROM library;
SQL
    [ "$status" -eq 0 ] || {
        echo "SQL cli failed:\n$output"
        false
    }
    [[ "$output" == *"Card"* ]] || {
        echo "Failed to find author [Card] in library:$output"
        false
    }
}

@test "[$GROUP] test elasticsearch-sql-cli when user refuses password" {
    # Run with empty stdin
    run $ESHOME/bin/elasticsearch-sql-cli --debug "http://elastic@127.0.0.1:9200" <<SQL
SQL
    [ "$status" -eq 77 ] || { #NOPERM
        echo "SQL cli failed:\n$output"
        false
    }
    [[ "$output" == *"password required"* ]] || {
        echo "Failed to find author [password required] in error:$output"
        false
    }
}

@test "[$GROUP] stop Elasticsearch" {
    stop_elasticsearch_service
}
