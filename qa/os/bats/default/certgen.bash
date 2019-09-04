#!/usr/bin/env bats

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

load $BATS_UTILS/utils.bash
load $BATS_UTILS/plugins.bash
load $BATS_UTILS/xpack.bash

# Description of the nodes instances
instances="/tmp/instances.yml"

# Destination for generated certificates
certificates="/tmp/certificates.zip"

setup() {
    if [ $BATS_TEST_NUMBER == 1 ]; then
        clean_before_test
    fi
}

DEFAULT_ARCHIVE_USER=elasticsearch
DEFAULT_ARCHIVE_ESHOME="/tmp/elasticsearch"
DEFAULT_ARCHIVE_UTILS=$BATS_UTILS/tar.bash

DEFAULT_PACKAGE_USER=root
DEFAULT_PACKAGE_ESHOME="/usr/share/elasticsearch"
DEFAULT_PACKAGE_UTILS=$BATS_UTILS/packages.bash

if [[ "$BATS_TEST_FILENAME" =~ 40_tar_certgen.bats$ ]]; then
    GROUP='TAR CERTGEN'

    MASTER_USER=$DEFAULT_ARCHIVE_USER
    MASTER_GROUP=$DEFAULT_ARCHIVE_USER
    MASTER_DPERMS=755
    MASTER_HOME=$DEFAULT_ARCHIVE_ESHOME
    MASTER_UTILS=$DEFAULT_ARCHIVE_UTILS

    DATA_USER=$DEFAULT_PACKAGE_USER
    DATA_GROUP=elasticsearch
    DATA_DPERMS=2755
    DATA_HOME=$DEFAULT_PACKAGE_ESHOME
    DATA_UTILS=$DEFAULT_PACKAGE_UTILS

    install_master_node() {
	    install_node_using_archive
    }
    start_master_node() {
	    start_node_using_archive
    }
    install_data_node() {
	    install_node_using_package
    }
    start_data_node() {
	    start_node_using_package
    }
else
    if is_rpm; then
        GROUP='RPM CERTGEN'
    elif is_dpkg; then
        GROUP='DEB CERTGEN'
    fi

    MASTER_USER=$DEFAULT_PACKAGE_USER
    MASTER_GROUP=elasticsearch
    MASTER_DPERMS=2755
    MASTER_HOME=$DEFAULT_PACKAGE_ESHOME
    MASTER_UTILS=$DEFAULT_PACKAGE_UTILS

    DATA_USER=$DEFAULT_ARCHIVE_USER
    DATA_GROUP=$DEFAULT_ARCHIVE_USER
    DATA_DPERMS=755
    DATA_HOME=$DEFAULT_ARCHIVE_ESHOME
    DATA_UTILS=$DEFAULT_ARCHIVE_UTILS

    install_master_node() {
	    install_node_using_package
    }
    start_master_node() {
	    start_node_using_package
    }
    install_data_node() {
	    install_node_using_archive
    }
    start_data_node() {
	    start_node_using_archive
    }
fi

# Install a node with x-pack using the archive file
install_node_using_archive() {
    load $BATS_UTILS/tar.bash
    export ESHOME="$DEFAULT_ARCHIVE_ESHOME"
    export_elasticsearch_paths

    assert_file_not_exist "/home/elasticsearch"
    install_archive
    set_debug_logging
    verify_archive_installation
    assert_file_not_exist "/home/elasticsearch"

    export ESPLUGIN_COMMAND_USER=$DEFAULT_ARCHIVE_USER
    generate_trial_license
    verify_xpack_installation
}

# Starts a node installed using the archive
start_node_using_archive() {
    load $BATS_UTILS/tar.bash
    export ESHOME="$DEFAULT_ARCHIVE_ESHOME"
    export_elasticsearch_paths

    run sudo -u $DEFAULT_ARCHIVE_USER "$ESHOME/bin/elasticsearch" -d -p $ESHOME/elasticsearch.pid
    [ "$status" -eq "0" ] || {
	echo "Failed to start node using archive: $output"
	false
    }
}

# Install a node with x-pack using a package file
install_node_using_package() {
    load $BATS_UTILS/packages.bash
    export ESHOME="$DEFAULT_PACKAGE_ESHOME"
    export_elasticsearch_paths

    assert_file_not_exist "/home/elasticsearch"
    install_package
    set_debug_logging
    verify_package_installation

    export ESPLUGIN_COMMAND_USER=$DEFAULT_PACKAGE_USER
    generate_trial_license
    verify_xpack_installation
}

# Starts a node installed using a package
start_node_using_package() {
    if is_systemd; then
	run systemctl daemon-reload
        [ "$status" -eq 0 ]

        run sudo systemctl start elasticsearch.service
        [ "$status" -eq "0" ]

    elif is_sysvinit; then
        run sudo service elasticsearch start
        [ "$status" -eq "0" ]
    fi
}


@test "[$GROUP] install master node" {
    install_master_node
}

@test "[$GROUP] add bootstrap password" {
    load $MASTER_UTILS
    export ESHOME="$MASTER_HOME"
    export_elasticsearch_paths

    # For the sake of simplicity we use a bootstrap password in this test. The
    # alternative would be to start the master node, use
    # elasticsearch-setup-passwords and restart the node once ssl/tls is
    # configured. Or use elasticsearch-setup-passwords over HTTPS with the right
    # cacerts imported into a Java keystore.
    run sudo -E -u $MASTER_USER bash <<"NEW_PASS"
if [[ ! -f $ESCONFIG/elasticsearch.keystore ]]; then
    $ESHOME/bin/elasticsearch-keystore create
fi
echo "changeme" | $ESHOME/bin/elasticsearch-keystore add --stdin bootstrap.password
NEW_PASS
    [ "$status" -eq 0 ] || {
        echo "Expected elasticsearch-keystore tool exit code to be zero"
        echo "$output"
        false
    }
}

@test "[$GROUP] create instances file" {
    rm -f /tmp/instances.yml
    run sudo -E -u $MASTER_USER bash <<"CREATE_INSTANCES_FILE"
cat > /tmp/instances.yml <<- EOF
instances:
  - name: "node-master"
    ip:
      - "127.0.0.1"
  - name: "node-data"
    ip:
      - "127.0.0.1"
EOF
CREATE_INSTANCES_FILE

    [ "$status" -eq 0 ] || {
        echo "Failed to create instances file [$instances]: $output"
        false
    }
}

@test "[$GROUP] create certificates" {
    if [[ -f "$certificates" ]]; then
	    sudo rm -f "$certificates"
    fi

    run sudo -E -u $MASTER_USER "$MASTER_HOME/bin/elasticsearch-certgen" --in "$instances" --out "$certificates"
    [ "$status" -eq 0 ] || {
        echo "Expected elasticsearch-certgen tool exit code to be zero"
        echo "$output"
        false
    }

    echo "$output" | grep "Certificates written to $certificates"
    assert_file "$certificates" f $MASTER_USER $MASTER_USER 600
}

@test "[$GROUP] install certificates on master node" {
    load $MASTER_UTILS
    export ESHOME="$MASTER_HOME"
    export_elasticsearch_paths

    certs="$ESCONFIG/certs"
    if [[ -d "$certs" ]]; then
	    sudo rm -rf "$certs"
    fi

    run sudo -E -u $MASTER_USER "unzip" $certificates -d $certs
    [ "$status" -eq 0 ] || {
	    echo "Failed to unzip certificates in $certs: $output"
	    false
    }

    assert_file "$certs/ca/ca.key" f $MASTER_USER $MASTER_GROUP 644
    assert_file "$certs/ca/ca.crt" f $MASTER_USER $MASTER_GROUP 644

    assert_file "$certs/node-master" d $MASTER_USER $MASTER_GROUP $MASTER_DPERMS
    assert_file "$certs/node-master/node-master.key" f $MASTER_USER $MASTER_GROUP 644
    assert_file "$certs/node-master/node-master.crt" f $MASTER_USER $MASTER_GROUP 644

    assert_file "$certs/node-data" d $MASTER_USER $MASTER_GROUP $MASTER_DPERMS
    assert_file "$certs/node-data/node-data.key" f $MASTER_USER $MASTER_GROUP 644
    assert_file "$certs/node-data/node-data.crt" f $MASTER_USER $MASTER_GROUP 644
}

@test "[$GROUP] update master node settings" {
    load $MASTER_UTILS
    export ESHOME="$MASTER_HOME"
    export_elasticsearch_paths

    run sudo -E -u $MASTER_USER bash <<"MASTER_SETTINGS"
cat >> $ESCONFIG/elasticsearch.yml <<- EOF
node.name: "node-master"
node.master: true
node.data: false
discovery.seed_hosts: ["127.0.0.1:9301"]
cluster.initial_master_nodes: ["node-master"]

xpack.security.transport.ssl.key: $ESCONFIG/certs/node-master/node-master.key
xpack.security.transport.ssl.certificate: $ESCONFIG/certs/node-master/node-master.crt
xpack.security.transport.ssl.certificate_authorities: ["$ESCONFIG/certs/ca/ca.crt"]
xpack.security.http.ssl.key: $ESCONFIG/certs/node-master/node-master.key
xpack.security.http.ssl.certificate: $ESCONFIG/certs/node-master/node-master.crt
xpack.security.http.ssl.certificate_authorities: ["$ESCONFIG/certs/ca/ca.crt"]

xpack.security.transport.ssl.enabled: true
transport.port: 9300

xpack.security.http.ssl.enabled: true
http.port: 9200

EOF
MASTER_SETTINGS

    start_master_node
        wait_for_xpack 127.0.0.1 9200
}

@test "[$GROUP] test connection to master node using HTTPS" {
    load $MASTER_UTILS
    export ESHOME="$MASTER_HOME"
    export_elasticsearch_paths

    run sudo -E -u $MASTER_USER curl -u "elastic:changeme" --cacert "$ESCONFIG/certs/ca/ca.crt" -XGET "https://127.0.0.1:9200"
    [ "$status" -eq 0 ] || {
	    echo "Failed to connect to master node using HTTPS:"
	    echo "$output"
	    debug_collect_logs
	    false
    }
    echo "$output" | grep "node-master"
}

@test "[$GROUP] install data node" {
    install_data_node
}

@test "[$GROUP] install certificates on data node" {
    load $DATA_UTILS
    export ESHOME="$DATA_HOME"
    export_elasticsearch_paths

    sudo chown $DATA_USER:$DATA_USER "$certificates"
    [ -f "$certificates" ] || {
	    echo "Could not find certificates: $certificates"
	    false
    }

    certs="$ESCONFIG/certs"
    if [[ -d "$certs" ]]; then
	    sudo rm -rf "$certs"
    fi

    run sudo -E -u $DATA_USER "unzip" $certificates -d $certs
    [ "$status" -eq 0 ] || {
	    echo "Failed to unzip certificates in $certs: $output"
	    false
    }

    assert_file "$certs/ca" d $DATA_USER $DATA_GROUP
    assert_file "$certs/ca/ca.key" f $DATA_USER $DATA_GROUP 644
    assert_file "$certs/ca/ca.crt" f $DATA_USER $DATA_GROUP 644

    assert_file "$certs/node-master" d $DATA_USER $DATA_GROUP
    assert_file "$certs/node-master/node-master.key" f $DATA_USER $DATA_GROUP 644
    assert_file "$certs/node-master/node-master.crt" f $DATA_USER $DATA_GROUP 644

    assert_file "$certs/node-data" d $DATA_USER $DATA_GROUP
    assert_file "$certs/node-data/node-data.key" f $DATA_USER $DATA_GROUP 644
    assert_file "$certs/node-data/node-data.crt" f $DATA_USER $DATA_GROUP 644
}

@test "[$GROUP] update data node settings" {
    load $DATA_UTILS
    export ESHOME="$DATA_HOME"
    export_elasticsearch_paths

    run sudo -E -u $DATA_USER bash <<"DATA_SETTINGS"
cat >> $ESCONFIG/elasticsearch.yml <<- EOF
node.name: "node-data"
node.master: false
node.data: true
discovery.seed_hosts: ["127.0.0.1:9300"]

xpack.security.transport.ssl.key: $ESCONFIG/certs/node-data/node-data.key
xpack.security.transport.ssl.certificate: $ESCONFIG/certs/node-data/node-data.crt
xpack.security.transport.ssl.certificate_authorities: ["$ESCONFIG/certs/ca/ca.crt"]
xpack.security.http.ssl.key: $ESCONFIG/certs/node-data/node-data.key
xpack.security.http.ssl.certificate: $ESCONFIG/certs//node-data/node-data.crt
xpack.security.http.ssl.certificate_authorities: ["$ESCONFIG/certs/ca/ca.crt"]

xpack.security.transport.ssl.enabled: true
transport.port: 9301

xpack.security.http.ssl.enabled: true
http.port: 9201

EOF
DATA_SETTINGS

    start_data_node
    wait_for_xpack 127.0.0.1 9201
}

@test "[$GROUP] test connection to data node using HTTPS" {
    load $DATA_UTILS
    export ESHOME="$DATA_HOME"
    export_elasticsearch_paths

    run sudo -E -u $DATA_USER curl --cacert "$ESCONFIG/certs/ca/ca.crt" -XGET "https://127.0.0.1:9201"
    [ "$status" -eq 0 ] || {
	    echo "Failed to connect to data node using HTTPS:"
	    echo "$output"
	    false
    }
    echo "$output" | grep "missing authentication credentials"
}

@test "[$GROUP] test node to node communication" {
    load $MASTER_UTILS
    export ESHOME="$MASTER_HOME"
    export_elasticsearch_paths

    testIndex=$(sudo curl -u "elastic:changeme" \
        -H "Content-Type: application/json" \
        --cacert "$ESCONFIG/certs/ca/ca.crt" \
        -XPOST "https://127.0.0.1:9200/books/book/0?refresh" \
        -d '{"title": "Elasticsearch The Definitive Guide"}')

    debug_collect_logs
    echo "$testIndex" | grep '"result":"created"'

    masterSettings=$(sudo curl -u "elastic:changeme" \
        -H "Content-Type: application/json" \
        --cacert "$ESCONFIG/certs/ca/ca.crt" \
        -XGET "https://127.0.0.1:9200/_nodes/node-master?filter_path=nodes.*.settings.xpack,nodes.*.settings.http.type,nodes.*.settings.transport.type")

    echo "$masterSettings" | grep '"http":{"ssl":{"enabled":"true"}'
    echo "$masterSettings" | grep '"http":{"type":"security4"}'
    echo "$masterSettings" | grep '"transport":{"ssl":{"enabled":"true"}'
    echo "$masterSettings" | grep '"transport":{"type":"security4"}'

    load $DATA_UTILS
    export ESHOME="$DATA_HOME"
    export_elasticsearch_paths

    dataSettings=$(curl -u "elastic:changeme" \
        -H "Content-Type: application/json" \
        --cacert "$ESCONFIG/certs/ca/ca.crt" \
        -XGET "https://127.0.0.1:9200/_nodes/node-data?filter_path=nodes.*.settings.xpack,nodes.*.settings.http.type,nodes.*.settings.transport.type")

    echo "$dataSettings" | grep '"http":{"ssl":{"enabled":"true"}'
    echo "$dataSettings" | grep '"http":{"type":"security4"}'
    echo "$dataSettings" | grep '"transport":{"ssl":{"enabled":"true"}'
    echo "$dataSettings" | grep '"transport":{"type":"security4"}'

    testSearch=$(curl -u "elastic:changeme" \
        -H "Content-Type: application/json" \
        --cacert "$ESCONFIG/certs/ca/ca.crt" \
        -XGET "https://127.0.0.1:9200/_search?q=title:guide")

    echo "$testSearch" | grep '"_index":"books"'
    echo "$testSearch" | grep '"_id":"0"'
}

@test "[$GROUP] exit code on failure" {
    run sudo -E -u $MASTER_USER "$MASTER_HOME/bin/elasticsearch-certgen" --not-a-valid-option
    [ "$status" -ne 0 ] || {
        echo "Expected elasticsearch-certgen tool exit code to be non-zero"
        echo "$output"
        false
    }
}

@test "[$GROUP] remove Elasticsearch" {
    # NOTE: this must be the last test, so that running oss tests does not already have the default distro still installed
    clean_before_test
}
