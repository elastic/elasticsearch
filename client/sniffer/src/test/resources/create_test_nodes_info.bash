#!/bin/bash

# Recreates the v_nodes_http.json files in this directory. This is
# meant to be an "every once in a while" thing that we do only when
# we want to add a new version of Elasticsearch or configure the
# nodes differently. That is why we don't do this in gradle. It also
# allows us to play fast and loose with error handling. If something
# goes wrong you have to manually clean up which is good because it
# leaves around the kinds of things that we need to debug the failure.

# I built this file so the next time I have to regenerate these
# v_nodes_http.json files I won't have to reconfigure Elasticsearch
# from scratch. While I was at it I took the time to make sure that
# when we do rebuild the files they don't jump around too much. That
# way the diffs are smaller.

set -e

script_path="$( cd "$(dirname "$0")" ; pwd -P )"
work=$(mktemp -d)
pushd ${work} >> /dev/null
echo Working in ${work}

wget https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.0.0/elasticsearch-2.0.0.tar.gz
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.0.0.tar.gz
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.0.0.tar.gz
sha1sum -c - << __SHAs
e369d8579bd3a2e8b5344278d5043f19f14cac88 elasticsearch-2.0.0.tar.gz
d25f6547bccec9f0b5ea7583815f96a6f50849e0 elasticsearch-5.0.0.tar.gz
__SHAs
sha512sum -c - << __SHAs
25bb622d2fc557d8b8eded634a9b333766f7b58e701359e1bcfafee390776eb323cb7ea7a5e02e8803e25d8b1d3aabec0ec1b0cf492d0bab5689686fe440181c elasticsearch-6.0.0.tar.gz
__SHAs


function do_version() {
    local version=$1
    local nodes='m1 m2 m3 d1 d2 d3 c1 c2'
    rm -rf ${version}
    mkdir -p ${version}
    pushd ${version} >> /dev/null

    tar xf ../elasticsearch-${version}.tar.gz
    local http_port=9200
    for node in ${nodes}; do
        mkdir ${node}
        cp -r elasticsearch-${version}/* ${node}
        local master=$([[ "$node" =~ ^m.* ]] && echo true || echo false)
        local data=$([[ "$node" =~ ^d.* ]] && echo true || echo false)
        # m2 is always master and data for these test just so we have a node like that
        data=$([[ "$node" == 'm2' ]] && echo true || echo ${data})
        local attr=$([ ${version} == '2.0.0' ] && echo '' || echo '.attr')
        local transport_port=$((http_port+100))

        cat >> ${node}/config/elasticsearch.yml << __ES_YML
node.name:          ${node}
node.master:        ${master}
node.data:          ${data}
node${attr}.dummy:  everyone_has_me
node${attr}.number: ${node:1}
node${attr}.array:  [${node:0:1}, ${node:1}]
http.port:          ${http_port}
transport.tcp.port: ${transport_port}
discovery.zen.minimum_master_nodes: 3
discovery.zen.ping.unicast.hosts: ['localhost:9300','localhost:9301','localhost:9302']
__ES_YML

        if [ ${version} != '2.0.0' ]; then
            perl -pi -e 's/-Xm([sx]).+/-Xm${1}512m/g' ${node}/config/jvm.options
        fi

        echo "starting ${version}/${node}..."
        ${node}/bin/elasticsearch -d -p ${node}/pidfile

        ((http_port++))
    done

    echo "waiting for cluster to form"
    # got to wait for all the nodes
    until curl -s localhost:9200; do
        sleep .25
    done

    echo "waiting for all nodes to join"
    until [ $(echo ${nodes} | wc -w) -eq $(curl -s localhost:9200/_cat/nodes | wc -l) ]; do
        sleep .25
    done

    # jq sorts the nodes by their http host so the file doesn't jump around when we regenerate it
    curl -s localhost:9200/_nodes/http?pretty \
        | jq '[to_entries[] | ( select(.key == "nodes").value|to_entries|sort_by(.value.http.publish_address)|from_entries|{"key": "nodes", "value": .} ) // .] | from_entries' \
        > ${script_path}/${version}_nodes_http.json

    for node in ${nodes}; do
        echo "stopping ${version}/${node}..."
        kill $(cat ${node}/pidfile)
    done

    popd >> /dev/null
}

JAVA_HOME=$JAVA8_HOME do_version 2.0.0
JAVA_HOME=$JAVA8_HOME do_version 5.0.0
JAVA_HOME=$JAVA8_HOME do_version 6.0.0

popd >> /dev/null
rm -rf ${work}
