/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.nodesinfo;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.nodesInfoRequest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class SimpleNodesInfoTests extends AbstractNodesTests {

    @AfterMethod
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testNodesInfos() {
        startNode("server1");
        startNode("server2", ImmutableSettings.settingsBuilder().put("discovery.zen.minimum_master_nodes", 2)); 
        /* Use minimum master nodes here to ensure we joined the cluster such that both servers see each other to execute the node info. */
        String server1NodeId = ((InternalNode) node("server1")).injector().getInstance(ClusterService.class).state().nodes().localNodeId();
        String server2NodeId = ((InternalNode) node("server2")).injector().getInstance(ClusterService.class).state().nodes().localNodeId();

        NodesInfoResponse response = client("server1").admin().cluster().prepareNodesInfo().execute().actionGet();
        assertThat(response.nodes().length, equalTo(2));
        assertThat(response.nodesMap().get(server1NodeId), notNullValue());
        assertThat(response.nodesMap().get(server2NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfoRequest()).actionGet();
        assertThat(response.nodes().length, equalTo(2));
        assertThat(response.nodesMap().get(server1NodeId), notNullValue());
        assertThat(response.nodesMap().get(server2NodeId), notNullValue());

        response = client("server1").admin().cluster().nodesInfo(nodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.nodes().length, equalTo(1));
        assertThat(response.nodesMap().get(server1NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.nodes().length, equalTo(1));
        assertThat(response.nodesMap().get(server1NodeId), notNullValue());

        response = client("server1").admin().cluster().nodesInfo(nodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.nodes().length, equalTo(1));
        assertThat(response.nodesMap().get(server2NodeId), notNullValue());

        response = client("server2").admin().cluster().nodesInfo(nodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.nodes().length, equalTo(1));
        assertThat(response.nodesMap().get(server2NodeId), notNullValue());
    }
}
