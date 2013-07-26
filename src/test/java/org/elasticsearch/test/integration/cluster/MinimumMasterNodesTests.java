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

package org.elasticsearch.test.integration.cluster;

import com.google.common.collect.Sets;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.After;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Set;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MinimumMasterNodesTests extends AbstractZenNodesTests {

    @After
    public void cleanAndCloseNodes() throws Exception {
        for (int i = 0; i < 10; i++) {
            if (node("node" + i) != null) {
                node("node" + i).stop();
                // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
                if (((InternalNode) node("node" + i)).injector().getInstance(NodeEnvironment.class).hasNodeFile()) {
                    ((InternalNode) node("node" + i)).injector().getInstance(Gateway.class).reset();
                }
            }
        }
        closeAllNodes();
    }

    @Test
    public void simpleMinimumMasterNodes() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local"));
        buildNode("node2", settingsBuilder().put("gateway.type", "local"));
        cleanAndCloseNodes();


        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .put("index.number_of_shards", 1)
                .build();

        logger.info("--> start first node");
        startNode("node1", settings);

        logger.info("--> should be blocked, no master...");
        ClusterState state = client("node1").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        logger.info("--> start second node, cluster should be formed");
        startNode("node2", settings);

        ClusterHealthResponse clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client("node1").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));
        state = client("node2").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));

        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(false));

        client("node1").admin().indices().prepareCreate("test").execute().actionGet();
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client("node1").prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(client("node1").admin().cluster().prepareHealth("test").setWaitForActiveShards(2).execute().actionGet().getActiveShards(), equalTo(2));
        // flush for simpler debugging
        client("node1").admin().indices().prepareFlush().execute().actionGet();

        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        String masterNodeName = state.nodes().masterNode().name();
        String nonMasterNodeName = masterNodeName.equals("node1") ? "node2" : "node1";
        logger.info("--> closing master node {}", masterNodeName);
        closeNode(masterNodeName);

        long time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - time) < TimeValue.timeValueSeconds(5).millis()) {
            state = client(nonMasterNodeName).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            if (state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK)) {
                break;
            }
        }
        state = client(nonMasterNodeName).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        logger.info("--> starting the previous master node again...");
        startNode(masterNodeName, settings);

        clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client("node1").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));
        state = client("node2").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));

        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        masterNodeName = state.nodes().masterNode().name();
        nonMasterNodeName = masterNodeName.equals("node1") ? "node2" : "node1";
        logger.info("--> closing non master node {}", nonMasterNodeName);
        closeNode(nonMasterNodeName);

        time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - time) < TimeValue.timeValueSeconds(5).millis()) {
            state = client(masterNodeName).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            if (state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK)) {
                break;
            }
        }
        state = client(masterNodeName).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        logger.info("--> starting the previous master node again...");
        startNode(nonMasterNodeName, settings);

        clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client("node1").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));
        state = client("node2").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(false));

        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }
    }

    @Test
    public void multipleNodesShutdownNonMasterNodes() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local"));
        buildNode("node2", settingsBuilder().put("gateway.type", "local"));
        buildNode("node3", settingsBuilder().put("gateway.type", "local"));
        buildNode("node4", settingsBuilder().put("gateway.type", "local"));
        cleanAndCloseNodes();


        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 3)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .build();

        logger.info("--> start first 2 nodes");
        startNode("node1", settings);
        startNode("node2", settings);

        ClusterState state;

        long time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - time) < TimeValue.timeValueSeconds(5).millis()) {
            state = client("node1").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            if (state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK)) {
                break;
            }
        }
        time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - time) < TimeValue.timeValueSeconds(5).millis()) {
            state = client("node2").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            if (state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK)) {
                break;
            }
        }

        state = client("node1").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));
        state = client("node2").admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        logger.info("--> start two more nodes");
        startNode("node3", settings);
        startNode("node4", settings);

        ClusterHealthResponse clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(4));
        String masterNode = state.nodes().masterNode().name();
        LinkedList<String> nonMasterNodes = new LinkedList<String>();
        for (DiscoveryNode node : state.nodes()) {
            if (!node.name().equals(masterNode)) {
                nonMasterNodes.add(node.name());
            }
        }

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client("node1").prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(client("node1").admin().cluster().prepareHealth("test").setWaitForActiveShards(10).execute().actionGet().isTimedOut(), equalTo(false));
        // flush for simpler debugging
        client("node1").admin().indices().prepareFlush().execute().actionGet();

        client("node1").admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        Set<String> nodesToShutdown = Sets.newHashSet();
        nodesToShutdown.add(nonMasterNodes.removeLast());
        nodesToShutdown.add(nonMasterNodes.removeLast());
        logger.info("--> shutting down two master nodes {}", nodesToShutdown);
        for (String nodeToShutdown : nodesToShutdown) {
            closeNode(nodeToShutdown);
        }

        String lastNonMasterNodeUp = nonMasterNodes.removeLast();
        logger.info("--> verify that there is no master anymore on remaining nodes");
        // spin here to wait till the state is set
        time = System.currentTimeMillis();
        while ((System.currentTimeMillis() - time) < TimeValue.timeValueSeconds(20).millis()) {
            state = client(masterNode).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            boolean firstNoMasterLock = state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK);
            state = client(lastNonMasterNodeUp).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            boolean secondNoMasterLock = state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK);
            if (firstNoMasterLock && secondNoMasterLock) {
                break;
            }
        }
        state = client(masterNode).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));
        state = client(lastNonMasterNodeUp).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK), equalTo(true));

        logger.info("--> start back the nodes {}", nodesToShutdown);
        for (String nodeToShutdown : nodesToShutdown) {
            startNode(nodeToShutdown, settings);
        }

        clusterHealthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(4));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }
    }
}
