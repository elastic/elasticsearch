/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.cluster;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.TEST, numDataNodes =0)
public class MinimumMasterNodesTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleMinimumMasterNodes() throws Exception {

        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .build();

        logger.info("--> start first node");
        internalCluster().startNode(settings);

        logger.info("--> should be blocked, no master...");
        ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
        assertThat(state.nodes().size(), equalTo(1)); // verify that we still see the local node in the cluster state

        logger.info("--> start second node, cluster should be formed");
        internalCluster().startNode(settings);

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(false));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForActiveShards(numShards.totalNumShards).execute().actionGet().getActiveShards(), equalTo(numShards.totalNumShards));
        // flush for simpler debugging
        flushAndRefresh();

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        internalCluster().stopCurrentMasterNode();
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState  state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            }
        });
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
        assertThat(state.nodes().size(), equalTo(1)); // verify that we still see the local node in the cluster state

        logger.info("--> starting the previous master node again...");
        internalCluster().startNode(settings);

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        internalCluster().stopRandomNonMasterNode();
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            }
        }), equalTo(true));

        logger.info("--> starting the previous master node again...");
        internalCluster().startNode(settings);

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        ensureGreen();

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }
    }

    @Test
    @TestLogging("cluster.service:TRACE,discovery:TRACE,indices.cluster:TRACE")
    public void multipleNodesShutdownNonMasterNodes() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.minimum_master_nodes", 3)
                .put("discovery.zen.ping_timeout", "1s")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .build();

        logger.info("--> start first 2 nodes");
        internalCluster().startNode(settings);
        internalCluster().startNode(settings);

        ClusterState state;

        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            }
        });
        
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            }
        });

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));

        logger.info("--> start two more nodes");
        internalCluster().startNodesAsync(2, settings).get();

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(4));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        // make sure that all shards recovered before trying to flush
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForActiveShards(numShards.totalNumShards).execute().actionGet().isTimedOut(), equalTo(false));
        // flush for simpler debugging
        client().admin().indices().prepareFlush().execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }

        internalCluster().stopRandomNonMasterNode();
        internalCluster().stopRandomNonMasterNode();

        logger.info("--> verify that there is no master anymore on remaining nodes");
        // spin here to wait till the state is set
        assertNoMasterBlockOnAllNodes();

        logger.info("--> start back the 2 nodes ");
        String[] newNodes = internalCluster().startNodesAsync(2, settings).get().toArray(Strings.EMPTY_ARRAY);

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().size(), equalTo(4));
        // we prefer to elect up and running nodes
        assertThat(state.nodes().masterNodeId(), not(isOneOf(newNodes)));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(100l));
        }
    }

    @Test
    public void dynamicUpdateMinimumMasterNodes() throws InterruptedException, IOException {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("discovery.zen.ping_timeout", "400ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put("gateway.type", "local")
                .build();

        logger.info("--> start 2 nodes");
        internalCluster().startNode(settings);
        internalCluster().startNode(settings);

        // wait until second node join the cluster
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> setting minimum master node to 2");
        setMinimumMasterNodes(2);

        // make sure it has been processed on all nodes (master node spawns a secondary cluster state update task)
        for (Client client : internalCluster()) {
            assertThat(client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true).get().isTimedOut(),
                    equalTo(false));
        }

        logger.info("--> stopping a node");
        internalCluster().stopRandomDataNode();
        logger.info("--> verifying min master node has effect");
        assertNoMasterBlockOnAllNodes();

        logger.info("--> bringing another node up");
        internalCluster().startNode(settingsBuilder().put(settings).put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 2).build());
        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
    }

    private void assertNoMasterBlockOnAllNodes() throws InterruptedException {
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                boolean success = true;
                for (Client client : internalCluster()) {
                    ClusterState state = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                    success &= state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Checking for NO_MASTER_BLOCK on client: {} NO_MASTER_BLOCK: [{}]", client, state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID));
                    }
                }
                return success;
            }
        }, 20, TimeUnit.SECONDS), equalTo(true));
    }
}
