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

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoMinMasterNodes = false)
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE,org.elasticsearch.discovery.zen:TRACE")
public class MinimumMasterNodesIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false).build();
    }

    public void testSimpleMinimumMasterNodes() throws Exception {

        Settings settings = Settings.builder()
                .put("discovery.zen.minimum_master_nodes", 2)
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .build();

        logger.info("--> start first node");
        internalCluster().startNode(settings);

        logger.info("--> should be blocked, no master...");
        ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
        assertThat(state.nodes().getSize(), equalTo(1)); // verify that we still see the local node in the cluster state

        logger.info("--> start second node, cluster should be formed");
        internalCluster().startNode(settings);

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
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
            assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(100L));
        }

        internalCluster().stopCurrentMasterNode();
        awaitBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            return clusterState.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
        });
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
        // verify that both nodes are still in the cluster state but there is no master
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.nodes().getMasterNode(), equalTo(null));

        logger.info("--> starting the previous master node again...");
        internalCluster().startNode(settings);

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        ensureGreen();

        logger.info("--> verify we the data back after cluster reform");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }

        internalCluster().stopRandomNonMasterNode();
        assertBusy(() -> {
            ClusterState state1 = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertThat(state1.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
        });

        logger.info("--> starting the previous master node again...");
        internalCluster().startNode(settings);

        ensureGreen();
        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));
        state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
        assertThat(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(2));
        assertThat(state.metaData().indices().containsKey("test"), equalTo(true));

        logger.info("Running Cluster Health");
        ensureGreen();

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }
    }

    public void testMultipleNodesShutdownNonMasterNodes() throws Exception {
        Settings settings = Settings.builder()
                .put("discovery.zen.minimum_master_nodes", 3)
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "1s")
                .put("discovery.initial_state_timeout", "500ms")
                .build();

        logger.info("--> start first 2 nodes");
        internalCluster().startNodes(2, settings);

        ClusterState state;

        assertBusy(() -> {
            for (Client client : clients()) {
                ClusterState state1 = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                assertThat(state1.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID), equalTo(true));
            }
        });

        logger.info("--> start two more nodes");
        internalCluster().startNodes(2, settings);

        ensureGreen();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("4").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(4));

        createIndex("test");
        NumShards numShards = getNumShards("test");
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        ensureGreen();
        // make sure that all shards recovered before trying to flush
        assertThat(client().admin().cluster().prepareHealth("test").setWaitForActiveShards(numShards.totalNumShards).execute().actionGet().isTimedOut(), equalTo(false));
        // flush for simpler debugging
        client().admin().indices().prepareFlush().execute().actionGet();

        refresh();
        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }

        internalCluster().stopRandomNonMasterNode();
        internalCluster().stopRandomNonMasterNode();

        logger.info("--> verify that there is no master anymore on remaining nodes");
        // spin here to wait till the state is set
        assertNoMasterBlockOnAllNodes();

        logger.info("--> start back the 2 nodes ");
        String[] newNodes = internalCluster().startNodes(2, settings).stream().toArray(String[]::new);

        internalCluster().validateClusterFormed();
        ensureGreen();

        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.nodes().getSize(), equalTo(4));
        // we prefer to elect up and running nodes
        assertThat(state.nodes().getMasterNodeId(), not(isOneOf(newNodes)));

        logger.info("--> verify we the data back");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 100);
        }
    }

    public void testDynamicUpdateMinimumMasterNodes() throws Exception {
        Settings settingsWithMinMaster1 = Settings.builder()
            .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "400ms")
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 1)
            .build();

        Settings settingsWithMinMaster2 = Settings.builder()
            .put(settingsWithMinMaster1).put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2)
            .build();

        logger.info("--> start two nodes and wait for them to form a cluster");
        internalCluster().startNodes(settingsWithMinMaster1, settingsWithMinMaster2);
        ensureClusterSizeConsistency();

        logger.info("--> setting minimum master node to 2");
        setMinimumMasterNodes(2);

        // make sure it has been processed on all nodes (master node spawns a secondary cluster state update task)
        for (Client client : internalCluster().getClients()) {
            assertThat(client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setLocal(true).get().isTimedOut(),
                    equalTo(false));
        }

        logger.info("--> stopping a node");
        internalCluster().stopRandomDataNode();
        logger.info("--> verifying min master node has effect");
        assertNoMasterBlockOnAllNodes();

        logger.info("--> bringing another node up");
        internalCluster().startNode(settingsWithMinMaster2);
        ensureClusterSizeConsistency();
    }

    private void assertNoMasterBlockOnAllNodes() throws InterruptedException {
        Predicate<Client> hasNoMasterBlock = client -> {
            ClusterState state = client.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
        };
        assertTrue(awaitBusy(
                        () -> {
                            boolean success = true;
                            for (Client client : internalCluster().getClients()) {
                                boolean clientHasNoMasterBlock = hasNoMasterBlock.test(client);
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Checking for NO_MASTER_BLOCK on client: {} NO_MASTER_BLOCK: [{}]", client, clientHasNoMasterBlock);
                                }
                                success &= clientHasNoMasterBlock;
                            }
                            return success;
                        },
                        20,
                        TimeUnit.SECONDS
                )
        );
    }

    public void testCanNotBringClusterDown() throws ExecutionException, InterruptedException {
        int nodeCount = scaledRandomIntBetween(1, 5);
        Settings.Builder settings = Settings.builder()
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "200ms")
                .put("discovery.initial_state_timeout", "500ms");

        // set an initial value which is at least quorum to avoid split brains during initial startup
        int initialMinMasterNodes = randomIntBetween(nodeCount / 2 + 1, nodeCount);
        settings.put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), initialMinMasterNodes);


        logger.info("--> starting [{}] nodes. min_master_nodes set to [{}]", nodeCount, initialMinMasterNodes);
        internalCluster().startNodes(nodeCount, settings.build());

        logger.info("--> waiting for nodes to join");
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodeCount)).get().isTimedOut());

        int updateCount = randomIntBetween(1, nodeCount);

        logger.info("--> updating [{}] to [{}]", ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), updateCount);
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), updateCount)));

        logger.info("--> verifying no node left and master is up");
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodeCount)).get().isTimedOut());

        updateCount = nodeCount + randomIntBetween(1, 2000);
        logger.info("--> trying to updating [{}] to [{}]", ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), updateCount);
        try {
            client().admin().cluster().prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), updateCount));
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "cannot set discovery.zen.minimum_master_nodes to more than the current master nodes count [" +updateCount+ "]");
        }

        logger.info("--> verifying no node left and master is up");
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodeCount)).get().isTimedOut());
    }

    public void testCanNotPublishWithoutMinMastNodes() throws Exception {
        Settings settings = Settings.builder()
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "200ms")
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2)
                .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "100ms") // speed things up
                .build();
        internalCluster().startNodes(3, settings);
        ensureGreen(); // ensure cluster state is recovered before we disrupt things

        final String master = internalCluster().getMasterName();
        Set<String> otherNodes = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        otherNodes.remove(master);
        NetworkDisruption partition = new NetworkDisruption(
            new TwoPartitions(Collections.singleton(master), otherNodes),
            new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(partition);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        logger.debug("--> submitting for cluster state to be rejected");
        final ClusterService masterClusterService = internalCluster().clusterService(master);
        masterClusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                logger.debug("--> starting the disruption, preventing cluster state publishing");
                partition.startDisrupting();
                MetaData.Builder metaData = MetaData.builder(currentState.metaData()).persistentSettings(
                        Settings.builder().put(currentState.metaData().persistentSettings()).put("_SHOULD_NOT_BE_THERE_", true).build()
                );
                return ClusterState.builder(currentState).metaData(metaData).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                failure.set(e);
                latch.countDown();
            }
        });

        logger.debug("--> waiting for cluster state to be processed/rejected");
        latch.await();

        assertThat(failure.get(), instanceOf(Discovery.FailedToCommitClusterStateException.class));
        assertBusy(() -> assertThat(masterClusterService.state().nodes().getMasterNode(), nullValue()));

        partition.stopDisrupting();

        logger.debug("--> waiting for cluster to heal");
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes("3").setWaitForEvents(Priority.LANGUID));

        for (String node : internalCluster().getNodeNames()) {
            Settings nodeSetting = internalCluster().clusterService(node).state().metaData().settings();
            assertThat(node + " processed the cluster state despite of a min master node violation", nodeSetting.get("_SHOULD_NOT_BE_THERE_"), nullValue());
        }

    }
}
