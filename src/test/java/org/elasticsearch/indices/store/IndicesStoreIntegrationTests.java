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

package org.elasticsearch.indices.store;

import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.SingleNodeDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndicesStoreIntegrationTests extends ElasticsearchIntegrationTest {
    private static final Settings SETTINGS = settingsBuilder().put("gateway.type", "local").build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                // by default this value is 1 sec in tests (30 sec in practice) but we adding disruption here
                // which is between 1 and 2 sec can cause each of the shard deletion requests to timeout.
                // to prevent this we are setting the timeout here to something highish ie. the default in practice
                .put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT, new TimeValue(30, TimeUnit.SECONDS))
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
                .build();
    }

    @Test
    @LuceneTestCase.Slow
    public void indexCleanup() throws Exception {
        final String masterNode = internalCluster().startNode(ImmutableSettings.builder().put(SETTINGS).put("node.data", false));
        final String node_1 = internalCluster().startNode(ImmutableSettings.builder().put(SETTINGS).put("node.master", false));
        final String node_2 = internalCluster().startNode(ImmutableSettings.builder().put(SETTINGS).put("node.master", false));
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        ImmutableSettings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen("test");

        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, "test")), equalTo(true));
        assertThat(shardDirectory(node_2, "test", 0).exists(), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, "test")), equalTo(true));

        logger.info("--> starting node server3");
        final String node_3 = internalCluster().startNode(ImmutableSettings.builder().put(SETTINGS).put("node.master", false));
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("4")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, "test")), equalTo(true));
        assertThat(shardDirectory(node_2, "test", 0).exists(), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, "test")), equalTo(true));
        assertThat(shardDirectory(node_3, "test", 0).exists(), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_3, "test")), equalTo(false));

        logger.info("--> move shard from node_1 to node_3, and wait for relocation to finish");

        if (randomBoolean()) { // sometimes add cluster-state delay to trigger observers in IndicesStore.ShardActiveRequestHandler
            SingleNodeDisruption disruption = new BlockClusterStateProcessing(node_3, getRandom());
            internalCluster().setDisruptionScheme(disruption);
            MockTransportService transportServiceNode3 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_3);
            CountDownLatch beginRelocationLatch = new CountDownLatch(1);
            CountDownLatch endRelocationLatch = new CountDownLatch(1);
            transportServiceNode3.addTracer(new ReclocationStartEndTracer(logger, beginRelocationLatch, endRelocationLatch));
            internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(new ShardId("test", 0), node_1, node_3)).get();
            // wait for relocation to start
            beginRelocationLatch.await();
            disruption.startDisrupting();
            // wait for relocation to finish
            endRelocationLatch.await();
            // wait a little so that cluster state observer is registered
            sleep(50);
            disruption.stopDisrupting();
        } else {
            internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(new ShardId("test", 0), node_1, node_3)).get();
        }
        clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        assertThat(waitForShardDeletion(node_1, "test", 0), equalTo(false));
        assertThat(waitForIndexDeletion(node_1, "test"), equalTo(false));
        assertThat(shardDirectory(node_2, "test", 0).exists(), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, "test")), equalTo(true));
        assertThat(shardDirectory(node_3, "test", 0).exists(), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_3, "test")), equalTo(true));

    }

    @Test
    public void shardsCleanup() throws Exception {
        final String node_1 = internalCluster().startNode(SETTINGS);
        final String node_2 = internalCluster().startNode(SETTINGS);
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        ImmutableSettings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen("test");

        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(shardDirectory(node_2, "test", 0).exists(), equalTo(true));

        logger.info("--> starting node server3");
        String node_3 = internalCluster().startNode(SETTINGS);
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("3")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        logger.info("--> making sure that shard is not allocated on server3");
        assertThat(waitForShardDeletion(node_3, "test", 0), equalTo(false));

        File server2Shard = shardDirectory(node_2, "test", 0);
        logger.info("--> stopping node " + node_2);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_2));

        logger.info("--> running cluster_health");
        clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForNodes("2")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());

        assertThat(server2Shard.exists(), equalTo(true));

        logger.info("--> making sure that shard and its replica exist on server1, server2 and server3");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(server2Shard.exists(), equalTo(true));
        assertThat(shardDirectory(node_3, "test", 0).exists(), equalTo(true));

        logger.info("--> starting node node_4");
        final String node_4 = internalCluster().startNode(SETTINGS);

        logger.info("--> running cluster_health");
        ensureGreen();

        logger.info("--> making sure that shard and its replica are allocated on server1 and server3 but not on server2");
        assertThat(shardDirectory(node_1, "test", 0).exists(), equalTo(true));
        assertThat(shardDirectory(node_3, "test", 0).exists(), equalTo(true));
        assertThat(waitForShardDeletion(node_4, "test", 0), equalTo(false));
    }


    @Test
    @TestLogging("cluster.service:TRACE")
    public void testShardActiveElsewhereDoesNotDeleteAnother() throws Exception {
        Future<String> masterFuture = internalCluster().startNodeAsync(
                ImmutableSettings.builder().put(SETTINGS).put("node.master", true, "node.data", false).build());
        Future<List<String>> nodesFutures = internalCluster().startNodesAsync(4,
                ImmutableSettings.builder().put(SETTINGS).put("node.master", false, "node.data", true).build());

        final String masterNode = masterFuture.get();
        final String node1 = nodesFutures.get().get(0);
        final String node2 = nodesFutures.get().get(1);
        final String node3 = nodesFutures.get().get(2);
        // we will use this later on, handy to start now to make sure it has a different data folder that node 1,2 &3
        final String node4 = nodesFutures.get().get(3);

        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(FilterAllocationDecider.INDEX_ROUTING_EXCLUDE_GROUP + "_name", node4)
        ));
        assertFalse(client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).setWaitForGreenStatus().setWaitForNodes("5").get().isTimedOut());

        // disable allocation to control the situation more easily
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "none")));

        logger.debug("--> shutting down two random nodes");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1, node2, node3));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1, node2, node3));

        logger.debug("--> verifying index is red");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        if (health.getStatus() != ClusterHealthStatus.RED) {
            logClusterState();
            fail("cluster didn't become red, despite of shutting 2 of 3 nodes");
        }

        logger.debug("--> allowing index to be assigned to node [{}]", node4);
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(
                ImmutableSettings.builder()
                        .put(FilterAllocationDecider.INDEX_ROUTING_EXCLUDE_GROUP + "_name", "NONE")));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "all")));

        logger.debug("--> waiting for shards to recover on [{}]", node4);
        // we have to do this in two steps as we now do async shard fetching before assigning, so the change to the
        // allocation filtering may not have immediate effect
        // TODO: we should add an easier to do this. It's too much of a song and dance..
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertTrue(internalCluster().getInstance(IndicesService.class, node4).hasIndex("test"));
            }
        });

        // wait for 4 active shards - we should have lost one shard
        assertFalse(client().admin().cluster().prepareHealth().setWaitForActiveShards(4).get().isTimedOut());

        // disable allocation again to control concurrency a bit and allow shard active to kick in before allocation
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "none")));

        logger.debug("--> starting the two old nodes back");

        internalCluster().startNodesAsync(2,
                ImmutableSettings.builder().put(SETTINGS).put("node.master", false, "node.data", true).build());

        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("5").get().isTimedOut());


        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "all")));

        logger.debug("--> waiting for the lost shard to be recovered");

        ensureGreen("test");

    }

    @Test
    public void testShardActiveElseWhere() throws Exception {
        boolean node1IsMasterEligible = randomBoolean();
        boolean node2IsMasterEligible = !node1IsMasterEligible || randomBoolean();
        Future<String> node_1_future = internalCluster().startNodeAsync(ImmutableSettings.builder().put(SETTINGS).put("node.master", node1IsMasterEligible).build());
        Future<String> node_2_future = internalCluster().startNodeAsync(ImmutableSettings.builder().put(SETTINGS).put("node.master", node2IsMasterEligible).build());
        final String node_1 = node_1_future.get();
        final String node_2 = node_2_future.get();
        final String node_1_id = internalCluster().getInstance(DiscoveryService.class, node_1).localNode().getId();
        final String node_2_id = internalCluster().getInstance(DiscoveryService.class, node_2).localNode().getId();

        logger.debug("node {} (node_1) is {}master eligible", node_1, node1IsMasterEligible ? "" : "not ");
        logger.debug("node {} (node_2) is {}master eligible", node_2, node2IsMasterEligible ? "" : "not ");
        logger.debug("node {} became master", internalCluster().getMasterName());
        final int numShards = scaledRandomIntBetween(2, 20);
        assertAcked(prepareCreate("test")
                        .setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards))
        );
        ensureGreen("test");

        waitNoPendingTasksOnAll();
        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();

        RoutingNode routingNode = stateResponse.getState().routingNodes().node(node_2_id);
        final int[] node2Shards = new int[routingNode.numberOfOwningShards()];
        int i = 0;
        for (MutableShardRouting mutableShardRouting : routingNode) {
            node2Shards[i] = mutableShardRouting.shardId().id();
            i++;
        }
        logger.info("Node 2 has shards: {}", Arrays.toString(node2Shards));
        final long shardVersions[] = new long[numShards];
        final int shardIds[] = new int[numShards];
        i = 0;
        for (ShardRouting shardRouting : stateResponse.getState().getRoutingTable().allShards("test")) {
            shardVersions[i] = shardRouting.version();
            shardIds[i] = shardRouting.getId();
            i++;
        }
        // disable relocations when we do this, to make sure the shards are not relocated from node2
        // due to rebalancing, and delete its content
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settingsBuilder().put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE, 0)).get();
        internalCluster().getInstance(ClusterService.class, node_2).submitStateUpdateTask("test", Priority.IMMEDIATE, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder("test");
                for (int i = 0; i < numShards; i++) {
                    indexRoutingTableBuilder.addIndexShard(
                            new IndexShardRoutingTable.Builder(new ShardId("test", i), false)
                                    .addShard(new ImmutableShardRouting("test", i, node_1_id, true, ShardRoutingState.STARTED, shardVersions[shardIds[i]]))
                                    .build()
                    );
                }
                return ClusterState.builder(currentState)
                        .routingTable(RoutingTable.builder().add(indexRoutingTableBuilder).build())
                        .build();
            }

            public boolean runOnlyOnMaster() {
                return false;
            }

            @Override
            public void onFailure(String source, Throwable t) {
            }
        });
        waitNoPendingTasksOnAll();
        logger.info("Checking if shards aren't removed");
        for (int shard : node2Shards) {
            assertTrue(waitForShardDeletion(node_2, "test", shard));
        }
    }


    private Path indexDirectory(String server, String index) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        return env.indexPaths(new Index(index))[0];
    }

    private File shardDirectory(String server, String index, int shard) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        File blah = env.shardLocations(new ShardId(index, shard))[0];
        return blah;
    }

    private boolean waitForShardDeletion(final String server, final String index, final int shard) throws InterruptedException {
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                return !shardDirectory(server, index, shard).exists();
            }
        });
        return shardDirectory(server, index, shard).exists();
    }

    private boolean waitForIndexDeletion(final String server, final String index) throws InterruptedException {
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return !Files.exists(indexDirectory(server, index));
            }
        });
        return Files.exists(indexDirectory(server, index));
    }

    /**
     * This Tracer can be used to signal start and end of a recovery.
     * This is used to test the following:
     * Whenever a node deletes a shard because it was relocated somewhere else, it first
     * checks if enough other copies are started somewhere else. The node sends a ShardActiveRequest
     * to the other nodes that should have a copy according to cluster state.
     * The nodes that receive this request check if the shard is in state STARTED in which case they
     * respond with "true". If they have the shard in POST_RECOVERY they register a cluster state
     * observer that checks at each update if the shard has moved to STARTED.
     * To test that this mechanism actually works, this can be triggered by blocking the cluster
     * state processing when a recover starts and only unblocking it shortly after the node receives
     * the ShardActiveRequest.
     */
    static class ReclocationStartEndTracer extends MockTransportService.Tracer {
        private final ESLogger logger;
        private final CountDownLatch beginRelocationLatch;
        private final CountDownLatch receivedShardExistsRequestLatch;

        ReclocationStartEndTracer(ESLogger logger, CountDownLatch beginRelocationLatch, CountDownLatch receivedShardExistsRequestLatch) {
            this.logger = logger;
            this.beginRelocationLatch = beginRelocationLatch;
            this.receivedShardExistsRequestLatch = receivedShardExistsRequestLatch;
        }

        @Override
        public void receivedRequest(long requestId, String action) {
            if (action.equals(IndicesStore.ACTION_SHARD_EXISTS)) {
                receivedShardExistsRequestLatch.countDown();
                logger.info("received: {}, relocation done", action);
            }
        }

        @Override
        public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            if (action.equals(RecoverySource.Actions.START_RECOVERY)) {
                logger.info("sent: {}, relocation starts", action);
                beginRelocationLatch.countDown();
            }
        }
    }
}
