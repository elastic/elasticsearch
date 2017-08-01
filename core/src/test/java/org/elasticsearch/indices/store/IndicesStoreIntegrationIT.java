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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndicesStoreIntegrationIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) { // simplify this and only use a single data path
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(Environment.PATH_DATA_SETTING.getKey(), "")
                // by default this value is 1 sec in tests (30 sec in practice) but we adding disruption here
                // which is between 1 and 2 sec can cause each of the shard deletion requests to timeout.
                // to prevent this we are setting the timeout here to something highish ie. the default in practice
                .put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(30, TimeUnit.SECONDS))
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        // testShardActiveElseWhere might change the state of a non-master node
        // so we cannot check state consistency of this cluster
    }

    public void testIndexCleanup() throws Exception {
        final String masterNode = internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false));
        final String node_1 = internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false));
        final String node_2 = internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false));
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        Settings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen("test");
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Index index = state.metaData().index("test").getIndex();

        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(true));

        logger.info("--> starting node server3");
        final String node_3 = internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false));
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("4")
                .setWaitForNoRelocatingShards(true)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_3, index)), equalTo(false));

        logger.info("--> move shard from node_1 to node_3, and wait for relocation to finish");

        if (randomBoolean()) { // sometimes add cluster-state delay to trigger observers in IndicesStore.ShardActiveRequestHandler
            BlockClusterStateProcessing disruption = relocateAndBlockCompletion(logger, "test", 0, node_1, node_3);
            // wait a little so that cluster state observer is registered
            sleep(50);
            logger.info("--> stopping disruption");
            disruption.stopDisrupting();
        } else {
            internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_3)).get();
        }
        clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNoRelocatingShards(true)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        assertThat(waitForShardDeletion(node_1, index, 0), equalTo(false));
        assertThat(waitForIndexDeletion(node_1, index), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_3, index)), equalTo(true));

    }

    /**
     * relocate a shard and block cluster state processing on the relocation target node to activate the shard
     */
    public static BlockClusterStateProcessing relocateAndBlockCompletion(Logger logger, String index, int shard, String nodeFrom,
                                                                         String nodeTo) throws InterruptedException {
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(nodeTo, random());
        internalCluster().setDisruptionScheme(disruption);
        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeTo);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeTo);
        CountDownLatch beginRelocationLatch = new CountDownLatch(1);
        CountDownLatch receivedShardExistsRequestLatch = new CountDownLatch(1);
        // use a tracer on the target node to track relocation start and end
        transportService.addTracer(new MockTransportService.Tracer() {
            @Override
            public void receivedRequest(long requestId, String action) {
                if (action.equals(PeerRecoveryTargetService.Actions.FILES_INFO)) {
                    logger.info("received: {}, relocation starts", action);
                    beginRelocationLatch.countDown();
                } else if (action.equals(IndicesStore.ACTION_SHARD_EXISTS)) {
                    // Whenever a node deletes a shard because it was relocated somewhere else, it first
                    // checks if enough other copies are started somewhere else. The node sends a ShardActiveRequest
                    // to the other nodes that should have a copy according to cluster state.
                    receivedShardExistsRequestLatch.countDown();
                    logger.info("received: {}, relocation done", action);
                } else if (action.equals(PeerRecoveryTargetService.Actions.WAIT_CLUSTERSTATE)) {
                    logger.info("received: {}, waiting on cluster state", action);
                    // ensure that relocation target node is on the same cluster state as relocation source before proceeding with
                    // this request. If the target does not have the relocating cluster state exposed through ClusterService.state(),
                    // then waitForClusterState will have to register a ClusterObserver with the ClusterService, which can cause
                    // a race with the BlockClusterStateProcessing block that is added below.
                    try {
                        assertBusy(() -> assertTrue(
                            clusterService.state().routingTable().index(index).shard(shard).primaryShard().relocating()));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(index, shard, nodeFrom, nodeTo)).get();
        logger.info("--> waiting for relocation to start");
        beginRelocationLatch.await();
        logger.info("--> starting disruption");
        disruption.startDisrupting();
        logger.info("--> waiting for relocation to finish");
        receivedShardExistsRequestLatch.await();
        logger.info("--> relocation completed (but cluster state processing block still in place)");
        return disruption;
    }

    /* Test that shard is deleted in case ShardActiveRequest after relocation and next incoming cluster state is an index delete. */
    public void testShardCleanupIfShardDeletionAfterRelocationFailedAndIndexDeleted() throws Exception {
        final String node_1 = internalCluster().startNode();
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        Settings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        ensureGreen("test");
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Index index = state.metaData().index("test").getIndex();
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));

        final String node_2 = internalCluster().startDataOnlyNode(Settings.builder().build());
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());

        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(false));

        // add a transport delegate that will prevent the shard active request to succeed the first time after relocation has finished.
        // node_1 will then wait for the next cluster state change before it tries a next attempt to delete the shard.
        MockTransportService transportServiceNode_1 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_1);
        TransportService transportServiceNode_2 = internalCluster().getInstance(TransportService.class, node_2);
        final CountDownLatch shardActiveRequestSent = new CountDownLatch(1);
        transportServiceNode_1.addDelegate(transportServiceNode_2, new MockTransportService.DelegateTransport(transportServiceNode_1.original()) {
            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException {
                if (action.equals("internal:index/shard/exists") && shardActiveRequestSent.getCount() > 0) {
                    shardActiveRequestSent.countDown();
                    logger.info("prevent shard active request from being sent");
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: simulated");
                }
                super.sendRequest(connection, requestId, action, request, options);
            }
        });

        logger.info("--> move shard from {} to {}, and wait for relocation to finish", node_1, node_2);
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_2)).get();
        shardActiveRequestSent.await();
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNoRelocatingShards(true)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logClusterState();
        // delete the index. node_1 that still waits for the next cluster state update will then get the delete index next.
        // it must still delete the shard, even if it cannot find it anymore in indicesservice
        client().admin().indices().prepareDelete("test").get();

        assertThat(waitForShardDeletion(node_1, index, 0), equalTo(false));
        assertThat(waitForIndexDeletion(node_1, index), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(false));
        assertThat(waitForShardDeletion(node_2, index, 0), equalTo(false));
        assertThat(waitForIndexDeletion(node_2, index), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(false));
    }

    public void testShardsCleanup() throws Exception {
        final String node_1 = internalCluster().startNode();
        final String node_2 = internalCluster().startNode();
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                        Settings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen("test");

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Index index = state.metaData().index("test").getIndex();
        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));

        logger.info("--> starting node server3");
        String node_3 = internalCluster().startNode();
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("3")
                .setWaitForNoRelocatingShards(true)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        logger.info("--> making sure that shard is not allocated on server3");
        assertThat(waitForShardDeletion(node_3, index, 0), equalTo(false));

        Path server2Shard = shardDirectory(node_2, index, 0);
        logger.info("--> stopping node {}", node_2);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_2));

        logger.info("--> running cluster_health");
        clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForNodes("2")
                .setWaitForNoRelocatingShards(true)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());

        assertThat(Files.exists(server2Shard), equalTo(true));

        logger.info("--> making sure that shard and its replica exist on server1, server2 and server3");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(server2Shard), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(true));

        logger.info("--> starting node node_4");
        final String node_4 = internalCluster().startNode();

        logger.info("--> running cluster_health");
        ensureGreen();

        logger.info("--> making sure that shard and its replica are allocated on server1 and server3 but not on server2");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(true));
        assertThat(waitForShardDeletion(node_4, index, 0), equalTo(false));
    }

    public void testShardActiveElsewhereDoesNotDeleteAnother() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final List<String> nodes = internalCluster().startDataOnlyNodes(4);

        final String node1 = nodes.get(0);
        final String node2 = nodes.get(1);
        final String node3 = nodes.get(2);
        // we will use this later on, handy to start now to make sure it has a different data folder that node 1,2 &3
        final String node4 = nodes.get(3);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                        .put(indexSettings())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", node4)
        ));
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNoRelocatingShards(true).setWaitForGreenStatus().setWaitForNodes("5").get().isTimedOut());

        // disable allocation to control the situation more easily
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")));

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
                Settings.builder()
                        .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", "NONE")));

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all")));

        logger.debug("--> waiting for shards to recover on [{}]", node4);
        // we have to do this in two steps as we now do async shard fetching before assigning, so the change to the
        // allocation filtering may not have immediate effect
        // TODO: we should add an easier to do this. It's too much of a song and dance..
        Index index = resolveIndex("test");
        assertBusy(() -> assertTrue(internalCluster().getInstance(IndicesService.class, node4).hasIndex(index)));

        // wait for 4 active shards - we should have lost one shard
        assertFalse(client().admin().cluster().prepareHealth().setWaitForActiveShards(4).get().isTimedOut());

        // disable allocation again to control concurrency a bit and allow shard active to kick in before allocation
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")));

        logger.debug("--> starting the two old nodes back");

        internalCluster().startDataOnlyNodes(2);

        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("5").get().isTimedOut());


        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all")));

        logger.debug("--> waiting for the lost shard to be recovered");

        ensureGreen("test");

    }

    public void testShardActiveElseWhere() throws Exception {
        List<String> nodes = internalCluster().startNodes(2);

        final String masterNode = internalCluster().getMasterName();
        final String nonMasterNode = nodes.get(0).equals(masterNode) ? nodes.get(1) : nodes.get(0);

        final String masterId = internalCluster().clusterService(masterNode).localNode().getId();
        final String nonMasterId = internalCluster().clusterService(nonMasterNode).localNode().getId();

        final int numShards = scaledRandomIntBetween(2, 10);
        assertAcked(prepareCreate("test")
                        .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards))
        );
        ensureGreen("test");

        waitNoPendingTasksOnAll();
        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final Index index = stateResponse.getState().metaData().index("test").getIndex();
        RoutingNode routingNode = stateResponse.getState().getRoutingNodes().node(nonMasterId);
        final int[] node2Shards = new int[routingNode.numberOfOwningShards()];
        int i = 0;
        for (ShardRouting shardRouting : routingNode) {
            node2Shards[i] = shardRouting.shardId().id();
            i++;
        }
        logger.info("Node [{}] has shards: {}", nonMasterNode, Arrays.toString(node2Shards));

        // disable relocations when we do this, to make sure the shards are not relocated from node2
        // due to rebalancing, and delete its content
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)).get();

        ClusterApplierService clusterApplierService = internalCluster().getInstance(ClusterService.class, nonMasterNode).getClusterApplierService();
        ClusterState currentState = clusterApplierService.state();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int j = 0; j < numShards; j++) {
            indexRoutingTableBuilder.addIndexShard(
                new IndexShardRoutingTable.Builder(new ShardId(index, j))
                    .addShard(TestShardRouting.newShardRouting("test", j, masterId, true, ShardRoutingState.STARTED))
                    .build()
            );
        }
        ClusterState newState = ClusterState.builder(currentState)
            .incrementVersion()
            .routingTable(RoutingTable.builder().add(indexRoutingTableBuilder).build())
            .build();
        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState("test", () -> newState, new ClusterStateTaskListener() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                latch.countDown();
                fail("Excepted proper response " + ExceptionsHelper.detailedMessage(e));
            }
        });
        latch.await();
        waitNoPendingTasksOnAll();
        logger.info("Checking if shards aren't removed");
        for (int shard : node2Shards) {
            assertTrue(waitForShardDeletion(nonMasterNode, index, shard));
        }
    }

    private Path indexDirectory(String server, Index index) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        final Path[] paths = env.indexPaths(index);
        assert paths.length == 1;
        return paths[0];
    }

    private Path shardDirectory(String server, Index index, int shard) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        final Path[] paths = env.availableShardPaths(new ShardId(index, shard));
        assert paths.length == 1;
        return paths[0];
    }

    private boolean waitForShardDeletion(final String server, final Index index, final int shard) throws InterruptedException {
        awaitBusy(() -> !Files.exists(shardDirectory(server, index, shard)));
        return Files.exists(shardDirectory(server, index, shard));
    }

    private boolean waitForIndexDeletion(final String server, final Index index) throws InterruptedException {
        awaitBusy(() -> !Files.exists(indexDirectory(server, index)));
        return Files.exists(indexDirectory(server, index));
    }
}
