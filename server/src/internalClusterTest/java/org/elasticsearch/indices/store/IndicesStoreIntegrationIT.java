/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndicesStoreIntegrationIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir())
            .build();
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        // testShardActiveElseWhere might change the state of a non-master node
        // so we cannot check state consistency of this cluster
    }

    public void testIndexCleanup() throws Exception {
        internalCluster().startNode(nonDataNode());
        final String node_1 = internalCluster().startNode(nonMasterNode());
        final String node_2 = internalCluster().startNode(nonMasterNode());
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen("test");
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        Index index = state.metadata().getProject().index("test").getIndex();

        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(true));

        logger.info("--> starting node server3");
        final String node_3 = internalCluster().startNode(nonMasterNode());
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
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
        ClusterRerouteUtils.reroute(internalCluster().client(), new MoveAllocationCommand("test", 0, node_1, node_3));
        clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNoRelocatingShards(true).get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        assertShardDeleted(node_1, index, 0);
        assertIndexDeleted(node_1, index);
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_3, index)), equalTo(true));

    }

    public void testShardsCleanup() throws Exception {
        final String node_1 = internalCluster().startNode();
        final String node_2 = internalCluster().startNode();
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen("test");

        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        Index index = state.metadata().getProject().index("test").getIndex();
        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));

        logger.info("--> starting node server3");
        String node_3 = internalCluster().startNode();
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForNodes("3")
            .setWaitForNoRelocatingShards(true)
            .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        logger.info("--> making sure that shard is not allocated on server3");
        assertShardDeleted(node_3, index, 0);

        Path server2Shard = shardDirectory(node_2, index, 0);
        logger.info("--> stopping node {}", node_2);
        internalCluster().stopNode(node_2);

        logger.info("--> running cluster_health");
        clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
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
        assertShardDeleted(node_4, index, 0);
    }

    public void testShardActiveElsewhereDoesNotDeleteAnother() throws Exception {
        internalCluster().startMasterOnlyNode();
        final List<String> nodes = internalCluster().startDataOnlyNodes(4);

        final String node1 = nodes.get(0);
        final String node2 = nodes.get(1);
        final String node3 = nodes.get(2);
        // we will use this later on, handy to start now to make sure it has a different data folder that node 1,2 &3
        final String node4 = nodes.get(3);

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", node4)
            )
        );
        assertFalse(
            clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                .setWaitForNoRelocatingShards(true)
                .setWaitForGreenStatus()
                .setWaitForNodes("5")
                .get()
                .isTimedOut()
        );

        // disable allocation to control the situation more easily
        updateClusterSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none"));

        logger.debug("--> shutting down two random nodes");
        List<String> nodesToShutDown = randomSubsetOf(2, node1, node2, node3);
        Settings node1DataPathSettings = internalCluster().dataPathSettings(nodesToShutDown.get(0));
        Settings node2DataPathSettings = internalCluster().dataPathSettings(nodesToShutDown.get(1));
        internalCluster().stopNode(nodesToShutDown.get(0));
        internalCluster().stopNode(nodesToShutDown.get(1));

        logger.debug("--> verifying index is red");
        ClusterHealthResponse health = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNodes("3").get();
        if (health.getStatus() != ClusterHealthStatus.RED) {
            logClusterState();
            fail("cluster didn't become red, despite of shutting 2 of 3 nodes");
        }

        logger.debug("--> allowing index to be assigned to node [{}]", node4);
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", "NONE"), "test");
        updateClusterSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all"));

        logger.debug("--> waiting for shards to recover on [{}]", node4);
        // we have to do this in two steps as we now do async shard fetching before assigning, so the change to the
        // allocation filtering may not have immediate effect
        // TODO: we should add an easier to do this. It's too much of a song and dance..
        Index index = resolveIndex("test");
        assertBusy(() -> assertTrue(internalCluster().getInstance(IndicesService.class, node4).hasIndex(index)));

        // wait for 4 active shards - we should have lost one shard
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForActiveShards(4).get().isTimedOut());

        // disable allocation again to control concurrency a bit and allow shard active to kick in before allocation
        updateClusterSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none"));

        logger.debug("--> starting the two old nodes back");

        internalCluster().startNodes(node1DataPathSettings, node2DataPathSettings);

        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNodes("5").get().isTimedOut());

        updateClusterSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all"));

        logger.debug("--> waiting for the lost shard to be recovered");

        ensureGreen("test");

    }

    public void testShardActiveElseWhere() throws Exception {
        List<String> nodes = internalCluster().startNodes(2);

        final String masterNode = internalCluster().getMasterName();
        final String nonMasterNode = nodes.get(0).equals(masterNode) ? nodes.get(1) : nodes.get(0);

        final String masterId = getNodeId(masterNode);
        final String nonMasterId = getNodeId(nonMasterNode);

        final int numShards = scaledRandomIntBetween(2, 10);
        assertAcked(prepareCreate("test").setSettings(indexSettings(numShards, 0)));
        ensureGreen("test");

        waitNoPendingTasksOnAll();
        ClusterStateResponse stateResponse = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get();
        final Index index = stateResponse.getState().metadata().getProject().index("test").getIndex();
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
        updateClusterSettings(
            Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        );

        ClusterApplierService clusterApplierService = internalCluster().getInstance(ClusterService.class, nonMasterNode)
            .getClusterApplierService();
        ClusterState currentState = clusterApplierService.state();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (int j = 0; j < numShards; j++) {
            final var shardId = new ShardId(index, j);
            indexRoutingTableBuilder.addIndexShard(
                new IndexShardRoutingTable.Builder(shardId).addShard(
                    TestShardRouting.newShardRouting(shardId, masterId, true, ShardRoutingState.STARTED)
                )
            );
        }
        ClusterState newState = ClusterState.builder(currentState)
            .incrementVersion()
            .routingTable(RoutingTable.builder().add(indexRoutingTableBuilder).build())
            .build();
        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.onNewClusterState("test", () -> newState, new ActionListener<>() {
            @Override
            public void onResponse(Void ignored) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                throw new AssertionError("Expected a proper response", e);
            }
        });
        latch.await();
        waitNoPendingTasksOnAll();
        logger.info("Checking if shards aren't removed");
        for (int shard : node2Shards) {
            assertShardExists(nonMasterNode, index, shard);
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

    private void assertShardDeleted(final String server, final Index index, final int shard) throws Exception {
        final Path path = shardDirectory(server, index, shard);
        assertBusy(() -> assertFalse("Expected shard to not exist: " + path, Files.exists(path)));
    }

    private void assertShardExists(final String server, final Index index, final int shard) throws Exception {
        final Path path = shardDirectory(server, index, shard);
        assertBusy(() -> assertTrue("Expected shard to exist: " + path, Files.exists(path)));
    }

    private void assertIndexDeleted(final String server, final Index index) throws Exception {
        final Path path = indexDirectory(server, index);
        assertBusy(() -> assertFalse("Expected index to be deleted: " + path, Files.exists(path)));
    }
}
