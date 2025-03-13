/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanationUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.COMPLETE;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.STALLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeShutdownShardsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ShutdownPlugin.class);
    }

    /**
     * Verifies that a node that's removed from the cluster with zero shards stays in the `COMPLETE` status after it leaves, rather than
     * reverting to `NOT_STARTED` (this was a bug in the initial implementation).
     */
    public void testShardStatusStaysCompleteAfterNodeLeaves() throws Exception {
        final String nodeToRestartName = internalCluster().startNode();
        final String nodeToRestartId = getNodeId(nodeToRestartName);
        internalCluster().startNode();

        putNodeShutdown(nodeToRestartId, SingleNodeShutdownMetadata.Type.REMOVE, null);

        internalCluster().stopNode(nodeToRestartName);

        NodesInfoResponse nodes = clusterAdmin().prepareNodesInfo().clear().get();
        assertThat(nodes.getNodes().size(), equalTo(1));

        assertNodeShutdownStatus(nodeToRestartId, COMPLETE);
    }

    /**
     * Similar to the previous test, but ensures that the status stays at `COMPLETE` when the node is offline when the shutdown is
     * registered. This may happen if {@link NodeSeenService} isn't working as expected.
     */
    public void testShardStatusStaysCompleteAfterNodeLeavesIfRegisteredWhileNodeOffline() throws Exception {
        final String nodeToRestartName = internalCluster().startNode();
        final String nodeToRestartId = getNodeId(nodeToRestartName);
        internalCluster().startNode();

        // Stop the node we're going to shut down and mark it as shutting down while it's offline. This checks that the cluster state
        // listener is working correctly.
        internalCluster().restartNode(nodeToRestartName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                putNodeShutdown(nodeToRestartId, SingleNodeShutdownMetadata.Type.REMOVE, null);
                return super.onNodeStopped(nodeName);
            }
        });

        internalCluster().stopNode(nodeToRestartName);

        NodesInfoResponse nodes = clusterAdmin().prepareNodesInfo().clear().get();
        assertThat(nodes.getNodes().size(), equalTo(1));

        assertBusy(() -> { assertNodeShutdownStatus(nodeToRestartId, COMPLETE); });
    }

    /**
     * Checks that non-data nodes that are registered for shutdown have a shard migration status of `COMPLETE` rather than `NOT_STARTED`.
     * (this was a bug in the initial implementation).
     */
    public void testShardStatusIsCompleteOnNonDataNodes() throws Exception {
        final String nodeToShutDownName = internalCluster().startMasterOnlyNode();
        internalCluster().startMasterOnlyNode(); // Just to have at least one other node
        final String nodeToRestartId = getNodeId(nodeToShutDownName);

        putNodeShutdown(nodeToRestartId, SingleNodeShutdownMetadata.Type.REMOVE, null);
        assertNodeShutdownStatus(nodeToRestartId, COMPLETE);
    }

    /**
     * Checks that, if we get to a situation where a shard can't move because all other nodes already have a copy of that shard,
     * we'll still return COMPLETE instead of STALLED.
     */
    public void testNotStalledIfAllShardsHaveACopyOnAnotherNode() throws Exception {
        internalCluster().startNodes(2);

        final String indexName = "test";
        prepareCreate(indexName).setSettings(
            // Ensure we have a copy of the shard on both nodes and disable "normal" delayed allocation
            indexSettings(1, 1).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0)
        ).get();
        ensureGreen(indexName);
        indexRandomData(indexName);

        String nodeToStopId = findIdOfNodeWithPrimaryShard(indexName);
        putNodeShutdown(nodeToStopId, SingleNodeShutdownMetadata.Type.REMOVE, null);
        assertBusy(() -> assertNodeShutdownStatus(nodeToStopId, COMPLETE));
    }

    public void testNodeReplacementOnlyAllowsShardsFromReplacedNode() throws Exception {
        String nodeA = internalCluster().startNode(Settings.builder().put("node.name", "node-a"));
        createIndex("myindex", 3, 1);
        final String nodeAId = getNodeId(nodeA);
        final String nodeB = "node_t1"; // TODO: fix this to so it's actually overrideable

        putNodeShutdown(nodeAId, SingleNodeShutdownMetadata.Type.REPLACE, nodeB);
        assertNodeShutdownStatus(nodeAId, STALLED);

        internalCluster().startNode(Settings.builder().put("node.name", nodeB));
        final String nodeBId = getNodeId(nodeB);

        logger.info("Started NodeB [{}] to replace NodeA [{}]", nodeBId, nodeAId);

        assertBusy(() -> {
            assertIndexPrimaryShardsAreAllocatedOnNode("myindex", nodeBId);
            assertIndexReplicaShardsAreNotAllocated("myindex");
        });
        assertBusy(() -> assertNodeShutdownStatus(nodeAId, COMPLETE));

        final String nodeC = internalCluster().startNode();

        createIndex("other", 1, 1);

        ensureYellow("other");

        // Validate that the replica cannot be allocated to nodeB because it's the target of a node replacement
        ClusterAllocationExplanationUtils.getClusterAllocationExplanation(client(), "other", 0, false)
            .getShardAllocationDecision()
            .getAllocateDecision()
            .getNodeDecisions()
            .stream()
            .filter(nodeDecision -> nodeDecision.getNode().getId().equals(nodeBId))
            .findFirst()
            .ifPresentOrElse(nodeAllocationResult -> {
                assertThat(nodeAllocationResult.getCanAllocateDecision().type(), equalTo(Decision.Type.NO));
                assertTrue(
                    "expected decisions to mention node replacement: "
                        + nodeAllocationResult.getCanAllocateDecision()
                            .getDecisions()
                            .stream()
                            .map(Decision::getExplanation)
                            .collect(Collectors.joining(",")),
                    nodeAllocationResult.getCanAllocateDecision()
                        .getDecisions()
                        .stream()
                        .anyMatch(
                            decision -> decision.getExplanation().contains("is replacing the vacating node")
                                && decision.getExplanation().contains("may be allocated to it until the replacement is complete")
                        )
                );
            }, () -> fail("expected a 'NO' decision for nodeB but there was no explanation for that node"));
    }

    public void testNodeReplacementOverridesFilters() throws Exception {
        String nodeA = internalCluster().startNode(Settings.builder().put("node.name", "node-a"));
        // Create an index and pin it to nodeA, when we replace it with nodeB,
        // it'll move the data, overridding the `_name` allocation filter
        createIndex("myindex", indexSettings(3, 0).put("index.routing.allocation.require._name", nodeA).build());
        final String nodeAId = getNodeId(nodeA);
        final String nodeB = "node_t2"; // TODO: fix this to so it's actually overrideable

        putNodeShutdown(nodeAId, SingleNodeShutdownMetadata.Type.REPLACE, nodeB);
        assertNodeShutdownStatus(nodeAId, STALLED);

        final String nodeC = internalCluster().startNode();
        internalCluster().startNode(Settings.builder().put("node.name", nodeB));
        final String nodeBId = getNodeId(nodeB);

        logger.info("--> NodeA: {} -- {}", nodeA, nodeAId);
        logger.info("--> NodeB: {} -- {}", nodeB, nodeBId);

        assertBusy(() -> assertIndexPrimaryShardsAreAllocatedOnNode("myindex", nodeBId));
        assertBusy(() -> assertNodeShutdownStatus(nodeAId, COMPLETE));
        assertIndexSetting("myindex", "index.routing.allocation.require._name", nodeA);

        createIndex("other", 1, 1);

        ensureYellow("other");

        // Validate that the replica cannot be allocated to nodeB because it's the target of a node replacement
        ClusterAllocationExplanationUtils.getClusterAllocationExplanation(client(), "other", 0, false)
            .getShardAllocationDecision()
            .getAllocateDecision()
            .getNodeDecisions()
            .stream()
            .filter(nodeDecision -> nodeDecision.getNode().getId().equals(nodeBId))
            .findFirst()
            .ifPresentOrElse(nodeAllocationResult -> {
                assertThat(nodeAllocationResult.getCanAllocateDecision().type(), equalTo(Decision.Type.NO));
                assertTrue(
                    "expected decisions to mention node replacement: "
                        + nodeAllocationResult.getCanAllocateDecision()
                            .getDecisions()
                            .stream()
                            .map(Decision::getExplanation)
                            .collect(Collectors.joining(",")),
                    nodeAllocationResult.getCanAllocateDecision()
                        .getDecisions()
                        .stream()
                        .anyMatch(
                            decision -> decision.getExplanation().contains("is replacing the vacating node")
                                && decision.getExplanation().contains("may be allocated to it until the replacement is complete")
                        )
                );
            }, () -> fail("expected a 'NO' decision for nodeB but there was no explanation for that node"));
    }

    public void testNodeReplacementAcceptIndexThatCouldNotBeAllocatedAnywhere() throws Exception {
        String nodeA = internalCluster().startNode(Settings.builder().put("node.name", "node-a"));
        // Create an index on nodeA, then create allocation filter that could not be satisfied.
        // when we replace it with nodeB, it'll move the data, overridding the `_name` allocation filter
        createIndex("myindex", indexSettings(3, 0).put("index.routing.allocation.require._name", nodeA).build());

        var fakeNodeName = UUIDs.randomBase64UUID();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", fakeNodeName), "myindex");

        final String nodeAId = getNodeId(nodeA);
        final String nodeB = "node_t1"; // TODO: fix this to so it's actually overrideable

        putNodeShutdown(nodeAId, SingleNodeShutdownMetadata.Type.REPLACE, nodeB);
        assertNodeShutdownStatus(nodeAId, STALLED);

        internalCluster().startNode(Settings.builder().put("node.name", nodeB));
        final String nodeBId = getNodeId(nodeB);

        assertBusy(() -> assertNodeShutdownStatus(nodeAId, COMPLETE));
        assertIndexPrimaryShardsAreAllocatedOnNode("myindex", nodeBId);
        assertIndexSetting("myindex", "index.routing.allocation.require._name", fakeNodeName);
    }

    public void testNodeReplacementOnlyToTarget() throws Exception {
        String nodeA = internalCluster().startNode(
            Settings.builder().put("node.name", "node-a").put("cluster.routing.rebalance.enable", "none")
        );
        createIndex("myindex", 4, 0);
        final String nodeAId = getNodeId(nodeA);
        final String nodeB = "node_t1"; // TODO: fix this to so it's actually overrideable
        final String nodeC = "node_t2"; // TODO: fix this to so it's actually overrideable

        putNodeShutdown(nodeAId, SingleNodeShutdownMetadata.Type.REPLACE, nodeB);

        assertNodeShutdownStatus(nodeAId, STALLED);

        internalCluster().startNode(Settings.builder().put("node.name", nodeB));
        internalCluster().startNode(Settings.builder().put("node.name", nodeC));
        final String nodeBId = getNodeId(nodeB);
        final String nodeCId = getNodeId(nodeC);

        logger.info("--> NodeA: {} -- {}", nodeA, nodeAId);
        logger.info("--> NodeB: {} -- {}", nodeB, nodeBId);
        logger.info("--> NodeC: {} -- {}", nodeC, nodeCId);

        assertBusy(() -> assertIndexPrimaryShardsAreAllocatedOnNode("myindex", nodeBId));
        assertBusy(() -> assertNodeShutdownStatus(nodeAId, COMPLETE));
    }

    public void testReallocationForReplicaDuringNodeReplace() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeAId = getNodeId(nodeA);
        createIndex("myindex", 1, 1);
        ensureYellow("myindex");

        // Start a second node, so the replica will be on nodeB
        final String nodeB = internalCluster().startNode();
        ensureGreen("myindex");

        final String nodeC = internalCluster().startNode();

        putNodeShutdown(nodeAId, SingleNodeShutdownMetadata.Type.REPLACE, nodeC);

        // Wait for the node replace shutdown to be complete
        assertBusy(() -> assertNodeShutdownStatus(nodeAId, COMPLETE));

        // Remove nodeA from the cluster (it's been terminated)
        internalCluster().stopNode(nodeA);

        // Restart nodeC, the replica on nodeB will be flipped to primary and
        // when nodeC comes back up, it should have the replica assigned to it
        internalCluster().restartNode(nodeC);

        // All shards for the index should be allocated
        ensureGreen("myindex");
    }

    /**
     * Test that a node that is restarting does not affect auto-expand and that 0-1/0-all auto-expand indices stay available during
     * shutdown for restart and restart.
     * We used to have a bug where we would not auto-expand to a restarting node. That in essence meant that the index would contract. If
     * the primary were on the restarting node, we would end up with one shard on that sole node. The subsequent restart would make that
     * index unavailable.
     */
    public void testAutoExpandDuringRestart() throws Exception {
        final String primaryNode = internalCluster().startNode();
        final String primaryNodeId = getNodeId(primaryNode);
        createIndex(
            "myindex",
            Settings.builder().put("index.number_of_shards", 1).put("index.auto_expand_replicas", randomFrom("0-all", "0-1")).build()
        );

        final String nodeB = internalCluster().startNode();
        assertBusy(() -> assertIndexSetting("myindex", "index.number_of_replicas", "1"));
        ensureGreen("myindex");

        putNodeShutdown(primaryNodeId, SingleNodeShutdownMetadata.Type.RESTART, null);

        assertIndexSetting("myindex", "index.number_of_replicas", "1");
        indexRandomData("myindex");

        internalCluster().restartNode(primaryNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureYellow("myindex");
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen("myindex");
    }

    public void testAutoExpandDuringReplace() throws Exception {

        var node1 = internalCluster().startNode();
        var node2 = internalCluster().startNode();

        createIndex("index", indexSettings(1, 0).put("index.auto_expand_replicas", randomFrom("0-all", "0-1")).build());
        indexRandomData("index");

        ensureGreen("index");
        assertIndexSetting("index", "index.number_of_replicas", "1");

        var nodeNameToReplace = randomFrom(node1, node2);
        var nodeIdToReplace = getNodeId(nodeNameToReplace);
        var replacementNodeName = "node_t2";

        putNodeShutdown(nodeIdToReplace, SingleNodeShutdownMetadata.Type.REPLACE, replacementNodeName);

        ensureGreen("index");
        assertIndexSetting("index", "index.number_of_replicas", "1");

        var nodeName3 = internalCluster().startNode();
        assertThat("started node name did not match registered replacement", nodeName3, equalTo(replacementNodeName));

        ensureGreen("index");
        assertIndexSetting("index", "index.number_of_replicas", "1");

        assertBusy(() -> assertNodeShutdownStatus(nodeIdToReplace, COMPLETE));
        internalCluster().stopNode(nodeNameToReplace);

        ensureGreen("index");
        assertIndexSetting("index", "index.number_of_replicas", "1");
    }

    public void testAutoExpandDuringShutdown() throws Exception {

        var node1 = internalCluster().startNode();
        var node2 = internalCluster().startNode();

        createIndex("index", indexSettings(1, 0).put("index.auto_expand_replicas", randomFrom("0-all", "0-1")).build());
        indexRandomData("index");

        ensureGreen("index");
        assertIndexSetting("index", "index.number_of_replicas", "1");

        var nodeNameToShutdown = randomFrom(node1, node2);
        var nodeIdToShutdown = getNodeId(nodeNameToShutdown);

        putNodeShutdown(nodeIdToShutdown, SingleNodeShutdownMetadata.Type.REMOVE, null);

        ensureGreen("index");
        assertIndexSetting("index", "index.number_of_replicas", "0");

        assertBusy(() -> assertNodeShutdownStatus(nodeIdToShutdown, COMPLETE));
        internalCluster().stopNode(nodeIdToShutdown);

        ensureGreen("index");
        assertIndexSetting("index", "index.number_of_replicas", "0");
    }

    public void testNodeShutdownWithUnassignedShards() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeAId = getNodeId(nodeA);

        createIndex("index", indexSettings(1, 0).put("index.routing.allocation.include._name", nodeA).build());

        ensureYellow("index");

        // Start a second node, so the replica will be on nodeB
        final String nodeB = internalCluster().startNode();
        final String nodeBId = getNodeId(nodeB);
        ensureGreen("index");

        putNodeShutdown(nodeAId, SingleNodeShutdownMetadata.Type.REMOVE, null);

        internalCluster().stopNode(nodeA);

        assertBusy(() -> assertNodeShutdownStatus(nodeAId, STALLED));
    }

    public void testRemoveNodeWaitsForAutoExpandReplicas() throws Exception {
        final var nodes = internalCluster().startNodes(2);
        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1").build());
        ensureGreen(indexName);

        final var nodeToShutdownName = randomFrom(nodes);
        final var nodeToShutdownId = getNodeId(nodeToShutdownName);
        putNodeShutdown(nodeToShutdownId, SingleNodeShutdownMetadata.Type.REMOVE, null);
        assertBusy(() -> assertNodeShutdownStatus(nodeToShutdownId, COMPLETE));
        internalCluster().stopNode(nodeToShutdownName);

        ensureGreen(indexName);
    }

    private void indexRandomData(String index) throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(index).setSource("field", "value");
        }
        indexRandom(true, builders);
    }

    private String findIdOfNodeWithPrimaryShard(String indexName) {
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        List<ShardRouting> startedShards = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED);
        return startedShards.stream()
            .filter(ShardRouting::primary)
            .filter(shardRouting -> indexName.equals(shardRouting.index().getName()))
            .map(ShardRouting::currentNodeId)
            .findFirst()
            .orElseThrow(
                () -> new AssertionError(
                    Strings.format("could not find a primary shard of index [%s] in list of started shards [%s]", indexName, startedShards)
                )
            );
    }

    private void putNodeShutdown(String nodeId, SingleNodeShutdownMetadata.Type type, String nodeReplacementName) throws Exception {
        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    nodeId,
                    type,
                    this.getTestName(),
                    null,
                    nodeReplacementName,
                    null
                )
            )
        );
    }

    private void assertNodeShutdownStatus(String nodeId, SingleNodeShutdownMetadata.Status status) throws Exception {
        var response = client().execute(GetShutdownStatusAction.INSTANCE, new GetShutdownStatusAction.Request(TEST_REQUEST_TIMEOUT, nodeId))
            .get();
        assertThat(response.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(status));
    }

    private void assertIndexPrimaryShardsAreAllocatedOnNode(String indexName, String nodeId) {
        var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().setRoutingTable(true).get().getState();
        var indexRoutingTable = state.routingTable().index(indexName);
        for (int p = 0; p < indexRoutingTable.size(); p++) {
            var primaryShard = indexRoutingTable.shard(p).primaryShard();
            assertThat(
                "expected all primary shards for index ["
                    + indexName
                    + "] to be on node ["
                    + nodeId
                    + "] but "
                    + primaryShard
                    + " is on "
                    + primaryShard.currentNodeId(),
                primaryShard.currentNodeId(),
                equalTo(nodeId)
            );
        }
    }

    private void assertIndexReplicaShardsAreNotAllocated(String indexName) {
        var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().setRoutingTable(true).get().getState();
        var indexRoutingTable = state.routingTable().index(indexName);
        for (int p = 0; p < indexRoutingTable.size(); p++) {
            for (ShardRouting replicaShard : indexRoutingTable.shard(p).replicaShards()) {
                assertThat(replicaShard.unassigned(), equalTo(true));

                assertThat(
                    "expected all replica shards for index ["
                        + indexName
                        + "] to be unallocated but "
                        + replicaShard
                        + " is on "
                        + replicaShard.currentNodeId(),
                    replicaShard.currentNodeId(),
                    nullValue()
                );
            }
        }
    }

    private void assertIndexSetting(String index, String setting, String expectedValue) {
        var response = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, index).get();
        assertThat(response.getSetting(index, setting), equalTo(expectedValue));
    }
}
