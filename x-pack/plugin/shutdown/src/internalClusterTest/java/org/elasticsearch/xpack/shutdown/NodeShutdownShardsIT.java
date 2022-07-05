/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.COMPLETE;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.STALLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeShutdownShardsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ShutdownPlugin.class);
    }

    /**
     * Verifies that a node that's removed from the cluster with zero shards stays in the `COMPLETE` status after it leaves, rather than
     * reverting to `NOT_STARTED` (this was a bug in the initial implementation).
     */
    public void testShardStatusStaysCompleteAfterNodeLeaves() throws Exception {
        assumeTrue("must be on a snapshot build of ES to run in order for the feature flag to be set", Build.CURRENT.isSnapshot());
        var nodeToRestart = createNode("node_t0", internalCluster()::startNode);
        createNode("node_t1", internalCluster()::startNode);

        putNodeShutdown(nodeToRestart.id, SingleNodeShutdownMetadata.Type.REMOVE, null);

        internalCluster().stopNode(nodeToRestart.name);

        NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
        assertThat(nodes.getNodes().size(), equalTo(1));

        assertNodeShutdownStatus(nodeToRestart.id, COMPLETE);
    }

    /**
     * Similar to the previous test, but ensures that the status stays at `COMPLETE` when the node is offline when the shutdown is
     * registered. This may happen if {@link NodeSeenService} isn't working as expected.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/76689")
    public void testShardStatusStaysCompleteAfterNodeLeavesIfRegisteredWhileNodeOffline() throws Exception {
        assumeTrue("must be on a snapshot build of ES to run in order for the feature flag to be set", Build.CURRENT.isSnapshot());
        var nodeToRestart = createNode("node_t0", internalCluster()::startNode);
        createNode("node_t1", internalCluster()::startNode);

        // Stop the node we're going to shut down and mark it as shutting down while it's offline. This checks that the cluster state
        // listener is working correctly.
        internalCluster().restartNode(nodeToRestart.name, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                putNodeShutdown(nodeToRestart.id, SingleNodeShutdownMetadata.Type.REMOVE, null);
                return super.onNodeStopped(nodeName);
            }
        });

        internalCluster().stopNode(nodeToRestart.name);

        NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
        assertThat(nodes.getNodes().size(), equalTo(1));

        assertNodeShutdownStatus(nodeToRestart.id, COMPLETE);
    }

    /**
     * Checks that non-data nodes that are registered for shutdown have a shard migration status of `COMPLETE` rather than `NOT_STARTED`.
     * (this was a bug in the initial implementation).
     */
    public void testShardStatusIsCompleteOnNonDataNodes() throws Exception {
        assumeTrue("must be on a snapshot build of ES to run in order for the feature flag to be set", Build.CURRENT.isSnapshot());
        var nodeToRestart = createNode("node_t0", () -> internalCluster().startMasterOnlyNode());
        internalCluster().startMasterOnlyNode(); // Just to have at least one other node

        putNodeShutdown(nodeToRestart.id, SingleNodeShutdownMetadata.Type.REMOVE, null);

        assertNodeShutdownStatus(nodeToRestart.id, COMPLETE);
    }

    /**
     * Checks that, if we get to a situation where a shard can't move because all other nodes already have a copy of that shard,
     * we'll still return COMPLETE instead of STALLED.
     */
    public void testNotStalledIfAllShardsHaveACopyOnAnotherNode() throws Exception {
        internalCluster().startNodes(2);

        final String indexName = "test";
        prepareCreate(indexName).setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1) // <- Ensure we have a copy of the shard on both nodes
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0) // Disable "normal" delayed allocation
        ).get();
        ensureGreen(indexName);
        indexRandomData(indexName);

        String nodeToStopId = findIdOfNodeWithPrimaryShard(indexName);

        putNodeShutdown(nodeToStopId, SingleNodeShutdownMetadata.Type.REMOVE, null);
        assertBusy(() -> assertNodeShutdownStatus(nodeToStopId, COMPLETE));
    }

    public void testNodeReplacementOnlyAllowsShardsFromReplacedNode() throws Exception {
        var node0 = createNode("node_t0", internalCluster()::startNode);

        createIndex(
            "myindex",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );

        putNodeShutdown(node0.id, SingleNodeShutdownMetadata.Type.REPLACE, "node_t1");
        assertNodeShutdownStatus(node0.id, STALLED);

        var node1 = createNode("node_t1", internalCluster()::startNode);

        assertBusy(() -> assertNodeShutdownStatus(node0.id, COMPLETE));
        assertIndexPrimaryShardsAreAllocatedOnNode("myindex", node1.id);
        assertIndexReplicaShardsAreUnAllocated("myindex");

        var node2 = createNode("node_t2", internalCluster()::startNode);

        // newly created index should be allocated elsewhere as `node1` is "reserved" for data migrated from `node0`
        createIndex(
            "other",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        assertIndexPrimaryShardsAreAllocatedOnNode("other", node2.id);
    }

    public void testNodeReplacementOverridesFilters() throws Exception {
        var node0 = createNode("node_t0", internalCluster()::startNode);

        // Create an index and pin it to node0, when we replace it with node1,
        // it'll move the data, overridding the `_name` allocation filter
        createIndex(
            "myindex",
            Settings.builder()
                .put("index.routing.allocation.require._name", node0.name)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );

        putNodeShutdown(node0.id, SingleNodeShutdownMetadata.Type.REPLACE, "node_t1");
        assertNodeShutdownStatus(node0.id, STALLED);

        var node1 = createNode("node_t1", internalCluster()::startNode);

        assertBusy(() -> assertNodeShutdownStatus(node0.id, COMPLETE));
        assertIndexPrimaryShardsAreAllocatedOnNode("myindex", node1.id);
    }

    public void testNodeReplacementMovesDataOnlyToTheTarget() throws Exception {
        var node0 = createNode(
            "node_t0",
            () -> internalCluster().startNode(Settings.builder().put("cluster.routing.rebalance.enable", "none"))
        );

        createIndex(
            "myindex",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        putNodeShutdown(node0.id, SingleNodeShutdownMetadata.Type.REPLACE, "node_t1");
        assertNodeShutdownStatus(node0.id, STALLED);

        var node1 = createNode("node_t1", internalCluster()::startNode);
        createNode("node_t2", internalCluster()::startNode);

        assertBusy(() -> assertNodeShutdownStatus(node0.id, COMPLETE));
        assertIndexPrimaryShardsAreAllocatedOnNode("myindex", node1.id);
    }

    public void testReallocationForReplicaDuringNodeReplace() throws Exception {
        var node0 = createNode("node_t0", internalCluster()::startNode);

        createIndex(
            "myindex",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        ensureYellow("myindex");

        // Start a second node, so the replica will be on node1
        createNode("node_t1", internalCluster()::startNode);
        ensureGreen("myindex");

        var node2 = createNode("node_t2", internalCluster()::startNode);

        // Register a replacement for node0, with node2 as the target
        putNodeShutdown(node0.id, SingleNodeShutdownMetadata.Type.REPLACE, node2.name);

        assertBusy(() -> assertNodeShutdownStatus(node0.id, COMPLETE));

        // Remove node0 from the cluster (it's been terminated)
        internalCluster().stopNode(node0.name);

        // Restart node2, the replica on node1 will be flipped to primary and
        // when node2 comes back up, it should have the replica assigned to it
        internalCluster().restartNode(node2.name);

        // All shards for the index should be allocated
        ensureGreen("myindex");
    }

    public void testAllocationExplainForReplacementNode() throws Exception {
        var node0 = createNode("node_t0", internalCluster()::startNode);

        putNodeShutdown(node0.id, SingleNodeShutdownMetadata.Type.REPLACE, "node_t2");

        createNode("node_t1", internalCluster()::startNode);
        var node2 = createNode("node_t2", internalCluster()::startNode);

        createIndex(
            "other",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        ensureYellow("other");
        assertCanNotAllocateReplicaDueToReplacement("other", node2.id);
    }

    private void assertCanNotAllocateReplicaDueToReplacement(String index, String nodeId) {
        ClusterAllocationExplainResponse explainResponse = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex(index)
            .setShard(0)
            .setPrimary(false)
            .get();

        // Validate that the replica cannot be allocated to the node because it's the target of a node replacement
        explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getAllocateDecision()
            .getNodeDecisions()
            .stream()
            .filter(nodeDecision -> nodeDecision.getNode().getId().equals(nodeId))
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
            }, () -> fail("expected a 'NO' decision for " + nodeId + " but there was no explanation for that node"));
    }

    /**
     * Test that a node that is restarting does not affect auto-expand and that 0-1/0-all auto-expand indices stay available during
     * shutdown for restart and restart.
     * We used to have a bug where we would not auto-expand to a restarting node. That in essence meant that the index would contract. If
     * the primary were on the restarting node, we would end up with one shard on that sole node. The subsequent restart would make that
     * index unavailable.
     */
    public void testAutoExpandDuringRestart() throws Exception {
        var primaryNode = createNode("node_t0", internalCluster()::startNode);
        createIndex(
            "myindex",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put("index.auto_expand_replicas", randomFrom("0-all", "0-1"))
                .build()
        );

        createNode("node_t1", internalCluster()::startNode);
        assertBusy(() -> {
            assertThat(
                client().admin()
                    .indices()
                    .prepareGetSettings("myindex")
                    .setNames("index.number_of_replicas")
                    .get()
                    .getSetting("myindex", "index.number_of_replicas"),
                equalTo("1")
            );
        });
        ensureGreen("myindex");

        putNodeShutdown(primaryNode.id, SingleNodeShutdownMetadata.Type.RESTART, null);

        // RESTART did not reroute, neither should it when we no longer contract replicas, but we provoke it here in the test to ensure
        // that auto-expansion has run.
        updateIndexSettings("myindex", Settings.builder().put("index.routing.allocation.exclude.name", "non-existent"));

        assertBusy(() -> {
            assertThat(
                client().admin()
                    .indices()
                    .prepareGetSettings("myindex")
                    .setNames("index.number_of_replicas")
                    .get()
                    .getSetting("myindex", "index.number_of_replicas"),
                equalTo("1")
            );
        });

        indexRandomData("myindex");

        internalCluster().restartNode(primaryNode.name, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureYellow("myindex");
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen("myindex");
    }

    private void indexRandomData(String indexName) throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(indexName).setSource("field", "value");
        }
        indexRandom(true, builders);
    }

    private String findIdOfNodeWithPrimaryShard(String indexName) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
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

    private String getNodeId(String nodeName) {
        NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
        return nodes.getNodes()
            .stream()
            .map(NodeInfo::getNode)
            .filter(node -> node.getName().equals(nodeName))
            .map(DiscoveryNode::getId)
            .findFirst()
            .orElseThrow();
    }

    private void putNodeShutdown(String nodeId, SingleNodeShutdownMetadata.Type type, String nodeReplacementName) throws Exception {
        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(nodeId, type, this.getTestName(), null, nodeReplacementName)
            ).get()
        );
    }

    private void assertNodeShutdownStatus(String nodeId, SingleNodeShutdownMetadata.Status status) throws Exception {
        var response = client().execute(GetShutdownStatusAction.INSTANCE, new GetShutdownStatusAction.Request(nodeId)).get();
        assertThat(response.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(status));
    }

    private void assertIndexPrimaryShardsAreAllocatedOnNode(String indexName, String nodeId) {
        var state = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState();
        var indexRoutingTable = state.routingTable().index(indexName);
        for (int p = 0; p < indexRoutingTable.size(); p++) {
            var primaryShard = indexRoutingTable.shard(p).primaryShard();
            assertThat(
                "expected all shards for index ["
                    + indexName
                    + "] to be on node ["
                    + nodeId
                    + "] but "
                    + primaryShard.toString()
                    + " is on "
                    + primaryShard.currentNodeId(),
                primaryShard.currentNodeId(),
                equalTo(nodeId)
            );
        }
    }

    private void assertIndexReplicaShardsAreUnAllocated(String indexName) {
        var state = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState();
        var indexRoutingTable = state.routingTable().index(indexName);
        for (int p = 0; p < indexRoutingTable.size(); p++) {
            for (ShardRouting replicaShard : indexRoutingTable.shard(p).replicaShards()) {
                assertThat(replicaShard.unassigned(), equalTo(true));
            }
        }
    }

    private NodeNameAndId createNode(String nodeName, Supplier<String> nodeStarter) {
        var name = nodeStarter.get();
        assertThat(name, equalTo(nodeName));
        return new NodeNameAndId(name, getNodeId(name));
    }

    private record NodeNameAndId(String name, String id) {}
}
