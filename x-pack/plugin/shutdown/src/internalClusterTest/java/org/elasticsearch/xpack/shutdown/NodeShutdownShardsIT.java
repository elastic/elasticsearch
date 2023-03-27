/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.COMPLETE;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.STALLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0)
public class NodeShutdownShardsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ShutdownPlugin.class);
    }

    /**
     * Verifies that a node that's removed from the cluster with zero shards stays in the `COMPLETE` status after it leaves, rather than
     * reverting to `NOT_STARTED` (this was a bug in the initial implementation).
     */
    public void testShardStatusStaysCompleteAfterNodeLeaves() throws Exception {
        assumeTrue("must be on a snapshot build of ES to run in order for the feature flag to be set", Build.CURRENT.isSnapshot());
        final String nodeToRestartName = internalCluster().startNode();
        final String nodeToRestartId = getNodeId(nodeToRestartName);
        internalCluster().startNode();

        // Mark the node for shutdown
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.REMOVE,
            this.getTestName(),
            null,
            null
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        internalCluster().stopNode(nodeToRestartName);

        NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
        assertThat(nodes.getNodes().size(), equalTo(1));

        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeToRestartId)
        ).get();

        assertThat(getResp.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
    }

    /**
     * Similar to the previous test, but ensures that the status stays at `COMPLETE` when the node is offline when the shutdown is
     * registered. This may happen if {@link NodeSeenService} isn't working as expected.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/76689")
    public void testShardStatusStaysCompleteAfterNodeLeavesIfRegisteredWhileNodeOffline() throws Exception {
        assumeTrue("must be on a snapshot build of ES to run in order for the feature flag to be set", Build.CURRENT.isSnapshot());
        final String nodeToRestartName = internalCluster().startNode();
        final String nodeToRestartId = getNodeId(nodeToRestartName);
        internalCluster().startNode();

        // Stop the node we're going to shut down and mark it as shutting down while it's offline. This checks that the cluster state
        // listener is working correctly.
        internalCluster().restartNode(nodeToRestartName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
                    nodeToRestartId,
                    SingleNodeShutdownMetadata.Type.REMOVE,
                    "testShardStatusStaysCompleteAfterNodeLeavesIfRegisteredWhileNodeOffline",
                    null,
                    null
                );
                AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
                assertTrue(putShutdownResponse.isAcknowledged());

                return super.onNodeStopped(nodeName);
            }
        });

        internalCluster().stopNode(nodeToRestartName);

        NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
        assertThat(nodes.getNodes().size(), equalTo(1));

        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeToRestartId)
        ).get();

        assertThat(getResp.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
    }

    /**
     * Checks that non-data nodes that are registered for shutdown have a shard migration status of `COMPLETE` rather than `NOT_STARTED`.
     * (this was a bug in the initial implementation).
     */
    public void testShardStatusIsCompleteOnNonDataNodes() throws Exception {
        assumeTrue("must be on a snapshot build of ES to run in order for the feature flag to be set", Build.CURRENT.isSnapshot());
        final String nodeToShutDownName = internalCluster().startMasterOnlyNode();
        internalCluster().startMasterOnlyNode(); // Just to have at least one other node
        final String nodeToRestartId = getNodeId(nodeToShutDownName);

        // Mark the node for shutdown
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.REMOVE,
            this.getTestName(),
            null,
            null
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeToRestartId)
        ).get();

        assertThat(getResp.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
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
        indexRandomData();

        String nodeToStopId = findIdOfNodeWithPrimaryShard(indexName);
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToStopId,
            SingleNodeShutdownMetadata.Type.REMOVE,
            this.getTestName(),
            null,
            null
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());
        assertBusy(() -> {
            GetShutdownStatusAction.Response getResp = client().execute(
                GetShutdownStatusAction.INSTANCE,
                new GetShutdownStatusAction.Request(nodeToStopId)
            ).get();

            assertThat(getResp.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
        });
    }

    public void testNodeReplacementOnlyAllowsShardsFromReplacedNode() throws Exception {
        String nodeA = internalCluster().startNode(Settings.builder().put("node.name", "node-a"));
        Settings.Builder nodeASettings = Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1);
        createIndex("myindex", nodeASettings.build());
        final String nodeAId = getNodeId(nodeA);
        final String nodeB = "node_t1"; // TODO: fix this to so it's actually overrideable

        // Mark the nodeA as being replaced
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeAId,
            SingleNodeShutdownMetadata.Type.REPLACE,
            this.getTestName(),
            null,
            nodeB
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeAId)
        ).get();

        assertThat(getResp.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(STALLED));

        internalCluster().startNode(Settings.builder().put("node.name", nodeB));
        final String nodeBId = getNodeId(nodeB);

        logger.info("--> NodeA: {} -- {}", nodeA, nodeAId);
        logger.info("--> NodeB: {} -- {}", nodeB, nodeBId);

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState();
            int active = 0;
            for (ShardRouting sr : state.routingTable().allShards("myindex")) {
                if (sr.active()) {
                    active++;
                    assertThat(
                        "expected shard on nodeB (" + nodeBId + ") but it was on a different node",
                        sr.currentNodeId(),
                        equalTo(nodeBId)
                    );
                }
            }
            assertThat("expected all 3 of the primary shards to be allocated", active, equalTo(3));
        });

        assertBusy(() -> {
            GetShutdownStatusAction.Response shutdownStatus = client().execute(
                GetShutdownStatusAction.INSTANCE,
                new GetShutdownStatusAction.Request(nodeAId)
            ).get();
            assertThat(shutdownStatus.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
        });

        final String nodeC = internalCluster().startNode();

        createIndex("other", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1).build());

        ensureYellow("other");

        // Explain the replica for the "other" index
        ClusterAllocationExplainResponse explainResponse = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex("other")
            .setShard(0)
            .setPrimary(false)
            .get();

        // Validate that the replica cannot be allocated to nodeB because it's the target of a node replacement
        explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getAllocateDecision()
            .getNodeDecisions()
            .stream()
            .filter(nodeDecision -> nodeDecision.getNode().getId().equals(nodeBId))
            .findFirst()
            .ifPresent(nodeAllocationResult -> {
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
            });
    }

    public void testNodeReplacementOverridesFilters() throws Exception {
        String nodeA = internalCluster().startNode(Settings.builder().put("node.name", "node-a"));
        // Create an index and pin it to nodeA, when we replace it with nodeB,
        // it'll move the data, overridding the `_name` allocation filter
        Settings.Builder nodeASettings = Settings.builder()
            .put("index.routing.allocation.require._name", nodeA)
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 0);
        createIndex("myindex", nodeASettings.build());
        final String nodeAId = getNodeId(nodeA);
        final String nodeB = "node_t2"; // TODO: fix this to so it's actually overrideable

        // Mark the nodeA as being replaced
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeAId,
            SingleNodeShutdownMetadata.Type.REPLACE,
            this.getTestName(),
            null,
            nodeB
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeAId)
        ).get();

        assertThat(getResp.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(STALLED));

        final String nodeC = internalCluster().startNode();
        internalCluster().startNode(Settings.builder().put("node.name", nodeB));
        final String nodeBId = getNodeId(nodeB);

        logger.info("--> NodeA: {} -- {}", nodeA, nodeAId);
        logger.info("--> NodeB: {} -- {}", nodeB, nodeBId);

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState();
            for (ShardRouting sr : state.routingTable().allShards("myindex")) {
                assertThat(
                    "expected shard on nodeB (" + nodeBId + ") but it was on a different node",
                    sr.currentNodeId(),
                    equalTo(nodeBId)
                );
            }
        });

        assertBusy(() -> {
            GetShutdownStatusAction.Response shutdownStatus = client().execute(
                GetShutdownStatusAction.INSTANCE,
                new GetShutdownStatusAction.Request(nodeAId)
            ).get();
            assertThat(shutdownStatus.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
        });

        createIndex("other", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1).build());

        ensureYellow("other");

        // Explain the replica for the "other" index
        ClusterAllocationExplainResponse explainResponse = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex("other")
            .setShard(0)
            .setPrimary(false)
            .get();

        // Validate that the replica cannot be allocated to nodeB because it's the target of a node replacement
        explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getAllocateDecision()
            .getNodeDecisions()
            .stream()
            .filter(nodeDecision -> nodeDecision.getNode().getId().equals(nodeBId))
            .findFirst()
            .ifPresent(nodeAllocationResult -> {
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
            });
    }

    public void testNodeReplacementOnlyToTarget() throws Exception {
        String nodeA = internalCluster().startNode(
            Settings.builder().put("node.name", "node-a").put("cluster.routing.rebalance.enable", "none")
        );
        Settings.Builder nodeASettings = Settings.builder().put("index.number_of_shards", 4).put("index.number_of_replicas", 0);
        createIndex("myindex", nodeASettings.build());
        final String nodeAId = getNodeId(nodeA);
        final String nodeB = "node_t1"; // TODO: fix this to so it's actually overrideable
        final String nodeC = "node_t2"; // TODO: fix this to so it's actually overrideable

        // Mark the nodeA as being replaced
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeAId,
            SingleNodeShutdownMetadata.Type.REPLACE,
            this.getTestName(),
            null,
            nodeB
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeAId)
        ).get();

        assertThat(getResp.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(STALLED));

        internalCluster().startNode(Settings.builder().put("node.name", nodeB));
        internalCluster().startNode(Settings.builder().put("node.name", nodeC));
        final String nodeBId = getNodeId(nodeB);
        final String nodeCId = getNodeId(nodeC);

        logger.info("--> NodeA: {} -- {}", nodeA, nodeAId);
        logger.info("--> NodeB: {} -- {}", nodeB, nodeBId);
        logger.info("--> NodeC: {} -- {}", nodeC, nodeCId);

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState();
            for (ShardRouting sr : state.routingTable().allShards("myindex")) {
                assertThat(
                    "expected all shards for index to be on node B (" + nodeBId + ") but " + sr.toString() + " is on " + sr.currentNodeId(),
                    sr.currentNodeId(),
                    equalTo(nodeBId)
                );
            }
        });

        assertBusy(() -> {
            GetShutdownStatusAction.Response shutdownStatus = client().execute(
                GetShutdownStatusAction.INSTANCE,
                new GetShutdownStatusAction.Request(nodeAId)
            ).get();
            assertThat(shutdownStatus.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
        });
    }

    public void testReallocationForReplicaDuringNodeReplace() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeAId = getNodeId(nodeA);
        createIndex("myindex", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1).build());
        ensureYellow("myindex");

        // Start a second node, so the replica will be on nodeB
        final String nodeB = internalCluster().startNode();
        ensureGreen("myindex");

        final String nodeC = internalCluster().startNode();

        // Register a replace for nodeA, with nodeC as the target
        PutShutdownNodeAction.Request shutdownRequest = new PutShutdownNodeAction.Request(
            nodeAId,
            SingleNodeShutdownMetadata.Type.REPLACE,
            "testing",
            null,
            nodeC
        );
        client().execute(PutShutdownNodeAction.INSTANCE, shutdownRequest).get();

        // Wait for the node replace shutdown to be complete
        assertBusy(() -> {
            GetShutdownStatusAction.Response shutdownStatus = client().execute(
                GetShutdownStatusAction.INSTANCE,
                new GetShutdownStatusAction.Request(nodeAId)
            ).get();
            assertThat(shutdownStatus.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(COMPLETE));
        });

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

        // Mark the node for shutdown
        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(primaryNodeId, SingleNodeShutdownMetadata.Type.RESTART, this.getTestName(), null, null)
            ).get()
        );

        // RESTART did not reroute, neither should it when we no longer contract replicas, but we provoke it here in the test to ensure
        // that auto-expansion has run.
        UpdateSettingsRequestBuilder settingsRequest = client().admin().indices().prepareUpdateSettings("myindex");
        settingsRequest.setSettings(Settings.builder().put("index.routing.allocation.exclude.name", "non-existent"));
        assertAcked(settingsRequest.execute().actionGet());

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

        client().prepareIndex("myindex", "_doc").setSource("field", "value");

        internalCluster().restartNode(primaryNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureYellow("myindex");
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen("myindex");
    }

    public void testNodeShutdownWithUnassignedShards() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeAId = getNodeId(nodeA);

        createIndex(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", nodeA)
                .build()
        );

        ensureYellow("index");

        // Start a second node, so the replica will be on nodeB
        final String nodeB = internalCluster().startNode();
        final String nodeBId = getNodeId(nodeB);
        ensureGreen("index");

        putNodeShutdown(nodeAId, SingleNodeShutdownMetadata.Type.REMOVE, null);

        internalCluster().stopNode(nodeA);

        assertBusy(() -> assertNodeShutdownStatus(nodeAId, STALLED));
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
        GetShutdownStatusAction.Response response = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(nodeId)
        ).get();
        assertThat(response.getShutdownStatuses().get(0).migrationStatus().getStatus(), equalTo(status));
    }

    private void indexRandomData() throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "_doc").setSource("field", "value");
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
                    new ParameterizedMessage(
                        "could not find a primary shard of index [{}] in list of started shards [{}]",
                        indexName,
                        startedShards
                    )
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
            .orElseThrow(() -> new AssertionError("requested node name [" + nodeName + "] not found"));
    }
}
