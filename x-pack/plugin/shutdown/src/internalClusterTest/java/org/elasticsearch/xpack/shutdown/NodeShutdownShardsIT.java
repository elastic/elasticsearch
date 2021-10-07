/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Status.COMPLETE;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
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

    private void indexRandomData() throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("field", "value");
        }
        indexRandom(true, builders);
    }

    private String findIdOfNodeWithPrimaryShard(String indexName) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> startedShards = state.routingTable().shardsWithState(ShardRoutingState.STARTED);
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

    private String getNodeId(String nodeName) throws Exception {
        NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().clear().get();
        return nodes.getNodes()
            .stream()
            .map(NodeInfo::getNode)
            .filter(node -> node.getName().equals(nodeName))
            .map(DiscoveryNode::getId)
            .findFirst()
            .orElseThrow();
    }
}
