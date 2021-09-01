/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class NodeShutdownDelayedAllocationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ShutdownPlugin.class);
    }

    public void testShardAllocationIsDelayedForRestartingNode() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0) // Disable "normal" delayed allocation
        ).get();
        ensureGreen("test");
        indexRandomData();

        final String nodeToRestartId = findIdOfNodeWithShard();
        final String nodeToRestartName = findNodeNameFromId(nodeToRestartId);

        // Mark the node for shutdown
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.RESTART,
            this.getTestName(),
            null // Make sure it works with the default - we'll check this override in other tests
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        internalCluster().restartNode(nodeToRestartName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                assertBusy(
                    () -> { assertThat(client().admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(1)); }
                );
                return super.onNodeStopped(nodeName);
            }
        });

        // And the index should turn green again
        ensureGreen("test");
    }

    public void testShardAllocationWillProceedAfterTimeout() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0) // Disable "normal" delayed allocation
        ).get();
        ensureGreen("test");
        indexRandomData();

        final String nodeToRestartId = findIdOfNodeWithShard();
        final String nodeToRestartName = findNodeNameFromId(nodeToRestartId);

        // Mark the node for shutdown
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.RESTART,
            this.getTestName(),
            TimeValue.timeValueMillis(randomIntBetween(10, 1000))
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        // Actually stop the node
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeToRestartName));

        // And the index should turn green again well within the 30-second timeout
        ensureGreen("test");
    }

    public void testIndexLevelAllocationDelayWillBeUsedIfLongerThanShutdownDelay() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "3h") // Use a long timeout we definitely won't hit
        ).get();
        ensureGreen("test");
        indexRandomData();

        final String nodeToRestartId = findIdOfNodeWithShard();
        final String nodeToRestartName = findNodeNameFromId(nodeToRestartId);

        // Mark the node for shutdown
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.RESTART,
            this.getTestName(),
            TimeValue.timeValueMillis(0) // No delay for reallocating these shards, IF this timeout is used.
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        internalCluster().restartNode(nodeToRestartName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                assertBusy(
                    () -> { assertThat(client().admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(1)); }
                );
                return super.onNodeStopped(nodeName);
            }
        });

        // And the index should turn green again
        ensureGreen("test");
    }

    public void testShardAllocationTimeoutCanBeChanged() throws Exception {
        String nodeToRestartId = setupLongTimeoutTestCase();

        // Update the timeout on the shutdown request to something shorter
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.RESTART,
            this.getTestName(),
            TimeValue.timeValueMillis(1)
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        // And the index should turn green again
        ensureGreen("test");
    }

    public void testShardAllocationStartsImmediatelyIfShutdownDeleted() throws Exception {
        String nodeToRestartId = setupLongTimeoutTestCase();

        DeleteShutdownNodeAction.Request deleteShutdownRequest = new DeleteShutdownNodeAction.Request(nodeToRestartId);
        AcknowledgedResponse deleteShutdownResponse = client().execute(DeleteShutdownNodeAction.INSTANCE, deleteShutdownRequest).get();
        assertTrue(deleteShutdownResponse.isAcknowledged());

        // And the index should turn green again
        ensureGreen("test");
    }

    /**
     * Sets up a cluster and an index, picks a random node that has a shard, marks it for shutdown with a long timeout, and then stops the
     * node.
     *
     * @return The ID of the node that was randomly chosen to be marked for shutdown and stopped.
     */
    private String setupLongTimeoutTestCase() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0) // Disable "normal" delayed allocation
        ).get();
        ensureGreen("test");
        indexRandomData();

        final String nodeToRestartId = findIdOfNodeWithShard();
        final String nodeToRestartName = findNodeNameFromId(nodeToRestartId);

        {
            // Mark the node for shutdown with a delay that we'll never reach in the test
            PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
                nodeToRestartId,
                SingleNodeShutdownMetadata.Type.RESTART,
                this.getTestName(),
                TimeValue.timeValueHours(3)
            );
            AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
            assertTrue(putShutdownResponse.isAcknowledged());
        }

        // Actually stop the node
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeToRestartName));

        // Verify that the shard's allocation is delayed
        assertBusy(() -> { assertThat(client().admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(1)); });

        return nodeToRestartId;
    }

    private void indexRandomData() throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "_doc").setSource("field", "value");
        }
        indexRandom(true, builders);
    }

    private String findIdOfNodeWithShard() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> startedShards = state.routingTable().shardsWithState(ShardRoutingState.STARTED);
        Collections.shuffle(startedShards, random());
        return startedShards.get(0).currentNodeId();
    }

    private String findNodeNameFromId(String id) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        return state.nodes().get(id).getName();
    }
}
