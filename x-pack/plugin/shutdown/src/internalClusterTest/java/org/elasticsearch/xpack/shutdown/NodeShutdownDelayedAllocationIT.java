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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeShutdownDelayedAllocationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ShutdownPlugin.class);
    }

    public void testShardAllocationIsDelayedForRestartingNode() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0)).get(); // Disable "normal" delayed allocation
        ensureGreen("test");
        indexRandomData();

        final String nodeToRestartId = findIdOfNodeWithShard();
        final String nodeToRestartName = findNodeNameFromId(nodeToRestartId);
        Settings nodeToRestartDataPathSettings = internalCluster().dataPathSettings(nodeToRestartName);

        // Mark the node for shutdown
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            nodeToRestartId,
            SingleNodeShutdownMetadata.Type.RESTART,
            this.getTestName()
        );
        AcknowledgedResponse putShutdownResponse = client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get();
        assertTrue(putShutdownResponse.isAcknowledged());

        // Actually stop the node
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeToRestartName));

        // Verify that the shard's allocation is delayed
        assertBusy(() -> {
            assertThat(client().admin().cluster().prepareHealth().get().getDelayedUnassignedShards(), equalTo(1));
        });

        // Bring the node back
        internalCluster().startNode(nodeToRestartDataPathSettings); // this will use the same data location as the stopped node

        // And the index should turn green again
        ensureGreen("test");
    }

    // GWB-> Next tests to write:
    //       - Timeout
    //       - Change the timeout setting
    //       - Delete the shutdown record

    private void indexRandomData() throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setSource("field", "value");
        }
        indexRandom(true, builders);
    }

    private String findIdOfNodeWithShard() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> startedShards = state.routingTable().shardsWithState(ShardRoutingState.STARTED);
        Collections.shuffle(startedShards,random());
        return startedShards.get(0).currentNodeId();
    }

    private String findNodeNameFromId(String id) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        return state.nodes().get(id).getName();
    }
}
