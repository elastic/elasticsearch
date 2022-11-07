/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportCheckShardsOnDataPathAction;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrevalidateNodeRemovalIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    public void testNodeRemovalFromNonRedCluster() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        String indexName = "test-idx";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        ensureGreen();
        // Prevalidate removal of one of the two nodes
        String nodeName = randomFrom(node1, node2);
        PrevalidateNodeRemovalRequest.Builder req = PrevalidateNodeRemovalRequest.builder();
        switch (randomIntBetween(0, 2)) {
            case 0 -> req.setNames(nodeName);
            case 1 -> req.setIds(internalCluster().clusterService(nodeName).localNode().getId());
            case 2 -> req.setExternalIds(internalCluster().clusterService(nodeName).localNode().getExternalId());
            default -> throw new IllegalStateException("Unexpected value");
        }
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req.build()).get();
        assertThat(resp.getPrevalidation().getResult().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
        assertThat(resp.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().getNodes().get(0);
        assertNotNull(nodeResult);
        assertThat(nodeResult.name(), equalTo(nodeName));
        assertThat(nodeResult.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
        // Enforce a replica to get unassigned
        updateIndexSettings(indexName, Settings.builder().put("index.routing.allocation.require._name", node1));
        ensureYellow();
        PrevalidateNodeRemovalRequest req2 = PrevalidateNodeRemovalRequest.builder().setNames(node2).build();
        PrevalidateNodeRemovalResponse resp2 = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req2).get();
        assertThat(resp2.getPrevalidation().getResult().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
        assertThat(resp2.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult2 = resp2.getPrevalidation().getNodes().get(0);
        assertNotNull(nodeResult2);
        assertThat(nodeResult2.name(), equalTo(node2));
        assertThat(nodeResult2.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
    }

    // Test that in case the nodes that are being prevalidated do not contain copies of any of the
    // red shards, their removal is considered to be safe.
    public void testNodeRemovalFromRedClusterWithNoLocalShardCopy() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        // Create an index pinned to one node, and then stop that node so the index is RED.
        String indexName = "test-idx";
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put("index.routing.allocation.require._name", node1).build()
        );
        ensureYellow(indexName);
        internalCluster().stopNode(node1);
        ensureRed(indexName);
        // With a RED non-searchable-snapshot index, node removal is potentially unsafe
        // since that node might have the last copy of the unassigned index.
        PrevalidateNodeRemovalRequest req = PrevalidateNodeRemovalRequest.builder().setNames(node2).build();
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        assertThat(resp.getPrevalidation().getResult().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
        assertThat(resp.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().getNodes().get(0);
        assertThat(nodeResult.name(), equalTo(node2));
        assertThat(nodeResult.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
    }

    public void testNodeRemovalFromRedClusterWithLocalShardCopy() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        String indexName = "test-idx";
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.require._name", node1)
                .build()
        );
        ensureGreen(indexName);
        final CountDownLatch shardActiveRequestSent = new CountDownLatch(1);
        MockTransportService node1transport = (MockTransportService) internalCluster().getInstance(TransportService.class, node1);
        TransportService node2transport = internalCluster().getInstance(TransportService.class, node2);
        node1transport.addSendBehavior(node2transport, (connection, requestId, action, request, options) -> {
            if (action.equals(IndicesStore.ACTION_SHARD_EXISTS)) {
                shardActiveRequestSent.countDown();
                logger.info("prevent shard active request from being sent");
                throw new ConnectTransportException(connection.getNode(), "DISCONNECT: simulated");
            }
            connection.sendRequest(requestId, action, request, options);
        });
        logger.info("--> move shard from {} to {}, and wait for relocation to finish", node1, node2);
        updateIndexSettings(indexName, Settings.builder().put("index.routing.allocation.require._name", node2));
        shardActiveRequestSent.await();
        ensureGreen(indexName);
        internalCluster().stopNode(node2);
        ensureRed(indexName);
        // Ensure that node1 still has data for the unassigned index
        NodeEnvironment nodeEnv = internalCluster().getInstance(NodeEnvironment.class, node1);
        Index index = internalCluster().clusterService().state().metadata().index(indexName).getIndex();
        assertTrue("local index shards not found", FileSystemUtils.exists(nodeEnv.indexPaths(index)));
        PrevalidateNodeRemovalRequest req = PrevalidateNodeRemovalRequest.builder().setNames(node1).build();
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        NodesRemovalPrevalidation.Result prevalidationResult = resp.getPrevalidation().getResult();
        String node1Id = internalCluster().clusterService(node1).localNode().getId();
        assertThat(prevalidationResult.isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.NO));
        assertThat(prevalidationResult.reason(), equalTo("nodes with the following IDs contain copies of red shards: [" + node1Id + "]"));
        assertThat(resp.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().getNodes().get(0);
        assertThat(nodeResult.name(), equalTo(node1));
        assertThat(nodeResult.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.NO));
        assertThat(nodeResult.result().reason(), equalTo("node contains copies of the following red shards: [[" + indexName + "][0]]"));
    }

    public void testNodeRemovalFromRedClusterWithTimeout() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        String indexName = "test-index";
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put("index.routing.allocation.require._name", node1)
                .build()
        );
        ensureGreen(indexName);
        // make it red!
        internalCluster().stopNode(node1);
        ensureRed(indexName);
        MockTransportService node2TransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node2);
        node2TransportService.addRequestHandlingBehavior(
            TransportCheckShardsOnDataPathAction.ACTION_NAME + "[n]",
            (handler, request, channel, task) -> { logger.info("drop the check shards request"); }
        );
        PrevalidateNodeRemovalRequest req = PrevalidateNodeRemovalRequest.builder()
            .setNames(node2)
            .build()
            .timeout(TimeValue.timeValueSeconds(1));
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        NodesRemovalPrevalidation.Result prevalidationResult = resp.getPrevalidation().getResult();
        assertThat(prevalidationResult.isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.UNKNOWN));
        String node2Id = internalCluster().clusterService(node2).localNode().getId();
        assertThat(prevalidationResult.reason(), equalTo("cannot prevalidate removal of nodes with the following IDs: [" + node2Id + "]"));
        assertThat(resp.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().getNodes().get(0);
        assertThat(nodeResult.name(), equalTo(node2));
        assertThat(nodeResult.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.UNKNOWN));
        assertThat(nodeResult.result().reason(), startsWith("failed contacting the node"));
    }

    private void ensureRed(String indexName) throws Exception {
        assertBusy(() -> {
            ClusterHealthResponse healthResponse = client().admin()
                .cluster()
                .prepareHealth(indexName)
                .setWaitForStatus(ClusterHealthStatus.RED)
                .setWaitForEvents(Priority.LANGUID)
                .execute()
                .actionGet();
            assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        });
    }
}
