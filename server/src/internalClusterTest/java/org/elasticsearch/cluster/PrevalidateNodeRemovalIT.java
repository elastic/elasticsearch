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
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportPrevalidateShardPathAction;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;
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
        createIndex(indexName, 1, 1);
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
        assertTrue(resp.getPrevalidation().isSafe());
        assertThat(resp.getPrevalidation().message(), equalTo("cluster status is not RED"));
        assertThat(resp.getPrevalidation().nodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().nodes().get(0);
        assertNotNull(nodeResult);
        assertThat(nodeResult.name(), equalTo(nodeName));
        assertThat(nodeResult.result().reason(), equalTo(NodesRemovalPrevalidation.Reason.NO_PROBLEMS));
        assertThat(nodeResult.result().message(), equalTo(""));
        assertTrue(nodeResult.result().isSafe());
        // Enforce a replica to get unassigned
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", node1), indexName);
        ensureYellow();
        PrevalidateNodeRemovalRequest req2 = PrevalidateNodeRemovalRequest.builder().setNames(node2).build();
        PrevalidateNodeRemovalResponse resp2 = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req2).get();
        assertTrue(resp2.getPrevalidation().isSafe());
        assertThat(resp2.getPrevalidation().message(), equalTo("cluster status is not RED"));
        assertThat(resp2.getPrevalidation().nodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult2 = resp2.getPrevalidation().nodes().get(0);
        assertNotNull(nodeResult2);
        assertThat(nodeResult2.name(), equalTo(node2));
        assertTrue(nodeResult2.result().isSafe());
        assertThat(nodeResult2.result().reason(), equalTo(NodesRemovalPrevalidation.Reason.NO_PROBLEMS));
        assertThat(nodeResult2.result().message(), equalTo(""));
    }

    // Test that in case the nodes that are being prevalidated do not contain copies of any of the
    // red shards, their removal is considered to be safe.
    public void testNodeRemovalFromRedClusterWithNoLocalShardCopy() throws Exception {
        internalCluster().startMasterOnlyNode();
        String nodeWithIndex = internalCluster().startDataOnlyNode();
        List<String> otherNodes = internalCluster().startDataOnlyNodes(randomIntBetween(1, 3));
        // Create an index pinned to one node, and then stop that node so the index is RED.
        String indexName = "test-idx";
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put("index.routing.allocation.require._name", nodeWithIndex)
                .build()
        );
        ensureYellow(indexName);
        internalCluster().stopNode(nodeWithIndex);
        ensureRed(indexName);
        String[] otherNodeNames = otherNodes.toArray(new String[otherNodes.size()]);
        PrevalidateNodeRemovalRequest req = PrevalidateNodeRemovalRequest.builder().setNames(otherNodeNames).build();
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        assertTrue(resp.getPrevalidation().isSafe());
        assertThat(resp.getPrevalidation().message(), equalTo(""));
        assertThat(resp.getPrevalidation().nodes().size(), equalTo(otherNodes.size()));
        for (NodesRemovalPrevalidation.NodeResult nodeResult : resp.getPrevalidation().nodes()) {
            assertThat(nodeResult.name(), oneOf(otherNodeNames));
            assertThat(nodeResult.result().reason(), equalTo(NodesRemovalPrevalidation.Reason.NO_RED_SHARDS_ON_NODE));
            assertTrue(nodeResult.result().isSafe());
        }
    }

    public void testNodeRemovalFromRedClusterWithLocalShardCopy() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        String indexName = "test-idx";
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", node1).build());
        ensureGreen(indexName);
        // Prevent node1 from removing its local index shard copies upon removal, by blocking
        // its ACTION_SHARD_EXISTS requests since after a relocation, the source first waits
        // until the shard exists somewhere else, then it removes it locally.
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
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", node2), indexName);
        shardActiveRequestSent.await();
        ensureGreen(indexName);
        // To ensure that the index doesn't get relocated back to node1 after stopping node2, we
        // index a doc to make the index copy on node1 (in case not deleted after the relocation) stale.
        indexDoc(indexName, "some_id", "foo", "bar");
        internalCluster().stopNode(node2);
        ensureRed(indexName);
        // Ensure that node1 still has data for the unassigned index
        NodeEnvironment nodeEnv = internalCluster().getInstance(NodeEnvironment.class, node1);
        Index index = internalCluster().clusterService().state().metadata().index(indexName).getIndex();
        ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnv, new ShardId(index, 0), "");
        assertNotNull("local index shards not found", shardPath);
        // Prevalidate removal of node1
        PrevalidateNodeRemovalRequest req = PrevalidateNodeRemovalRequest.builder().setNames(node1).build();
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        String node1Id = internalCluster().clusterService(node1).localNode().getId();
        assertFalse(resp.getPrevalidation().isSafe());
        assertThat(resp.getPrevalidation().message(), equalTo("removal of the following nodes might not be safe: [" + node1Id + "]"));
        assertThat(resp.getPrevalidation().nodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().nodes().get(0);
        assertThat(nodeResult.name(), equalTo(node1));
        assertFalse(nodeResult.result().isSafe());
        assertThat(nodeResult.result().reason(), equalTo(NodesRemovalPrevalidation.Reason.RED_SHARDS_ON_NODE));
        assertThat(nodeResult.result().message(), equalTo("node contains copies of the following red shards: [[" + indexName + "][0]]"));
    }

    public void testNodeRemovalFromRedClusterWithTimeout() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", node1).build());
        ensureGreen(indexName);
        // make it red!
        internalCluster().stopNode(node1);
        ensureRed(indexName);
        MockTransportService node2TransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, node2);
        node2TransportService.addRequestHandlingBehavior(
            TransportPrevalidateShardPathAction.ACTION_NAME + "[n]",
            (handler, request, channel, task) -> {
                logger.info("drop the check shards request");
            }
        );
        PrevalidateNodeRemovalRequest req = PrevalidateNodeRemovalRequest.builder()
            .setNames(node2)
            .build()
            .timeout(TimeValue.timeValueSeconds(1));
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        assertFalse("prevalidation result should return false", resp.getPrevalidation().isSafe());
        String node2Id = internalCluster().clusterService(node2).localNode().getId();
        assertThat(
            resp.getPrevalidation().message(),
            equalTo("cannot prevalidate removal of nodes with the following IDs: [" + node2Id + "]")
        );
        assertThat(resp.getPrevalidation().nodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().nodes().get(0);
        assertThat(nodeResult.name(), equalTo(node2));
        assertFalse(nodeResult.result().isSafe());
        assertThat(nodeResult.result().message(), startsWith("failed contacting the node"));
        assertThat(nodeResult.result().reason(), equalTo(NodesRemovalPrevalidation.Reason.UNABLE_TO_VERIFY));
    }

    private void ensureRed(String indexName) throws Exception {
        assertBusy(() -> {
            ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth(indexName)
                .setWaitForStatus(ClusterHealthStatus.RED)
                .setWaitForEvents(Priority.LANGUID)
                .execute()
                .actionGet();
            assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        });
    }
}
