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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrevalidateNodeRemovalIT extends ESIntegTestCase {

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
        assertTrue(resp.getPrevalidation().isSafe());
        assertThat(resp.getPrevalidation().nodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().nodes().get(0);
        assertNotNull(nodeResult);
        assertThat(nodeResult.name(), equalTo(nodeName));
        assertTrue(nodeResult.result().isSafe());
        // Enforce a replica to get unassigned
        updateIndexSettings(indexName, Settings.builder().put("index.routing.allocation.require._name", node1));
        ensureYellow();
        PrevalidateNodeRemovalRequest req2 = PrevalidateNodeRemovalRequest.builder().setNames(node2).build();
        PrevalidateNodeRemovalResponse resp2 = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req2).get();
        assertTrue(resp2.getPrevalidation().isSafe());
        assertThat(resp2.getPrevalidation().nodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult2 = resp2.getPrevalidation().nodes().get(0);
        assertNotNull(nodeResult2);
        assertThat(nodeResult2.name(), equalTo(node2));
        assertTrue(nodeResult2.result().isSafe());
    }

    public void testNodeRemovalFromRedCluster() throws Exception {
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
        // With a RED non-searchable-snapshot index, node removal is potentially unsafe
        // since that node might have the last copy of the unassigned index.
        PrevalidateNodeRemovalRequest req = PrevalidateNodeRemovalRequest.builder().setNames(node2).build();
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        assertFalse(resp.getPrevalidation().isSafe());
        assertThat(resp.getPrevalidation().message(), equalTo("cluster health is RED"));
        assertThat(resp.getPrevalidation().nodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().nodes().get(0);
        assertThat(nodeResult.name(), equalTo(node2));
        assertFalse(nodeResult.result().isSafe());
    }
}
