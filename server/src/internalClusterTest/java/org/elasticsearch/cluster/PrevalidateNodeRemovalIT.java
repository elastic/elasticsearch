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
        String nodeName = randomFrom(node1, node2);
        PrevalidateNodeRemovalRequest req = new PrevalidateNodeRemovalRequest(nodeName);
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();

        assertThat(resp.getPrevalidation().getResult().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
        assertThat(resp.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().getNodes().get(0);
        assertNotNull(nodeResult);
        assertThat(nodeResult.name(), equalTo(nodeName));
        assertThat(nodeResult.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));

        // Enforce a replica to get unassigned
        updateIndexSettings(indexName, Settings.builder().put("index.routing.allocation.require._name", node1));
        ensureYellow();
        PrevalidateNodeRemovalRequest req2 = new PrevalidateNodeRemovalRequest(node2);
        PrevalidateNodeRemovalResponse resp2 = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req2).get();

        assertThat(resp2.getPrevalidation().getResult().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
        assertThat(resp2.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult2 = resp2.getPrevalidation().getNodes().get(0);
        assertNotNull(nodeResult2);
        assertThat(nodeResult2.name(), equalTo(node2));
        assertThat(nodeResult2.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
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
        // With a RED non-searchable-snapshot index, node removal safety is unknown
        // since that node might have the last copy of the unassigned index.
        PrevalidateNodeRemovalRequest req = new PrevalidateNodeRemovalRequest(node2);
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        assertThat(resp.getPrevalidation().getResult().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.UNKNOWN));
        assertThat(resp.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().getNodes().get(0);
        assertThat(nodeResult.name(), equalTo(node2));
        assertThat(nodeResult.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.UNKNOWN));
    }
}
