/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalAction;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrevalidateNodeRemovalWithSearchableSnapshotTests extends BaseSearchableSnapshotsIntegTestCase {

    public void testNodeRemovalFromClusterWihRedSearchableSnapshotIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        final String repoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createIndexWithContent(indexName, indexSettingsNoReplicas(1).put(INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        createRepository(repoName, "fs");
        createSnapshot(repoName, snapshotName, List.of(indexName));
        assertAcked(client().admin().indices().prepareDelete(indexName));
        // Pin the searchable snapshot index to one node
        final String restoredIndexName = mountSnapshot(
            repoName,
            snapshotName,
            indexName,
            Settings.builder().put("index.routing.allocation.require._name", node1).build()
        );
        ensureGreen(restoredIndexName);
        // Make sure the searchable snapshot index is red
        internalCluster().stopNode(node1);
        assertBusy(() -> {
            ClusterHealthResponse healthResponse = client().admin()
                .cluster()
                .prepareHealth(restoredIndexName)
                .setWaitForStatus(ClusterHealthStatus.RED)
                .setWaitForEvents(Priority.LANGUID)
                .execute()
                .actionGet();
            assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        });
        // Assert that removal of any node from the red cluster is safe
        PrevalidateNodeRemovalRequest req = new PrevalidateNodeRemovalRequest(node2);
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();
        assertThat(resp.getPrevalidation().getResult().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
        assertThat(resp.getPrevalidation().getNodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().getNodes().get(0);
        assertNotNull(nodeResult);
        assertThat(nodeResult.name(), equalTo(node2));
        assertThat(nodeResult.result().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
    }
}
