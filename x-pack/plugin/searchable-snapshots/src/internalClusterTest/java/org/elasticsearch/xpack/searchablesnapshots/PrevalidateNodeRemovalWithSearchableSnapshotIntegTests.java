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
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrevalidateNodeRemovalWithSearchableSnapshotIntegTests extends BaseSearchableSnapshotsIntegTestCase {

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
        assertAcked(indicesAdmin().prepareDelete(indexName));
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
            ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth(restoredIndexName)
                .setWaitForStatus(ClusterHealthStatus.RED)
                .setWaitForEvents(Priority.LANGUID)
                .execute()
                .actionGet();
            assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        });
        // Assert that removal of any node from the red cluster is safe
        PrevalidateNodeRemovalRequest.Builder req = PrevalidateNodeRemovalRequest.builder();
        switch (randomIntBetween(0, 2)) {
            case 0 -> req.setNames(node2);
            case 1 -> req.setIds(internalCluster().clusterService(node2).localNode().getId());
            case 2 -> req.setExternalIds(internalCluster().clusterService(node2).localNode().getExternalId());
            default -> throw new IllegalStateException("Unexpected value");
        }
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req.build()).get();
        assertTrue(resp.getPrevalidation().isSafe());
        assertThat(resp.getPrevalidation().message(), equalTo("all red indices are searchable snapshot indices"));
        assertThat(resp.getPrevalidation().nodes().size(), equalTo(1));
        NodesRemovalPrevalidation.NodeResult nodeResult = resp.getPrevalidation().nodes().get(0);
        assertNotNull(nodeResult);
        assertThat(nodeResult.name(), equalTo(node2));
        assertThat(nodeResult.Id(), not(emptyString()));
        assertThat(nodeResult.externalId(), not(emptyString()));
        assertTrue(nodeResult.result().isSafe());
        assertThat(nodeResult.result().reason(), equalTo(NodesRemovalPrevalidation.Reason.NO_RED_SHARDS_EXCEPT_SEARCHABLE_SNAPSHOTS));
        assertThat(nodeResult.result().message(), equalTo(""));
    }
}
