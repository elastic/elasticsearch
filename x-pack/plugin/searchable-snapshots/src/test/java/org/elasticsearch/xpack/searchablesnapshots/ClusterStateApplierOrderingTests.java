/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.routing.UnassignedInfo.Reason.ALLOCATION_FAILED;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class ClusterStateApplierOrderingTests extends BaseSearchableSnapshotsIntegTestCase {

    public void testRepositoriesServiceClusterStateApplierIsCalledBeforeIndicesClusterStateService() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        // The scenario is easily reproducible with a 2 node (data and master) cluster
        internalCluster().setBootstrapMasterNodeIndex(1);
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        final Path repo = randomRepoPath();
        assertAcked(
            client().admin().cluster().preparePutRepository(fsRepoName).setType("fs").setSettings(Settings.builder().put("location", repo))
        );

        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));

        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = between(10, 10_000); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"));
        }
        indexRandom(true, true, indexRequestBuilders);
        refresh(indexName);

        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(fsRepoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotInfo.snapshotId().getName(),
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        ensureGreen(restoredIndexName);

        // In order to reproduce this issue we need to force a full cluster restart so the new elected master
        // sends the entire ClusterState in one message, including assigned shards and repositories.
        internalCluster().fullRestart();

        List<UnassignedInfo.Reason> unassignedReasons = new ArrayList<>();
        internalCluster().clusterService().addListener(event -> {
            if (event.routingTableChanged()) {
                for (RoutingNode routingNode : event.state().getRoutingNodes()) {
                    for (ShardRouting shardRouting : routingNode) {
                        if (shardRouting.unassignedInfo() != null) unassignedReasons.add(shardRouting.unassignedInfo().getReason());
                    }
                }
            }
        });

        ensureGreen(restoredIndexName);

        assertThat("Unexpected shard allocation failure", unassignedReasons.stream().noneMatch(r -> r == ALLOCATION_FAILED), is(true));
    }
}
