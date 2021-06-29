/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.routing.UnassignedInfo.Reason.ALLOCATION_FAILED;
import static org.elasticsearch.gateway.GatewayService.RECOVER_AFTER_DATA_NODES_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 2)
public class ClusterStateApplierOrderingTests extends BaseSearchableSnapshotsIntegTestCase {

    public void testRepositoriesServiceClusterStateApplierIsCalledBeforeIndicesClusterStateService() throws Exception {
        final String fsRepoName = "fsrepo";
        final String indexName = "test-index";
        final String restoredIndexName = "restored-index";

        createRepository(fsRepoName, "fs");

        // Peer recovery always copies .liv files but we do not permit writing to searchable snapshot directories so this doesn't work, but
        // we can bypass this by forcing soft deletes to be used. TODO this restriction can be lifted when #55142 is resolved.
        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));

        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = between(10, 10_000); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"));
        }
        indexRandom(true, true, indexRequestBuilders);
        refresh(indexName);

        final SnapshotInfo snapshotInfo = createFullSnapshot(fsRepoName, "snapshot");
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotInfo.snapshotId().getName(),
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true,
            MountSearchableSnapshotRequest.Storage.FULL_COPY
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(restoredIndexName);

        // In order to reproduce this issue we need to force a full cluster restart so the new elected master
        // sends the entire ClusterState in one message, including assigned shards and repositories.
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                // make sure state is not recovered until a third node joins
                return Settings.builder().put(RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 3).build();
            }
        });

        List<UnassignedInfo.Reason> unassignedReasons = new ArrayList<>();
        internalCluster().clusterService().addListener(event -> {
            if (event.routingTableChanged()) {
                for (RoutingNode routingNode : event.state().getRoutingNodes()) {
                    for (ShardRouting shardRouting : routingNode) {
                        if (shardRouting.unassignedInfo() != null) {
                            unassignedReasons.add(shardRouting.unassignedInfo().getReason());
                        }
                    }
                }
            }
        });

        internalCluster().ensureAtLeastNumDataNodes(3);

        ensureGreen(restoredIndexName);
        assertThat("Unexpected shard allocation failure", unassignedReasons.stream().noneMatch(r -> r == ALLOCATION_FAILED), is(true));
    }
}
