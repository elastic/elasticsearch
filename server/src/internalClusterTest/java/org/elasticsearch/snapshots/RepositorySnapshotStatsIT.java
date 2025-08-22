/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.RepositoriesStats;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class RepositorySnapshotStatsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Make upload time more accurate
            .put(ESTIMATED_TIME_INTERVAL_SETTING.getKey(), "0s")
            .build();
    }

    public void testRepositorySnapshotStats() {

        logger.info("--> starting a node");
        internalCluster().startNode();

        logger.info("--> create index");
        final int numberOfShards = randomIntBetween(2, 6);
        createIndex("test-idx", numberOfShards, 0);
        ensureGreen();
        indexRandomDocs("test-idx", 100);

        IndicesStatsResponse indicesStats = indicesAdmin().prepareStats("test-idx").get();
        IndexStats indexStats = indicesStats.getIndex("test-idx");
        long totalSizeInBytes = 0;
        for (ShardStats shard : indexStats.getShards()) {
            totalSizeInBytes += shard.getStats().getStore().sizeInBytes();
        }
        logger.info("--> total shards size: {} bytes", totalSizeInBytes);

        logger.info("--> create repository with really low snapshot/restore rate-limits");
        createRepository(
            "test-repo",
            "fs",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", false)
                // set rate limits at ~25% of total size
                .put("max_snapshot_bytes_per_sec", ByteSizeValue.ofBytes(totalSizeInBytes / 4))
                .put("max_restore_bytes_per_sec", ByteSizeValue.ofBytes(totalSizeInBytes / 4))
        );

        logger.info("--> create snapshot");
        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));

        logger.info("--> restore from snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(
            TEST_REQUEST_TIMEOUT,
            "test-repo",
            "test-snap"
        ).setRenamePattern("test-").setRenameReplacement("test2-").setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertDocCount("test-idx", 100);

        logger.info("--> access repository throttling stats via _nodes/stats api");
        NodesStatsResponse response = clusterAdmin().prepareNodesStats().setRepositoryStats(true).get();
        RepositoriesStats stats = response.getNodes().get(0).getRepositoriesStats();

        // These are just broad sanity checks on the values. There are more detailed checks in SnapshotMetricsIT
        assertTrue(stats.getRepositorySnapshotStats().containsKey("test-repo"));
        RepositoriesStats.SnapshotStats snapshotStats = stats.getRepositorySnapshotStats().get("test-repo");
        assertThat(snapshotStats.totalWriteThrottledNanos(), greaterThan(0L));
        assertThat(snapshotStats.totalReadThrottledNanos(), greaterThan(0L));
        assertThat(snapshotStats.shardSnapshotsStarted(), equalTo((long) numberOfShards));
        assertThat(snapshotStats.shardSnapshotsCompleted(), equalTo((long) numberOfShards));
        assertThat(snapshotStats.shardSnapshotsInProgress(), equalTo(0L));
        assertThat(snapshotStats.numberOfBlobsUploaded(), greaterThan(0L));
        assertThat(snapshotStats.numberOfBytesUploaded(), greaterThan(0L));
        assertThat(snapshotStats.totalUploadTimeInMillis(), greaterThan(0L));
        assertThat(snapshotStats.totalUploadReadTimeInMillis(), greaterThan(0L));
        assertThat(snapshotStats.totalUploadReadTimeInMillis(), lessThanOrEqualTo(snapshotStats.totalUploadTimeInMillis()));
    }
}
