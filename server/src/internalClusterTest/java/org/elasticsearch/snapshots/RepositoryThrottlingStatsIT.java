/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class RepositoryThrottlingStatsIT extends AbstractSnapshotIntegTestCase {

    public void testRepositoryThrottlingStats() throws Exception {

        logger.info("--> starting a node");
        internalCluster().startNode();

        logger.info("--> create index");
        createIndexWithRandomDocs("test-idx", 100);

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
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setRenamePattern("test-")
            .setRenameReplacement("test2-")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertDocCount("test-idx", 100);

        logger.info("--> access repository throttling stats via _nodes/stats api");
        NodesStatsResponse response = clusterAdmin().prepareNodesStats().setRepositoryStats(true).get();
        RepositoriesStats stats = response.getNodes().get(0).getRepositoriesStats();

        assertTrue(stats.getRepositoryThrottlingStats().containsKey("test-repo"));
        assertTrue(stats.getRepositoryThrottlingStats().get("test-repo").totalWriteThrottledNanos() > 0);
        assertTrue(stats.getRepositoryThrottlingStats().get("test-repo").totalReadThrottledNanos() > 0);

    }
}
