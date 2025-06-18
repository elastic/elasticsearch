/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.BeforeClass;

import java.util.Locale;
import java.util.stream.IntStream;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MergeWithLowDiskSpaceIT extends DiskUsageIntegTestCase {

    protected static long MERGE_DISK_HIGH_WATERMARK_BYTES;

    @BeforeClass
    public static void setAvailableDiskSpaceBufferLimit() throws Exception {
        MERGE_DISK_HIGH_WATERMARK_BYTES = randomLongBetween(10_000L, 100_000L);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // only the threadpool-based merge scheduler has the capability to block merges when disk space is insufficient
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            // the default of "5s" slows down testing
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), MERGE_DISK_HIGH_WATERMARK_BYTES + "b")
            .build();
    }

    public void testShardCloseWhenDiskSpaceInsufficient() {
        String node = internalCluster().startNode();
        var indicesService = internalCluster().getInstance(IndicesService.class, node);
        ensureStableCluster(1);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        indexRandom(
            true,
            IntStream.range(1, 10)
                .mapToObj(i -> prepareIndex(indexName).setSource("field", randomAlphaOfLength(50)))
                .toArray(IndexRequestBuilder[]::new)
        );
        forceMerge();
        refresh();
        assertFalse(indicesService.getThreadPoolMergeExecutorService().isMergingBlockedDueToInsufficientDiskSpace());

        IndicesStatsResponse stats = indicesAdmin().prepareStats().clear().setStore(true).get();
        long used = stats.getTotal().getStore().sizeInBytes();
        // the next merge will surely cross the disk watermark that's just 1 byte ahead
        long enoughSpace = used + 1L + MERGE_DISK_HIGH_WATERMARK_BYTES;
        setTotalSpace(node, enoughSpace);
        assertFalse(indicesService.getThreadPoolMergeExecutorService().isMergingBlockedDueToInsufficientDiskSpace());
    }

    public void setTotalSpace(String dataNodeName, long totalSpace) {
        getTestFileStore(dataNodeName).setTotalSpace(totalSpace);
        refreshClusterInfo();
    }
}
