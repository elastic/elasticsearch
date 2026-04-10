/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;

import java.util.Map;

/**
 * Test helper for {@link SharedBlobCacheWarmingService}.
 */
public final class SharedBlobCacheWarmingServiceTestHelper {

    private SharedBlobCacheWarmingServiceTestHelper() {}

    public record WarmTaskInfo(String blobName, ByteRange byteRangeToWarm) {}

    /**
     * Creates a {@link SharedBlobCacheWarmingService} that captures warming tasks into the provided map.
     */
    public static SharedBlobCacheWarmingService createInstrumentedWarmingService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        ClusterSettings clusterSettings,
        WarmingRatioProvider warmingRatioProvider,
        Map<String, WarmTaskInfo> warmTasksForBCCs
    ) {
        return new SharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, clusterSettings, warmingRatioProvider) {
            @Override
            protected void scheduleWarmingTask(ActionListener<Releasable> task) {
                if (task instanceof AbstractWarmer.WarmBlobByteRangeTask warmTask) {
                    warmTasksForBCCs.put(
                        warmTask.blobFile.blobName(),
                        new WarmTaskInfo(warmTask.blobFile.blobName(), warmTask.byteRangeToWarm)
                    );
                }
                super.scheduleWarmingTask(task);
            }
        };
    }

    /**
     * Calls the protected {@link SharedBlobCacheWarmingService#warmCache} with a listener.
     */
    public static void warmCache(
        SharedBlobCacheWarmingService warmingService,
        SharedBlobCacheWarmingService.Type type,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        @Nullable Map<BlobFile, Long> endOffsetsToWarm,
        boolean preWarmForIdLookup,
        ActionListener<Void> listener
    ) {
        warmingService.warmCache(type, indexShard, commit, directory, endOffsetsToWarm, preWarmForIdLookup, listener);
    }
}
