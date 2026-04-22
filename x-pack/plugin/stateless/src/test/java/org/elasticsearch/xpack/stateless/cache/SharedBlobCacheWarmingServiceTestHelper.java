/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
