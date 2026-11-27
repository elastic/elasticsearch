/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.WarmTarget;
import org.elasticsearch.xpack.stateless.commits.BccBlob;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;

import java.util.Map;

import static org.elasticsearch.test.NodeRoles.onlyRole;

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
        Map<String, WarmTaskInfo> warmTasksForBCCs
    ) {
        return new SharedBlobCacheWarmingService(
            cacheService,
            threadPool,
            telemetryProvider,
            clusterSettings,
            new SearchRecoveryTimeoutCalculationService(
                cacheService,
                threadPool,
                clusterSettings,
                onlyRole(DiscoveryNodeRole.SEARCH_ROLE),
                ByteSizeValue.ZERO,
                TelemetryProvider.NOOP
            )
        ) {
            @Override
            protected void scheduleWarmingTask(AbstractWarmingTask task) {
                if (task instanceof AbstractWarmer.WarmBlobByteRangeTask warmTask) {
                    warmTasksForBCCs.put(
                        warmTask.bccBlob.blobName(),
                        new WarmTaskInfo(warmTask.bccBlob.blobName(), warmTask.byteRangeToWarm)
                    );
                }
                super.scheduleWarmingTask(task);
            }
        };
    }

    /**
     * Calls the protected {@link SharedBlobCacheWarmingService#warmSearchOffline} with a listener.
     */
    public static void warmSearchOffline(
        SharedBlobCacheWarmingService warmingService,
        IndexShard indexShard,
        StatelessCompoundCommit commit,
        BlobStoreCacheDirectory directory,
        WarmingRatioProvider warmingRatioProvider,
        @Nullable Map<BccBlob, WarmTarget> endTargetsToWarm,
        ActionListener<Void> listener
    ) {
        warmingService.warmSearchOffline(indexShard, commit, directory, warmingRatioProvider, endTargetsToWarm, listener);
    }
}
