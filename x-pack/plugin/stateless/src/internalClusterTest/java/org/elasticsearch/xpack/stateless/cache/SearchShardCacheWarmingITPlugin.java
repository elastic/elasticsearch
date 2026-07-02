/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESIntegTestCase.assertBusy;
import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * For {@link SearchShardCacheWarmingIT} only: registers {@link DefaultWarmingRatioProviderFactory#SEARCH_RECOVERY_WARMING_RATIO_SETTING}
 * and installs {@link DelayWarmCacheUntilShardStartedService} so search recovery warming can be exercised after the timeout race while
 * avoiding starving the prewarm pool (see {@link DelayWarmCacheUntilShardStartedService}).
 */
public final class SearchShardCacheWarmingITPlugin extends TestUtils.StatelessPluginWithTrialLicense {

    public SearchShardCacheWarmingITPlugin(Settings settings) {
        super(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        var settings = new ArrayList<>(super.getSettings());
        settings.add(DefaultWarmingRatioProviderFactory.SEARCH_RECOVERY_WARMING_RATIO_SETTING);
        return List.copyOf(settings);
    }

    @Override
    protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        ClusterSettings clusterSettings,
        WarmingRatioProvider warmingRatioProvider
    ) {
        return new DelayWarmCacheUntilShardStartedService(
            cacheService,
            threadPool,
            telemetryProvider,
            clusterSettings,
            warmingRatioProvider
        );
    }

    /**
     * Defers {@link SharedBlobCacheWarmingService#warmCache} body until {@link IndexShardState#STARTED} for the internal
     * replicated-files search path ({@code endOffsetsToWarm} non-null). The search recovery warming timeout can still fire and resume
     * recovery on the {@code race} listener while this method waits, so the test can assert recovery completes without relying on filling
     * the prewarm pool.
     */
    private static final class DelayWarmCacheUntilShardStartedService extends SharedBlobCacheWarmingService {

        /**
         * Snapshot for gating: only {@link SearchShardCacheWarmingIT#searchNodeCacheSettingsWithFastNonRelocationWarmingTimeout()} uses a
         * sub-100ms non-relocation timeout; other ITs leave minutes-long defaults so we must not defer {@link #warmCache} there.
         */
        private final ClusterSettings clusterSettings;

        DelayWarmCacheUntilShardStartedService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            ClusterSettings clusterSettings,
            WarmingRatioProvider warmingRatioProvider
        ) {
            super(cacheService, threadPool, telemetryProvider, clusterSettings, warmingRatioProvider);
            this.clusterSettings = clusterSettings;
        }

        @Override
        protected void warmCache(
            Type type,
            IndexShard indexShard,
            StatelessCompoundCommit commit,
            BlobStoreCacheDirectory directory,
            @Nullable Map<BlobFile, Long> endOffsetsToWarm,
            boolean preWarmForIdLookup,
            ActionListener<Void> listener
        ) {
            if (type == Type.SEARCH && endOffsetsToWarm != null && listener != ActionListener.<Void>noop()) {
                TimeValue nonRelocation = clusterSettings.get(
                    SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING
                );
                if (nonRelocation.millis() <= 100L) {
                    try {
                        assertBusy(() -> assertThat(indexShard.state(), equalTo(IndexShardState.STARTED)));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }
            super.warmCache(type, indexShard, commit, directory, endOffsetsToWarm, preWarmForIdLookup, listener);
        }
    }
}
