/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;

import java.util.function.Supplier;

/**
 * Default {@link WarmingRatioProviderFactory} when no SPI implementation is registered: uses
 * {@link DefaultWarmingRatioProviderFactory#SEARCH_RECOVERY_WARMING_RATIO_SETTING} (default 0 = no BCC warming by ratio).
 *
 * The ratio is read once when the provider is created (once per search shard recovery), so it is stable for that recovery.
 *
 * This is a temporary means until we define the warming ratio or search power properly for stateless
 */
public class DefaultWarmingRatioProviderFactory implements WarmingRatioProviderFactory {

    /**
     * When no {@link WarmingRatioProviderFactory} SPI is registered, fraction of each compound commit to warm during search recovery
     * BCC warming (0 = off, 1 = full). Default 0. Ignored when an SPI implementation is present.
     */
    public static final Setting<Double> SEARCH_RECOVERY_WARMING_RATIO_SETTING = Setting.doubleSetting(
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_ratio",
        0.0d,
        0.0d,
        1.0d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Supplier<ClusterSettings> clusterSettings;

    /**
     * @param clusterSettings resolved on every {@link #create}, as the factory is instantiated before the cluster service is available
     */
    public DefaultWarmingRatioProviderFactory(Supplier<ClusterSettings> clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    @Override
    public WarmingRatioProvider create(ShardId shardId) {
        return new DefaultWarmingRatioProvider(clusterSettings.get());
    }

    static final class DefaultWarmingRatioProvider implements WarmingRatioProvider {

        private final double ratio;

        DefaultWarmingRatioProvider(ClusterSettings clusterSettings) {
            if (clusterSettings.get(SEARCH_RECOVERY_WARMING_RATIO_SETTING.getKey()) != null) {
                ratio = clusterSettings.get(SEARCH_RECOVERY_WARMING_RATIO_SETTING);
            } else {
                ratio = 0.0d;
            }
        }

        @Override
        public double getWarmingRatio(
            @Nullable StatelessCompoundCommit.TimestampFieldValueRange timestampFieldValueRange,
            long resolvedCCTimestampMillis,
            long nowMillis
        ) {
            return ratio;
        }
    }
}
