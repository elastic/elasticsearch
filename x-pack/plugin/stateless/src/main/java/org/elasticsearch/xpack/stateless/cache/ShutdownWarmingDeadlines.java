/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;

/// The time by which the shards of a shutting-down node have to be through, which bounds how long the warming of the shards relocating
/// from, or competing with, that node may take.
class ShutdownWarmingDeadlines {

    private volatile TimeValue searchRecoveryWarmingGracePeriodCap;

    ShutdownWarmingDeadlines(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING,
            value -> this.searchRecoveryWarmingGracePeriodCap = value
        );
    }

    /// The shutdown start of `shutdown` plus its grace period, which is capped as well as defaulted by
    /// [SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING].
    long of(SingleNodeShutdownMetadata shutdown) {
        TimeValue grace = shutdown.getGracePeriod();
        if (grace == null) {
            grace = searchRecoveryWarmingGracePeriodCap;
        }
        return shutdown.getStartedAtMillis() + Math.min(grace.getMillis(), searchRecoveryWarmingGracePeriodCap.millis());
    }
}
