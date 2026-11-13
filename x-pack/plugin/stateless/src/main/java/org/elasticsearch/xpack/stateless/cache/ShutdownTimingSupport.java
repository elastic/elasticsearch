/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings.IncomingRecoveryLimits;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/// The inputs that [RelocationSourceShutdownTimeouts] and [RelocationOtherNodeShutdownTimeouts] share when they turn the time left until
/// the grace deadline of a shutting-down node into a warming timeout: the deadline itself, the relocation slots of the target, and the
/// data-volume heuristic.
class ShutdownTimingSupport {

    /// The values of the master settings at which the master leaves the throttling of the recoveries to a search node to the data nodes, as
    /// with data node recovery throttling (DNRT)
    private static final Map<Setting<Integer>, Integer> MASTER_THROTTLING_DISABLED_VALUES = Map.of(
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
        Integer.MAX_VALUE,
        ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
        Integer.MAX_VALUE,
        ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
        -1
    );

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private volatile int maxConcurrentRelocationRecoveries;
    private volatile boolean masterThrottlingDisabled;
    private volatile TimeValue searchRecoveryWarmingGracePeriodCap;
    private volatile double searchRecoveryWarmingCacheRatio;

    ShutdownTimingSupport(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        Settings nodeSettings,
        ByteSizeValue maxHeap
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        DataNodeRecoveryThrottlingSettings.watchIncomingRecoveryLimits(
            clusterSettings,
            nodeSettings,
            maxHeap,
            limits -> this.maxConcurrentRelocationRecoveries = limits.maxConcurrentNonCriticalRecoveries()
        );
        this.masterThrottlingDisabled = masterThrottlingDisabled(clusterSettings::get);
        clusterSettings.addSettingsUpdateConsumer(
            settings -> this.masterThrottlingDisabled = masterThrottlingDisabled(setting -> setting.get(settings)),
            List.copyOf(MASTER_THROTTLING_DISABLED_VALUES.keySet())
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING,
            value -> this.searchRecoveryWarmingGracePeriodCap = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_CACHE_RATIO_SETTING,
            value -> this.searchRecoveryWarmingCacheRatio = value
        );
    }

    long nowMillis() {
        return threadPool.absoluteTimeInMillis();
    }

    /// The time by which the shards of the shutting-down node have to be through: its shutdown start plus its grace period, which is
    /// capped as well as defaulted by [SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING].
    long graceDeadline(SingleNodeShutdownMetadata shutdown) {
        TimeValue grace = shutdown.getGracePeriod();
        if (grace == null) {
            grace = searchRecoveryWarmingGracePeriodCap;
        }
        return shutdown.getStartedAtMillis() + Math.min(grace.getMillis(), searchRecoveryWarmingGracePeriodCap.millis());
    }

    /// The data-volume-proportional heuristic: `remainingMs` scaled by the fraction of the warming cache budget that `totalBytesToWarm`
    /// takes, or `0` when there is no warming cache. The baseline (the warming cache budget) is fixed, since it's hard to account for
    /// the bytes warmed by all the shards relocating from a given shutting-down node.
    double dataVolumeShareMs(long totalBytesToWarm, long remainingMs) {
        final long warmingCacheBytes = Math.round(cacheService.getCacheSize() * searchRecoveryWarmingCacheRatio);
        return warmingCacheBytes > 0 ? ((double) totalBytesToWarm / warmingCacheBytes) * remainingMs : 0;
    }

    /// The slots known to be available to the relocations to the target: the data node limit, or, while the master throttles and reserves
    /// no slots, only the ones in use by the relocations in flight.
    int knownRelocationSlots(final int allRelocationsToTarget) {
        return dataNodeRecoveryThrottlingEnabled() ? maxConcurrentRelocationRecoveries : allRelocationsToTarget;
    }

    /// The data nodes throttle the relocations, and not the master, when their non-critical limit is finite and the master does not
    /// throttle, see [#masterThrottlingDisabled]. The data node settings are only registered by the stateless plugin, and are unbounded by
    /// default. While the master throttles, the cluster state shows only the relocations it has started, whatever the data node limit is.
    // TODO: move to DataNodeRecoveryThrottlingSettings (elastic/elasticsearch-team#5653)
    boolean dataNodeRecoveryThrottlingEnabled() {
        return maxConcurrentRelocationRecoveries < IncomingRecoveryLimits.UNBOUNDED.maxConcurrentNonCriticalRecoveries()
            && masterThrottlingDisabled;
    }

    private static boolean masterThrottlingDisabled(final Function<Setting<Integer>, Integer> currentValue) {
        return MASTER_THROTTLING_DISABLED_VALUES.entrySet()
            .stream()
            .allMatch(disabledValue -> disabledValue.getValue().equals(currentValue.apply(disabledValue.getKey())));
    }
}
