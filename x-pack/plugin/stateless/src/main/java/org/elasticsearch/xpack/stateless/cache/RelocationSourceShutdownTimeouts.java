/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;

/// The warming timeouts of search shards that relocate from a source node that is shutting down, which depend on the grace deadline of
/// the source and on the relocation slots of the target.
///
/// It owns the settings and the cluster state accounting that only these timeouts use, so that
/// [SearchRecoveryTimeoutCalculationService] reads as a choice of the timeout context.
class RelocationSourceShutdownTimeouts {

    private final NonCriticalRecoverySlots nonCriticalRecoverySlots;
    private final ShutdownWarmingDeadlines shutdownWarmingDeadlines;
    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private volatile double searchRecoveryWarmingCacheRatio;
    private volatile double searchRecoveryWarmingSourceShutdownShareFactor;
    private volatile boolean searchRecoveryWarmingFilterSourceShardsByTarget;

    RelocationSourceShutdownTimeouts(
        NonCriticalRecoverySlots nonCriticalRecoverySlots,
        ShutdownWarmingDeadlines shutdownWarmingDeadlines,
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        ClusterSettings clusterSettings
    ) {
        this.nonCriticalRecoverySlots = nonCriticalRecoverySlots;
        this.shutdownWarmingDeadlines = shutdownWarmingDeadlines;
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_CACHE_RATIO_SETTING,
            value -> this.searchRecoveryWarmingCacheRatio = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING,
            value -> this.searchRecoveryWarmingSourceShutdownShareFactor = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_FILTER_SOURCE_SHARDS_BY_TARGET_SETTING,
            value -> this.searchRecoveryWarmingFilterSourceShardsByTarget = value
        );
    }

    /// Returns the warming timeout for a shard whose relocation source is shutting down, as the maximum of two heuristics:
    ///
    /// 1. _Equal-share_: `factor * (deadline - now) / shardsOnSource * concurrentRelocations`, ensuring every
    /// shard on the shutting-down source gets a fair slice of the remaining grace period.
    /// 2. _Data-volume-proportional_ (contributes only when `totalBytesToWarm` is greater than zero): the fraction of the
    /// node's warming cache budget consumed by this shard's data multiplied by the remaining time,
    /// i.e. `(totalBytesToWarm / (cacheSize * cacheRatio)) * remaining * concurrentRelocations`.
    ///
    /// with `deadline = start + min(metadata grace, cap)`. The timeout is capped at the remaining grace period.
    ///
    /// `concurrentRelocations` is this source's share of the slots known to be available to the target's relocations, see
    /// [#estimateConcurrentRelocations]. It may be below one slot, so the timeout may round down to zero (skipping warming). This is a
    /// rough estimate, and an upper bound: it ignores the other recoveries competing for the target's slots.
    ///
    /// The heuristics above decide the first plan of the shutdown phase. Only equal-share plans are
    /// [SearchRecoveryTimeout#extendable]. A re-evaluation of an equal-share plan (`previousPlan`) always yields an equal-share
    /// plan that only extends the wait by the time saved since then, regardless of which heuristic would win, see
    /// [SearchRecoveryTimeoutCalculationService#searchRecoveryTimeout].
    /// When all shards fit in the target's recovery slots, nobody waits behind this one, so the extension is all the remaining grace.
    SearchRecoveryTimeout timeout(
        ClusterState state,
        String sourceNodeId,
        String targetNodeId,
        long totalBytesToWarm,
        @Nullable SearchRecoveryTimeout previousPlan
    ) {
        final var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        assert shutdown != null;
        final long now = threadPool.absoluteTimeInMillis();
        final long deadline = shutdownWarmingDeadlines.of(shutdown);
        final long remaining = deadline - now;
        if (remaining <= 0) {
            return new SearchRecoveryTimeout(TimeValue.ZERO, TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_GRACE_ELAPSED);
        }
        final int shardsOnSource = Math.max(
            1,
            searchRecoveryWarmingFilterSourceShardsByTarget
                ? countShardsPotentiallyRelocatingToTarget(state, sourceNodeId, targetNodeId)
                : countShardsOnNode(state, sourceNodeId)
        );
        final double equalShareMs = (remaining / (double) shardsOnSource) * searchRecoveryWarmingSourceShutdownShareFactor;

        // Re-evaluations pass the bytes still to warm (an approximation, see SearchRecoveryTimeoutCalculationService#extendedTimeout),
        // the first calculation passes all of them.
        final double dataVolumeMs = dataVolumeShareMs(totalBytesToWarm, remaining);

        final double concurrentRelocations = estimateConcurrentRelocations(state, sourceNodeId, targetNodeId, shardsOnSource);

        if (previousPlan != null && previousPlan.isEqualSharePlan() && previousPlan.perShardShareMs() > 0) {
            // Re-evaluation of an equal-share plan: every shard still on the source was already budgeted previousPlan.perShardShareMs()
            // when the previous plan was computed, so only the part of the fresh share above that is time saved by shards that finished
            // early. When none did, the fresh share is not larger and there is nothing to extend by. The data-volume heuristic is
            // deliberately not consulted: it only decides the first plan of the shutdown phase, afterwards only saved time is handed out.
            // The exception is when all shards fit in the target's slots: nobody is queued behind this one, so the remaining grace would be
            // wasted if not used, so extend by max 1 minute on every reevaluation as long as the conditions don't change.
            // n.b. RELOCATING does not imply running, under recovery throttling a shard may still be queued for a slot.
            final boolean noShardWaitingForRelocationSlot = shardsOnSource <= concurrentRelocations;
            final double extensionMs = noShardWaitingForRelocationSlot
                ? Math.min(TimeValue.timeValueMinutes(1).millis(), remaining)
                : Math.max(0.0, equalShareMs - previousPlan.perShardShareMs()) * concurrentRelocations;
            return new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(Math.round(Math.min(remaining, extensionMs))),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
                equalShareMs
            );
        }

        // The decision below is per-shard whereas the two heuristics above assume all shards opt with the same heuristic
        // this is an inherent problem of the fact that, during relocation, we don't know apriori all the shards that are going
        // to be relocated between two given nodes, so we can't know which of the two heuristics is more suitable overall.
        // Though the per-shard local decision here is OKish, because it's all relative to the remaining deadline and shards,
        // so the impact of currently choosing a different heuristic from previous (or future) relocating shards is partially mitigated
        if (dataVolumeMs > equalShareMs) {
            return new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(Math.round(Math.min(remaining, dataVolumeMs * concurrentRelocations))),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME
            );
        } else {
            return new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(Math.round(Math.min(remaining, equalShareMs * concurrentRelocations))),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
                equalShareMs
            );
        }
    }

    /// The data-volume-proportional heuristic: `remainingMs` scaled by the fraction of the warming cache budget that `totalBytesToWarm`
    /// takes, or `0` when there is no warming cache. The baseline (the warming cache budget) is fixed, since it's hard to account for
    /// the bytes warmed by all the shards relocating from a given shutting-down node.
    private double dataVolumeShareMs(long totalBytesToWarm, long remainingMs) {
        final long warmingCacheBytes = Math.round(cacheService.getCacheSize() * searchRecoveryWarmingCacheRatio);
        return warmingCacheBytes > 0 ? ((double) totalBytesToWarm / warmingCacheBytes) * remainingMs : 0;
    }

    /// Returns the estimated number of the shards of `sourceNodeId` that relocate to `targetNodeId` at the same time: the source's share of
    /// the slots known to be available to the relocations from shutting-down nodes, see [NonCriticalRecoverySlots#effectiveSlots], split between the sources
    /// in proportion to their relocations in flight, and not more than `shardsOnSource`. It scales the equal share of one shard to the time
    /// the shards of the source can use the slots together. It can be fractional and below one, when the slots are split between sources.
    /// Without data node recovery throttling, it is the relocations from the source to the target that are in flight.
    private double estimateConcurrentRelocations(
        final ClusterState state,
        final String sourceNodeId,
        final String targetNodeId,
        final int shardsOnSource
    ) {
        final var relocationsToTarget = SearchRelocationsToTarget.count(state, targetNodeId);
        // The current shard is itself one such relocation, floor at 1 in case it is not yet visible on the target's RoutingNode.
        final int relocationsFromSourceToTarget = Math.max(1, relocationsToTarget.fromShuttingDownNodes().getOrDefault(sourceNodeId, 0));
        final int allRelocationsToTarget = Math.max(1, relocationsToTarget.allFromShuttingDownNodes());
        return Math.min(
            shardsOnSource,
            (double) nonCriticalRecoverySlots.effectiveSlots(allRelocationsToTarget) * relocationsFromSourceToTarget
                / allRelocationsToTarget
        );
    }

    private static int countShardsOnNode(ClusterState clusterState, String nodeId) {
        final var node = clusterState.getRoutingNodes().node(nodeId);
        return node == null ? 0 : node.size();
    }

    private static int countShardsPotentiallyRelocatingToTarget(ClusterState clusterState, String sourceNodeId, String targetNodeId) {
        final var sourceNode = clusterState.getRoutingNodes().node(sourceNodeId);
        if (sourceNode == null) {
            return 0;
        }
        int relocatingElsewhere = 0;
        for (ShardRouting shard : sourceNode.relocating()) {
            if (targetNodeId.equals(shard.relocatingNodeId()) == false) {
                relocatingElsewhere++;
            }
        }
        return sourceNode.size() - relocatingElsewhere;
    }
}
