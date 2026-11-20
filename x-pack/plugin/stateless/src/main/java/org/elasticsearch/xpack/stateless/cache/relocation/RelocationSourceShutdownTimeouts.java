/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.SourceShardWarmingInfo;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;

import java.util.Map;

/// The warming timeouts of search shards that relocate from a source node that is shutting down, which depend on the grace deadline of
/// the source and on the relocation slots of the target.
///
/// It owns the settings and the cluster state accounting that only these timeouts use, so that
/// [SearchRecoveryTimeoutCalculationService] reads as a choice of the timeout context.
public class RelocationSourceShutdownTimeouts {

    private static final Logger logger = LogManager.getLogger(RelocationSourceShutdownTimeouts.class);

    private final TargetRelocationSlotsEstimator targetRelocationSlotsEstimator;
    private final ShutdownWarmingDeadline shutdownWarmingDeadline;
    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private final LongCounter drainTimeoutHeuristicTotalMetric;
    private volatile double searchRecoveryWarmingSourceShutdownShareFactor;
    private volatile double searchRecoveryWarmingCacheRatio;
    private volatile boolean searchRecoveryWarmingFilterSourceShardsByTarget;

    public RelocationSourceShutdownTimeouts(
        TargetRelocationSlotsEstimator targetRelocationSlotsEstimator,
        ShutdownWarmingDeadline shutdownWarmingDeadline,
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        TelemetryProvider telemetryProvider
    ) {
        this.targetRelocationSlotsEstimator = targetRelocationSlotsEstimator;
        this.shutdownWarmingDeadline = shutdownWarmingDeadline;
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        this.drainTimeoutHeuristicTotalMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter(
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_TOTAL_METRIC,
                "Number of warming timeouts calculated for drain-path search recovery warming, broken down by the ["
                    + SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_ATTRIBUTE_KEY
                    + "] heuristic used for the timeout and the ["
                    + SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_SOURCE_WARMING_INFO_STATUS_ATTRIBUTE_KEY
                    + "] of the source's shard warming info",
                "count"
            );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING,
            value -> this.searchRecoveryWarmingSourceShutdownShareFactor = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_CACHE_RATIO_SETTING,
            value -> this.searchRecoveryWarmingCacheRatio = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_FILTER_SOURCE_SHARDS_BY_TARGET_SETTING,
            value -> this.searchRecoveryWarmingFilterSourceShardsByTarget = value
        );
    }

    /// Returns the warming timeout for a shard whose relocation source is shutting down, as the maximum of these heuristics:
    ///
    /// 1. _Equal-share_: `factor * (deadline - now) / shardsOnSource * concurrentRelocations`, ensuring every
    /// shard on the shutting-down source gets a fair slice of the remaining grace period.
    /// 2. _Data-volume-proportional_ (contributes only when `totalBytesToWarm` is greater than zero): the fraction of the
    /// node's warming cache budget consumed by this shard's data multiplied by the remaining time,
    /// i.e. `(totalBytesToWarm / (cacheSize * cacheRatio)) * remaining * concurrentRelocations`.
    /// 3. _Warm-amount share_ (when the source answered for its current shutdown signal before this plan):
    /// `(shardWarmAmount / sumOfWarmAmountsFromSourceToTarget) * remaining * concurrentRelocations`. Both amounts are the source's
    /// estimates. The source leaves copies already relocating to a different node out of the sum and keeps STARTED copies (no target
    /// yet) in it, and recomputes it on each request, so shards that finished relocating drop out of it.
    ///
    /// with `deadline = start + min(metadata grace, cap)`. The timeout is capped at the remaining grace period.
    ///
    /// `concurrentRelocations` is this source's share of the slots known to be available to the target's relocations, see
    /// [#estimateConcurrentRelocations]. It may be below one slot, so the timeout may round down to zero (skipping warming). This is a
    /// rough estimate, and an upper bound: it ignores the other recoveries competing for the target's slots.
    ///
    /// The heuristics above decide the first plan of the shutdown phase. Only equal-share plans are
    /// [SearchRecoveryTimeout#extendable]. Data-volume and warm-amount plans are not. A re-evaluation of an equal-share plan
    /// (`previousPlan`) always yields an equal-share plan that only extends the wait by the time saved since then, regardless of which
    /// heuristic would win, see [SearchRecoveryTimeoutCalculationService#searchRecoveryTimeout]. When all shards fit in the target's
    /// recovery slots, nobody waits behind this one, so each re-evaluation extends the wait by up to one minute
    /// (bounded by the remaining grace) instead of only by the saved time.
    public SearchRecoveryTimeout timeout(
        ClusterState state,
        String sourceNodeId,
        String targetNodeId,
        ShardId shardId,
        long totalBytesToWarm,
        @Nullable SearchRecoveryTimeout previousPlan,
        SourceShardWarmingInfo sourceShardWarmingInfo
    ) {
        final var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        assert shutdown != null;
        final long now = threadPool.absoluteTimeInMillis();
        final long deadline = shutdownWarmingDeadline.deadlineMillis(shutdown);
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

        // Data-volume-proportional heuristic: scale remaining time by the fraction of the warming cache this shard occupies.
        final long warmingCacheBytes = Math.round(cacheService.getCacheSize() * searchRecoveryWarmingCacheRatio);
        // Re-evaluations pass the bytes still to warm (an approximation, see SearchRecoveryTimeoutCalculationService#extendedTimeout),
        // the first calculation passes all of them. The baseline (the warming cache budget) is still fixed, since it's hard to do the
        // accounting of the bytes warmed for shards for all the relocations of a given node shutting down.
        final double dataVolumeMs = warmingCacheBytes > 0 ? ((double) totalBytesToWarm / warmingCacheBytes) * remaining : 0;

        final double concurrentRelocations = estimateConcurrentRelocations(state, sourceNodeId, targetNodeId, shardsOnSource);

        if (previousPlan != null && previousPlan.isEqualSharePlan() && previousPlan.perShardShareMs() > 0) {
            // Re-evaluation of an equal-share plan: every shard still on the source was already budgeted previousPlan.perShardShareMs()
            // when the previous plan was computed, so only the part of the fresh share above that is time saved by shards that finished
            // early. When none did, the fresh share is not larger and there is nothing to extend by. The data-volume and warm-amount
            // heuristics are deliberately not consulted: they only decide the first plan of the shutdown phase, afterwards only saved
            // time is handed out. The exception is when all shards fit in the target's slots: nobody is queued behind this one, so the
            // remaining grace would be wasted if not used, so extend by max 1 minute on every reevaluation as long as the conditions
            // don't change.
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

        // Warm-amount shares use the source's estimates of its current commits; they can differ from this target's WarmTarget plan.
        final long shutdownStartedAtMillis = shutdown.getStartedAtMillis();
        final double warmAmountMs = sourceShardWarmingInfo.get(shutdownStartedAtMillis).map(warmingInfo -> {
            logger.debug(
                "{} source shard warming info [{}/{}] bytes, target plans to warm [{}] bytes",
                shardId,
                warmingInfo.shardBytesToWarm(),
                warmingInfo.sourceBytesToWarm(),
                totalBytesToWarm
            );
            return warmingInfo.warmBytesRatio() * remaining;
        }).orElse(0.0);

        // The decision below is per-shard whereas the three heuristics above assume all shards opt with the same heuristic.
        // This is an inherent problem of the fact that, during relocation, we don't know up-front all the shards that are going
        // to be relocated between two given nodes, so we can't know which of the three heuristics is more suitable overall.
        // Though the per-shard local decision here is OKish, because it's all relative to the remaining deadline and shards,
        // so the impact of currently choosing a different heuristic from previous (or future) relocating shards is partially mitigated.
        final ShutdownHeuristic heuristic = ShutdownHeuristic.select(equalShareMs, dataVolumeMs, warmAmountMs);
        drainTimeoutHeuristicTotalMetric.incrementBy(
            1,
            Map.of(
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_ATTRIBUTE_KEY,
                heuristic.name(),
                SearchRecoveryTimeoutCalculationService.SEARCH_RECOVERY_SOURCE_WARMING_INFO_STATUS_ATTRIBUTE_KEY,
                sourceShardWarmingInfo.status(shutdownStartedAtMillis).attributeValue()
            )
        );
        return new SearchRecoveryTimeout(
            TimeValue.timeValueMillis(Math.round(Math.min(remaining, heuristic.perShardShareMs() * concurrentRelocations))),
            heuristic.context(),
            // Only equal-share plans are extended, and a re-evaluation compares this share with the fresh one.
            heuristic.context() == TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE ? heuristic.perShardShareMs() : 0
        );
    }

    private record ShutdownHeuristic(String name, TimeoutContext context, double perShardShareMs) {
        static ShutdownHeuristic select(double equalShareMs, double dataVolumeMs, double warmAmountMs) {
            if (warmAmountMs > equalShareMs && warmAmountMs > dataVolumeMs) {
                return new ShutdownHeuristic("warm_amount", TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_WARM_AMOUNT, warmAmountMs);
            } else if (dataVolumeMs > equalShareMs) {
                return new ShutdownHeuristic("data_volume", TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME, dataVolumeMs);
            } else {
                return new ShutdownHeuristic("equal_share", TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE, equalShareMs);
            }
        }
    }

    /// Returns this source's share of the target's relocation slots: how many of the shards of sourceNodeId can actively recover
    /// on targetNodeId concurrently: the source's share of the slots known to be available to the relocations from shutting-down nodes,
    /// see [TargetRelocationSlotsEstimator#knownAvailableSlots], split between the sources in proportion to their relocations in flight,
    /// and not more than `shardsOnSource`. It scales the equal share of one shard to the time the shards of the source can use the slots
    /// together. It can be fractional and below one, when the slots are split between sources.
    /// Without data node recovery throttling, it is the relocations from the source to the target that are in flight.
    private double estimateConcurrentRelocations(
        final ClusterState state,
        final String sourceNodeId,
        final String targetNodeId,
        final int shardsOnSource
    ) {
        // The current shard is itself one such relocation, floor at 1 in case it is not yet visible on the source's RoutingNode.
        final int relocationsFromSourceToTarget = Math.max(1, SearchShardRelocations.countBetween(state, sourceNodeId, targetNodeId));
        final int allRelocationsToTarget = Math.max(1, SearchShardRelocations.countFromShuttingDownNodesTo(state, targetNodeId));
        return Math.min(
            shardsOnSource,
            (double) targetRelocationSlotsEstimator.knownAvailableSlots(state, allRelocationsToTarget) * relocationsFromSourceToTarget
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
