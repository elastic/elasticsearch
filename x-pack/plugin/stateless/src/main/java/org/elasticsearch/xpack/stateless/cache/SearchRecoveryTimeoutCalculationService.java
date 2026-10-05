/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

/// Computes how long search shard recovery should await offline warming (internal replicated-files path only), see
/// [#searchRecoveryTimeout].
public class SearchRecoveryTimeoutCalculationService {

    private static final Logger logger = LogManager.getLogger(SearchRecoveryTimeoutCalculationService.class);

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private volatile ShardWarmVolumes shardWarmVolumes;
    private final LongCounter drainTimeoutHeuristicTotalMetric;
    private final DoubleHistogram drainTimeoutFormulaDeltaMetric;
    private volatile TimeValue searchRecoveryWarmingRelocationWithShutdownTimeout;
    private volatile TimeValue searchRecoveryWarmingRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingNonRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingReshardTargetTimeout;
    private volatile TimeValue searchRecoveryWarmingGracePeriodCap;
    private volatile double searchRecoveryWarmingSourceShutdownShareFactor;
    private volatile double searchRecoveryWarmingCacheRatio;

    SearchRecoveryTimeoutCalculationService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        ShardWarmVolumes shardWarmVolumes,
        TelemetryProvider telemetryProvider
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        this.shardWarmVolumes = shardWarmVolumes;
        this.drainTimeoutHeuristicTotalMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter(
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_TOTAL_METRIC,
                "Drain-path search recovery warming timeouts, broken down by the ["
                    + SharedBlobCacheWarmingService.SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_ATTRIBUTE_KEY
                    + "] heuristic that produced the timeout",
                "count"
            );
        this.drainTimeoutFormulaDeltaMetric = telemetryProvider.getMeterRegistry()
            .registerDoubleHistogram(
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_DRAIN_TIMEOUT_FORMULA_DELTA_METRIC,
                "Chosen drain warming timeout minus the timeout from the equal-share and data-volume formulas alone, "
                    + "broken down by whether warm volumes were ["
                    + SharedBlobCacheWarmingService.SEARCH_RECOVERY_DRAIN_TIMEOUT_VOLUMES_PRESENT_ATTRIBUTE_KEY
                    + "]",
                "s"
            );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
            value -> this.searchRecoveryWarmingRelocationWithShutdownTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_SETTING,
            value -> this.searchRecoveryWarmingRelocationTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING,
            value -> this.searchRecoveryWarmingNonRelocationTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RESHARD_TARGET_SETTING,
            value -> this.searchRecoveryWarmingReshardTargetTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING,
            value -> this.searchRecoveryWarmingGracePeriodCap = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_SOURCE_SHUTDOWN_SHARE_FACTOR_SETTING,
            value -> this.searchRecoveryWarmingSourceShutdownShareFactor = value
        );
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_CACHE_RATIO_SETTING,
            value -> this.searchRecoveryWarmingCacheRatio = value
        );
    }

    /// When to await search recovery warming (internal replicated-files path only). Relocation targets use relocation-specific timeouts or
    /// a computed share when the source is shutting down. Non-relocation: wait only if another active search shard copy exists and there
    /// is no cluster shutdown metadata, using [SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING].
    public SearchRecoveryTimeout searchRecoveryTimeout(ClusterState state, IndexShard indexShard, long totalBytesToWarm) {
        final ShardRouting shardRouting = indexShard.routingEntry();
        assert shardRouting.isPromotableToPrimary() == false;
        if (isRelocationTarget(shardRouting)) {
            final String sourceNodeId = shardRouting.relocatingNodeId();
            assert sourceNodeId != null;
            if (state.metadata().nodeShutdowns().isNodeMarkedForRemoval(sourceNodeId)) {
                return computeRelocationSourceShutdownWarmingTimeout(
                    state,
                    sourceNodeId,
                    shardRouting.currentNodeId(),
                    indexShard.shardId(),
                    totalBytesToWarm
                );
            }
            if (hasActiveShutdownForRemovalNodes(state)) {
                return new SearchRecoveryTimeout(
                    searchRecoveryWarmingRelocationWithShutdownTimeout,
                    "relocation source not shutting down, cluster shutdown metadata present"
                );
            }
            return new SearchRecoveryTimeout(
                searchRecoveryWarmingRelocationTimeout,
                "relocation source not shutting down, no cluster shutdown"
            );
        }
        if (hasAnotherActiveSearchShardCopy(state, indexShard) && hasActiveShutdownForRemovalNodes(state) == false) {
            return new SearchRecoveryTimeout(searchRecoveryWarmingNonRelocationTimeout, "not a relocation, another active shard copy");
        }
        if (searchRecoveryWarmingReshardTargetTimeout.millis() > 0 && isReshardSplitTarget(state, indexShard.shardId())) {
            return new SearchRecoveryTimeout(searchRecoveryWarmingReshardTargetTimeout, "reshard split target");
        }
        return SearchRecoveryTimeout.skip();
    }

    private static boolean isReshardSplitTarget(ClusterState state, ShardId shardId) {
        return state.metadata()
            .findIndex(shardId.getIndex())
            .map(meta -> IndexReshardingMetadata.isSplitTarget(shardId, meta.getReshardingMetadata()))
            .orElse(false);
    }

    private static boolean isRelocationTarget(ShardRouting self) {
        return self.initializing() && self.relocatingNodeId() != null;
    }

    /// Whether the routing table has at least one active search routing for this logical shard
    private static boolean hasAnotherActiveSearchShardCopy(ClusterState state, IndexShard indexShard) {
        final var projectId = state.metadata().projectFor(indexShard.shardId().getIndex()).id();
        final IndexShardRoutingTable shardTable = state.routingTable(projectId).shardRoutingTable(indexShard.shardId());
        return shardTable.getActiveSearchShardCount() > 0;
    }

    private static int countShardsOnNode(ClusterState clusterState, String nodeId) {
        var node = clusterState.getRoutingNodes().node(nodeId);
        return node == null ? 0 : node.size();
    }

    private static boolean hasActiveShutdownForRemovalNodes(ClusterState state) {
        for (Map.Entry<String, SingleNodeShutdownMetadata> entry : state.metadata().nodeShutdowns().getAll().entrySet()) {
            if (entry.getValue().getType().isRemovalType() && state.nodes().nodeExists(entry.getKey())) {
                return true;
            }
        }
        return false;
    }

    void setShardWarmVolumes(ShardWarmVolumes shardWarmVolumes) {
        this.shardWarmVolumes = shardWarmVolumes;
    }

    /// Returns the warming timeout for a shard whose relocation source is shutting down, as the maximum of three
    /// per-shard heuristics times ongoing relocations, capped at remaining grace:
    ///
    /// 1. _Equal-share_: `factor * remaining / shardsOnSource`.
    /// 2. _Data-volume-proportional_ (contributes only when `totalBytesToWarm` is greater than zero):
    /// `(totalBytesToWarm / (cacheSize * cacheRatio)) * remaining`.
    /// 3. _Warm-volume share_ (when a completed [ShardWarmVolumes.Entry] exists):
    /// `(warm volume / sum of warm volumes on source) * remaining`. An unknown shard contributes 0.
    ///
    /// with `deadline = start + min(metadata grace, cap)` and `remaining = deadline - now`.
    private SearchRecoveryTimeout computeRelocationSourceShutdownWarmingTimeout(
        ClusterState state,
        String sourceNodeId,
        String targetNodeId,
        ShardId shardId,
        long totalBytesToWarm
    ) {
        final var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        assert shutdown != null;
        TimeValue grace = shutdown.getGracePeriod();
        if (grace == null) {
            grace = searchRecoveryWarmingGracePeriodCap;
        }
        final long effectiveGraceMillis = Math.min(grace.getMillis(), searchRecoveryWarmingGracePeriodCap.millis());
        final long now = threadPool.absoluteTimeInMillis();
        final long deadline = shutdown.getStartedAtMillis() + effectiveGraceMillis;
        final long remaining = deadline - now;
        if (remaining <= 0) {
            return new SearchRecoveryTimeout(TimeValue.ZERO, "relocation source shutting down (grace period elapsed)");
        }
        int shardsOnSource = countShardsOnNode(state, sourceNodeId);
        if (shardsOnSource <= 0) {
            shardsOnSource = 1;
        }
        final double equalShareMs = (remaining / (double) shardsOnSource) * searchRecoveryWarmingSourceShutdownShareFactor;

        // Data-volume-proportional heuristic: scale remaining time by the fraction of the warming cache this shard occupies.
        final long warmingCacheBytes = Math.round(cacheService.getCacheSize() * searchRecoveryWarmingCacheRatio);
        // TODO
        // We're looking at the "remaining" time, but not at the "remaining" bytes to populate.
        // Instead, this uses the same fixed baseline (which itself is of dubious inspiration).
        // But it's hard to do the accounting of the bytes warmed for shards for all the relocations of a given node shutting down.
        final double dataVolumeMs = warmingCacheBytes > 0 ? ((double) totalBytesToWarm / warmingCacheBytes) * remaining : 0;
        // Warm-volume shares use the source's current-commit prefixes; they can differ from this target's WarmTarget plan.
        final WarmVolumeShare warmVolume = warmVolumeShare(state, sourceNodeId, shardId, remaining);
        final double warmVolumeMs = warmVolume.shareMs();
        int ongoingRelocations = countOngoingRelocationsBetween(state, sourceNodeId, targetNodeId);
        // The current shard is itself one such relocation; floor at 1 in case it is not yet visible on the source's RoutingNode.
        if (ongoingRelocations <= 0) {
            ongoingRelocations = 1;
        }

        // The decision below is per-shard whereas the heuristics above assume all shards opt with the same heuristic
        // this is an inherent problem of the fact that, during relocation, we don't know apriori all the shards that are going
        // to be relocated between two given nodes, so we can't know which of the heuristics is more suitable overall.
        // Though the per-shard local decision here is OKish, because it's all relative to the remaining deadline and shards,
        // so the impact of currently choosing a different heuristic from previous (or future) relocating shards is partially mitigated
        final double timeoutHeuristicMs;
        final String context;
        final String heuristic;
        if (warmVolumeMs > equalShareMs && warmVolumeMs > dataVolumeMs) {
            timeoutHeuristicMs = warmVolumeMs;
            heuristic = "warm_volume";
            context = "relocation source shutting down (warm volume share of remaining time to capped grace deadline)";
        } else if (dataVolumeMs > equalShareMs) {
            timeoutHeuristicMs = dataVolumeMs;
            heuristic = "data_volume";
            context = "relocation source shutting down (data volume proportional share of remaining time to capped grace deadline)";
        } else {
            timeoutHeuristicMs = equalShareMs;
            heuristic = "equal_share";
            context = "relocation source shutting down (equal share of remaining time to capped grace deadline)";
        }
        final double previousHeuristicMs = Math.max(equalShareMs, dataVolumeMs);
        final long previousTimeoutMs = Math.round(Math.min(remaining, previousHeuristicMs * ongoingRelocations));
        final long chosenTimeoutMs = Math.round(Math.min(remaining, timeoutHeuristicMs * ongoingRelocations));
        drainTimeoutHeuristicTotalMetric.incrementBy(
            1,
            Map.of(SharedBlobCacheWarmingService.SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_ATTRIBUTE_KEY, heuristic)
        );
        drainTimeoutFormulaDeltaMetric.record(
            (chosenTimeoutMs - previousTimeoutMs) / 1000.0,
            Map.of(
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_DRAIN_TIMEOUT_VOLUMES_PRESENT_ATTRIBUTE_KEY,
                Boolean.toString(warmVolume.present())
            )
        );
        logger.info(
            "drain warming timeout shard [{}] source [{}] equalShareMs [{}] dataVolumeMs [{}] warmVolumeMs [{}] "
                + "previousTimeoutMs [{}] chosenTimeoutMs [{}] deltaMs [{}] heuristic [{}] shardWarmBytes [{}] "
                + "sourceWarmBytes [{}] volumesPresent [{}] ongoingRelocations [{}] remainingMs [{}]",
            shardId,
            sourceNodeId,
            Math.round(equalShareMs),
            Math.round(dataVolumeMs),
            Math.round(warmVolumeMs),
            previousTimeoutMs,
            chosenTimeoutMs,
            chosenTimeoutMs - previousTimeoutMs,
            heuristic,
            warmVolume.shardBytes(),
            warmVolume.sourceBytes(),
            warmVolume.present(),
            ongoingRelocations,
            remaining
        );
        return new SearchRecoveryTimeout(TimeValue.timeValueMillis(chosenTimeoutMs), context);
    }

    /// Per-shard warm-volume share of `remaining`. `shareMs` is 0 when the map cannot be used for this shard.
    /// `present` is true when a completed volume snapshot exists for the source, even if this shard is missing from it.
    private WarmVolumeShare warmVolumeShare(ClusterState state, String sourceNodeId, ShardId shardId, long remaining) {
        var entry = shardWarmVolumes.get(state, sourceNodeId);
        if (entry == null) {
            return new WarmVolumeShare(0, false, 0L, 0L);
        }
        final var sourceNode = state.getRoutingNodes().node(sourceNodeId);
        if (sourceNode == null) {
            return new WarmVolumeShare(0, true, 0L, 0L);
        }
        long sourceWarmVolumeSum = 0L;
        Long thisShardVolume = null;
        for (ShardRouting routing : sourceNode) {
            assert routing.isSearchable();
            Long volume = entry.volumes().get(routing.shardId());
            if (volume == null) {
                continue;
            }
            sourceWarmVolumeSum += volume;
            if (routing.shardId().equals(shardId)) {
                thisShardVolume = volume;
            }
        }
        if (thisShardVolume == null || sourceWarmVolumeSum <= 0L) {
            return new WarmVolumeShare(0, true, thisShardVolume == null ? 0L : thisShardVolume, sourceWarmVolumeSum);
        }
        return new WarmVolumeShare(
            (thisShardVolume / (double) sourceWarmVolumeSum) * remaining,
            true,
            thisShardVolume,
            sourceWarmVolumeSum
        );
    }

    private record WarmVolumeShare(double shareMs, boolean present, long shardBytes, long sourceBytes) {}

    /// Counts ongoing relocations whose source is `sourceNodeId` and whose target is `targetNodeId` (i.e. shards relocating
    /// from `sourceNodeId` to `targetNodeId`, as seen from the source's `RoutingNode`).
    private static int countOngoingRelocationsBetween(ClusterState state, String sourceNodeId, String targetNodeId) {
        final var sourceNode = state.getRoutingNodes().node(sourceNodeId);
        if (sourceNode == null) {
            return 0;
        }
        int count = 0;
        for (ShardRouting r : sourceNode.relocating()) {
            if (targetNodeId.equals(r.relocatingNodeId())) {
                count++;
            }
        }
        return count;
    }
}
