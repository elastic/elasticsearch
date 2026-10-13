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
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

/// Computes how long search shard recovery should await offline warming (internal replicated-files path only), see
/// [#searchRecoveryTimeout].
public class SearchRecoveryTimeoutCalculationService {

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private volatile TimeValue searchRecoveryWarmingRelocationWithShutdownTimeout;
    private volatile TimeValue searchRecoveryWarmingRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingNonRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingReshardTargetTimeout;
    private volatile TimeValue searchRecoveryWarmingGracePeriodCap;
    private volatile double searchRecoveryWarmingSourceShutdownShareFactor;
    private volatile double searchRecoveryWarmingCacheRatio;

    public SearchRecoveryTimeoutCalculationService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        ClusterSettings clusterSettings
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
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
                return computeRelocationSourceShutdownWarmingTimeout(state, sourceNodeId, shardRouting.currentNodeId(), totalBytesToWarm);
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

    /// Returns the warming timeout for a shard whose relocation source is shutting down, as the maximum of two heuristics:
    ///
    /// 1. _Equal-share_: `factor * (deadline - now) / shardsOnSource * relocationsFromSourceToTarget`, ensuring every
    /// shard on the shutting-down source gets a fair slice of the remaining grace period.
    /// 2. _Data-volume-proportional_ (contributes only when `totalBytesToWarm` is greater than zero): the fraction of the
    /// node's warming cache budget consumed by this shard's data multiplied by the remaining time,
    /// i.e. `(totalBytesToWarm / (cacheSize * cacheRatio)) * remaining`.
    ///
    /// with `deadline = start + min(metadata grace, cap)`.
    private SearchRecoveryTimeout computeRelocationSourceShutdownWarmingTimeout(
        ClusterState state,
        String sourceNodeId,
        String targetNodeId,
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
        int ongoingRelocations = countOngoingRelocationsBetween(state, sourceNodeId, targetNodeId);
        // The current shard is itself one such relocation; floor at 1 in case it is not yet visible on the source's RoutingNode.
        if (ongoingRelocations <= 0) {
            ongoingRelocations = 1;
        }

        final double timeoutMs;
        final String context;
        // The decision below is per-shard whereas the two heuristics above assume all shards opt with the same heuristic
        // this is an inherent problem of the fact that, during relocation, we don't know apriori all the shards that are going
        // to be relocated between two given nodes, so we can't know which of the two heuristics is more suitable overall.
        // Though the per-shard local decision here is OKish, because it's all relative to the remaining deadline and shards,
        // so the impact of currently choosing a different heuristic from previous (or future) relocating shards is partially mitigated
        if (dataVolumeMs > equalShareMs) {
            timeoutMs = Math.min(remaining, dataVolumeMs * ongoingRelocations);
            context = "relocation source shutting down (data volume proportional share of remaining time to capped grace deadline)";
        } else {
            timeoutMs = Math.min(remaining, equalShareMs * ongoingRelocations);
            context = "relocation source shutting down (equal share of remaining time to capped grace deadline)";
        }
        return new SearchRecoveryTimeout(TimeValue.timeValueMillis(Math.round(timeoutMs)), context);
    }

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
