/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Comparator.comparingLong;

/// The warming timeouts of search shards that relocate from a source node that is not shutting down while other nodes are, which must
/// not hold a relocation slot of the target that a shard of a shutting-down node needs.
///
/// It owns the settings and the cluster state accounting that only these timeouts use, so that
/// [SearchRecoveryTimeoutCalculationService] reads as a choice of the timeout context.
class RelocationOtherNodeShutdownTimeouts {

    private final NonCriticalRecoverySlots nonCriticalRecoverySlots;
    private final ShutdownWarmingDeadlines shutdownWarmingDeadlines;
    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private volatile double searchRecoveryWarmingCacheRatio;
    private volatile TimeValue searchRecoveryWarmingRelocationWithShutdownTimeout;
    private volatile TimeValue searchRecoveryWarmingTotalTimeoutCap;
    private volatile boolean shareSlotsWithShuttingDownNodes;

    RelocationOtherNodeShutdownTimeouts(
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
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
            value -> this.searchRecoveryWarmingRelocationWithShutdownTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING,
            value -> this.searchRecoveryWarmingTotalTimeoutCap = value
        );
        clusterSettings.initializeAndWatch(
            SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING,
            value -> this.shareSlotsWithShuttingDownNodes = value
        );
    }

    /// Returns the warming timeout for a shard relocating to `targetNodeId` from a source that is not shutting down, while other nodes are,
    /// so that it does not hold a slot of the target that a shard of a shutting-down node needs.
    ///
    /// For each grace deadline, the time left until it is divided between this shard and the shards of the shutting-down nodes with an
    /// earlier or equal deadline, and the smallest such equal share decides. As when the relocation source is shutting down, the plan is
    /// the larger of that and the data-volume share. An equal-share plan is capped to the relocation-with-shutdown timeout and not
    /// extended again, a data-volume plan is waited for in a single slice.
    ///
    /// Falls back to the plain relocation-with-shutdown timeout, unless
    /// [SearchRecoveryTimeoutCalculationService#OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING] is enabled and the data nodes
    /// throttle the relocations.
    SearchRecoveryTimeout timeout(ClusterState state, String targetNodeId, long totalBytesToWarm) {
        // The slots can be shared only when the data nodes throttle the relocations. While the master does, the relocations of the
        // shutting-down nodes to this target are not all visible in the cluster state yet.
        if (shareSlotsWithShuttingDownNodes == false || nonCriticalRecoverySlots.dataNodeRecoveryThrottlingEnabled() == false) {
            return fixedTimeout();
        }
        final var relocationsToTarget = SearchRelocationsToTarget.count(state, targetNodeId);
        // This shard is itself an initializing relocation on the target from a non-shutting-down node, so the count above includes it:
        // it is added to the competing shards (+1) below, and left out of the "other" relocations here.
        final int otherRelocationsToTarget = Math.max(0, relocationsToTarget.fromNonShuttingDownNodes() - 1);
        final int slots = nonCriticalRecoverySlots.effectiveSlots(relocationsToTarget.allFromShuttingDownNodes());
        final long now = threadPool.absoluteTimeInMillis();

        final var shardsOfShuttingDownNodes = shardsOfShuttingDownNodes(state, relocationsToTarget);

        long shardsWithEarlierOrSameDeadline = 0;
        EqualShare tightestEqualShare = null;
        for (ShuttingDownNodeShards nodeShards : shardsOfShuttingDownNodes) {
            shardsWithEarlierOrSameDeadline += nodeShards.shards();
            final long remainingMs = nodeShards.graceDeadlineMillis() - now;
            if (remainingMs <= 0) {
                // waiting less cannot help the shards of a deadline that has passed, but they still compete with the later deadlines
                continue;
            }
            final long shardsCompetingForTarget = shardsWithEarlierOrSameDeadline + 1;
            final var equalShare = new EqualShare(
                remainingMs,
                shardsCompetingForTarget,
                slotShareOfCompetingShards(slots, shardsCompetingForTarget, otherRelocationsToTarget)
            );
            if (tightestEqualShare == null || equalShare.inMillis() < tightestEqualShare.inMillis()) {
                tightestEqualShare = equalShare;
            }
        }

        final long allCompetingShards = shardsWithEarlierOrSameDeadline + 1;
        if (tightestEqualShare == null
            || slotShareOfCompetingShards(slots, allCompetingShards, otherRelocationsToTarget) >= allCompetingShards) {
            // no shutting-down node waits for this target before its deadline, or nobody queues behind anybody: treat this as if it were
            // not a shutting-down node
            return new SearchRecoveryTimeout(
                searchRecoveryWarmingRelocationWithShutdownTimeout,
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE
            );
        }

        // the share never exceeds the time left until the grace deadline of the smallest equal share, nor the relocation-with-shutdown
        // timeout
        final long maxShareMs = Math.min(tightestEqualShare.remainingMs(), searchRecoveryWarmingRelocationWithShutdownTimeout.millis());
        // A data-volume share is waited for in a single slice, as it is not extendable, so a big shard gets time proportional to its data
        // and not only the relocation-with-shutdown timeout. It is bound by the time left and by the total timeout cap.
        final long maxDataVolumeShareMs = Math.min(tightestEqualShare.remainingMs(), searchRecoveryWarmingTotalTimeoutCap.millis());
        final double equalShareMs = tightestEqualShare.inMillis();
        final double dataVolumeMs = dataVolumeShareMs(totalBytesToWarm, tightestEqualShare.remainingMs()) * tightestEqualShare.slotShare();
        if (dataVolumeMs > equalShareMs) {
            return new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(Math.round(Math.min(maxDataVolumeShareMs, dataVolumeMs))),
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_DATA_VOLUME
            );
        } else {
            return new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(Math.round(Math.min(maxShareMs, equalShareMs))),
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT
            );
        }
    }

    private SearchRecoveryTimeout fixedTimeout() {
        return new SearchRecoveryTimeout(
            searchRecoveryWarmingRelocationWithShutdownTimeout,
            TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT
        );
    }

    private List<ShuttingDownNodeShards> shardsOfShuttingDownNodes(
        final ClusterState state,
        final SearchRelocationsToTarget relocationsToTarget
    ) {
        return removalShutdownsOfNodesInCluster(state) //
            .map(
                shutdown -> new ShuttingDownNodeShards(
                    shutdownWarmingDeadlines.of(shutdown),
                    relocationsToTarget.fromShuttingDownNodes().getOrDefault(shutdown.getNodeId(), 0) + countPendingSearchShardsOnNode(
                        state,
                        shutdown.getNodeId()
                    )
                )
            )
            .filter(nodeShards -> nodeShards.shards() > 0)
            .sorted(comparingLong(ShuttingDownNodeShards::graceDeadlineMillis))
            .toList();
    }

    /// The share of the target's `slots` that the `shardsCompetingForTarget` shards get, when `otherRelocationsToTarget`
    /// relocations from nodes that are not shutting down hold part of them as well. It is split by the number of shards, can be fractional,
    /// and never exceeds the number of competing shards: `min(N, slots * N / (N + others))`.
    private static double slotShareOfCompetingShards(int slots, long shardsCompetingForTarget, int otherRelocationsToTarget) {
        return Math.min(
            shardsCompetingForTarget,
            (double) slots * shardsCompetingForTarget / (shardsCompetingForTarget + otherRelocationsToTarget)
        );
    }

    /// The data-volume-proportional heuristic: `remainingMs` scaled by the fraction of the warming cache budget that `totalBytesToWarm`
    /// takes, or `0` when there is no warming cache. The baseline (the warming cache budget) is fixed, since it's hard to account for
    /// the bytes warmed by all the shards relocating from a given shutting-down node.
    private double dataVolumeShareMs(long totalBytesToWarm, long remainingMs) {
        final long warmingCacheBytes = Math.round(cacheService.getCacheSize() * searchRecoveryWarmingCacheRatio);
        return warmingCacheBytes > 0 ? ((double) totalBytesToWarm / warmingCacheBytes) * remainingMs : 0;
    }

    /// The `shards` of a shutting-down node that may need the target, i.e. already relocating to it or not relocating yet, which have to
    /// be through before the node's `graceDeadlineMillis`.
    private record ShuttingDownNodeShards(long graceDeadlineMillis, long shards) {}

    /// The equal share for one grace deadline: the time left until it, the shards competing for the target before it, and their share of
    /// the target's relocation limit.
    private record EqualShare(long remainingMs, long shardsCompetingForTarget, double slotShare) {

        double inMillis() {
            return (remainingMs / (double) shardsCompetingForTarget) * slotShare;
        }
    }

    /// Counts the search shards in STARTED state on `nodeId`: those that have not begun relocating yet.
    private static long countPendingSearchShardsOnNode(ClusterState state, String nodeId) {
        final var node = state.getRoutingNodes().node(nodeId);
        if (node == null) {
            return 0;
        }
        long count = 0;
        for (ShardRouting shard : node.started()) {
            if (shard.isPromotableToPrimary() == false) {
                count++;
            }
        }
        return count;
    }

    /// The shutdowns of the nodes marked for removal that are still in the cluster. Shutdown metadata outlives the node until it is
    /// deleted, and the stale entry of a node that has left must not count: its grace deadline is long past and it would hold every
    /// other relocation to the shutdown timeouts.
    private static Stream<SingleNodeShutdownMetadata> removalShutdownsOfNodesInCluster(ClusterState state) {
        return state.metadata()
            .nodeShutdowns()
            .getAll()
            .values()
            .stream()
            .filter(shutdown -> shutdown.getType().isRemovalType() && state.nodes().nodeExists(shutdown.getNodeId()));
    }
}
