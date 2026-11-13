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
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Comparator.comparingLong;

/// The warming timeouts of search shards that relocate from a source node that is not shutting down while other nodes are, which must
/// not hold a relocation slot of the target that a shard of a shutting-down node needs.
///
/// It owns the settings and the cluster state accounting that only these timeouts use, so that
/// [SearchRecoveryTimeoutCalculationService] reads as a choice of the timeout context.
class RelocationOtherNodeShutdownTimeouts {

    private final ShutdownTimingSupport timing;
    private volatile TimeValue searchRecoveryWarmingRelocationWithShutdownTimeout;
    private volatile TimeValue searchRecoveryWarmingTotalTimeoutCap;

    RelocationOtherNodeShutdownTimeouts(ShutdownTimingSupport timing, ClusterSettings clusterSettings) {
        this.timing = timing;
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
            value -> this.searchRecoveryWarmingRelocationWithShutdownTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING,
            value -> this.searchRecoveryWarmingTotalTimeoutCap = value
        );
    }

    /// Returns the warming timeout for a shard relocating to `targetNodeId` from a source that is not shutting down, while other nodes are,
    /// so that it does not hold a relocation slot on the target that a shard of a shutting-down node needs.
    ///
    /// Shutting-down nodes have different grace deadlines. The equal share is computed for each deadline on its own: the time left until
    /// it is divided between the shards of every shutting-down node with an earlier or equal deadline (plus this shard). The smallest
    /// equal share decides. This way a single shard with an early deadline does not squeeze the time of many shards with a later one. A
    /// deadline that has already passed is skipped, as waiting less cannot help its shards, but its shards still compete for the target
    /// with the shards of the later deadlines.
    ///
    /// Like when the relocation source is shutting down, the plan is the larger of the equal share and the data-volume share. An
    /// equal-share plan is capped to the relocation-with-shutdown timeout and is not extended again. A data-volume plan is waited for in a
    /// single slice, so that a big shard gets time proportional to its data.
    SearchRecoveryTimeout relocationOtherNodeShutdownTimeout(ClusterState state, String targetNodeId, long totalBytesToWarm) {
        final var relocationsToTarget = countSearchRelocationsToTarget(state, targetNodeId);
        // This shard is itself an initializing relocation on the target from a non-shutting-down node, so the count above includes it:
        // it is added to the competing shards (+1) below, and left out of the "other" relocations by the record.
        final int otherRelocationsToTarget = relocationsToTarget.otherFromNonShuttingDownNodes();
        final int slots = timing.knownRelocationSlots(relocationsToTarget.allFromShuttingDownNodes());
        final long now = timing.nowMillis();

        final var shardsOfShuttingDownNodes = shardsOfShuttingDownNodes(state, relocationsToTarget);

        long shardsWithEarlierOrSameDeadline = 0;
        EqualShare tightestEqualShare = null;
        for (ShuttingDownNodeShards nodeShards : shardsOfShuttingDownNodes) {
            shardsWithEarlierOrSameDeadline += nodeShards.shards();
            final long remainingMs = nodeShards.graceDeadlineMillis() - now;
            if (remainingMs <= 0) {
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
        final double dataVolumeMs = timing.dataVolumeShareMs(totalBytesToWarm, tightestEqualShare.remainingMs()) * tightestEqualShare
            .slotShare();
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

    private List<ShuttingDownNodeShards> shardsOfShuttingDownNodes(
        final ClusterState state,
        final SearchRelocationsToTarget relocationsToTarget
    ) {
        return removalShutdownsOfNodesInCluster(state) //
            .map(
                shutdown -> new ShuttingDownNodeShards(
                    timing.graceDeadline(shutdown),
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

    /// The search shards relocating to a node: how many come from each node marked for removal, by node id, and how many from the other
    /// nodes.
    private record SearchRelocationsToTarget(Map<String, Integer> fromShuttingDownNodes, int fromNonShuttingDownNodes) {

        /// The relocations from non-shutting-down nodes other than the one of the shard we compute the timeout for, which is among them as
        /// it is relocating to the target from a node that is not shutting down.
        int otherFromNonShuttingDownNodes() {
            return Math.max(0, fromNonShuttingDownNodes - 1);
        }

        /// The relocations from all the nodes marked for removal.
        int allFromShuttingDownNodes() {
            return fromShuttingDownNodes.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    /// Counts the search shards relocating to `targetNodeId`, which are INITIALIZING on it with their source set.
    private static SearchRelocationsToTarget countSearchRelocationsToTarget(ClusterState state, String targetNodeId) {
        final var targetNode = state.getRoutingNodes().node(targetNodeId);
        if (targetNode == null) {
            return new SearchRelocationsToTarget(Map.of(), 0);
        }
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        final Map<String, Integer> fromShuttingDownNodes = new HashMap<>();
        int fromNonShuttingDownNodes = 0;
        for (ShardRouting shard : targetNode.initializing()) {
            final String sourceNodeId = shard.relocatingNodeId();
            if (sourceNodeId == null || shard.isPromotableToPrimary()) {
                continue;
            }
            if (nodeShutdowns.isNodeMarkedForRemoval(sourceNodeId)) {
                fromShuttingDownNodes.merge(sourceNodeId, 1, Integer::sum);
            } else {
                fromNonShuttingDownNodes++;
            }
        }
        return new SearchRelocationsToTarget(fromShuttingDownNodes, fromNonShuttingDownNodes);
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
