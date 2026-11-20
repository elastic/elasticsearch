/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;

import java.util.stream.Stream;

import static java.util.Comparator.comparingLong;

/// The warming timeouts of search shards that relocate from a source node that is not shutting down while other nodes are, which must
/// not hold a relocation slot of the target that a shard of a shutting-down node needs.
public class RelocationOtherNodeShutdownTimeouts {

    private final TargetRelocationSlotsEstimator targetRelocationSlotsEstimator;
    private final ShutdownWarmingDeadline shutdownWarmingDeadline;
    private final ThreadPool threadPool;
    private volatile TimeValue searchRecoveryWarmingRelocationWithShutdownTimeout;
    private volatile boolean shareSlotsWithShuttingDownNodes;

    public RelocationOtherNodeShutdownTimeouts(
        TargetRelocationSlotsEstimator targetRelocationSlotsEstimator,
        ShutdownWarmingDeadline shutdownWarmingDeadline,
        ThreadPool threadPool,
        ClusterSettings clusterSettings
    ) {
        this.targetRelocationSlotsEstimator = targetRelocationSlotsEstimator;
        this.shutdownWarmingDeadline = shutdownWarmingDeadline;
        this.threadPool = threadPool;
        clusterSettings.initializeAndWatch(
            SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
            value -> this.searchRecoveryWarmingRelocationWithShutdownTimeout = value
        );
        clusterSettings.initializeAndWatch(
            SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING,
            value -> this.shareSlotsWithShuttingDownNodes = value
        );
    }

    /// Returns the warming timeout for a shard relocating to `targetNodeId` from a source that is not shutting down, while other nodes are.
    /// For each grace deadline, the time left until it is divided between this shard and the shards of the shutting-down nodes with an
    /// earlier or equal deadline. The smallest such equal share is the wait, so that this shard takes no more than its share of the slots.
    ///
    /// A re-evaluation (`previousPlan`) extends the wait only by the time saved by shards that finished early, as handing out a fresh equal
    /// share every time would add up to the whole time left.
    ///
    /// The plain relocation-with-shutdown timeout applies when everything fits in the slots, and when
    /// [SearchRecoveryTimeoutCalculationService#OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING] is disabled or a
    /// shutting-down node still holds shards that have not begun to relocate, as then the relocations to the target are not all known.
    public SearchRecoveryTimeout timeout(ClusterState state, String targetNodeId, @Nullable SearchRecoveryTimeout previousPlan) {
        if (shareSlotsWithShuttingDownNodes == false || SearchShardRelocations.hasPendingRelocationsOnShuttingDownNodes(state)) {
            return fallbackTimeout();
        }
        // This shard is itself an initializing relocation on the target from a non-shutting-down node, so the count includes it: it is
        // added to the competing shards (+1) below, and left out of the "other" relocations here.
        final int otherRelocationsToTarget = Math.max(0, SearchShardRelocations.countFromNodesNotShuttingDownTo(state, targetNodeId) - 1);
        final int slots = targetRelocationSlotsEstimator.maxConcurrentRelocations();
        final long now = threadPool.absoluteTimeInMillis();

        // the latest deadline and the shards competing before each deadline rely on the order
        final var shardsOfShuttingDownNodes = shardsOfShuttingDownNodes(state, targetNodeId) //
            .sorted(comparingLong(ShuttingDownNodeShards::graceDeadlineMillis))
            .toList();
        final long allCompetingShards = 1 + shardsOfShuttingDownNodes.stream().mapToLong(ShuttingDownNodeShards::shards).sum();
        final double allCompetingShardsSlotShare = slotShareOfCompetingShards(slots, allCompetingShards, otherRelocationsToTarget);
        if (shardsOfShuttingDownNodes.isEmpty()
            || shardsOfShuttingDownNodes.getLast().graceDeadlineMillis() <= now
            || allCompetingShardsSlotShare >= allCompetingShards) {
            // no shutting-down node waits for this target before its deadline, or nobody queues behind anybody: treat this as if it were
            // not a shutting-down node
            return new SearchRecoveryTimeout(
                searchRecoveryWarmingRelocationWithShutdownTimeout,
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE
            );
        }

        // The deadline that leaves the smallest equal share decides, so that no deadline is missed. The latest deadline has time left, as
        // the check above did not return, and all the shards compete for the target before it, which is where the search starts from.
        var tightestDeadline = new DeadlineShare(
            shardsOfShuttingDownNodes.getLast().graceDeadlineMillis() - now,
            allCompetingShards,
            allCompetingShardsSlotShare
        );
        long shardsWithEarlierOrSameDeadline = 0;
        for (ShuttingDownNodeShards nodeShards : shardsOfShuttingDownNodes) {
            shardsWithEarlierOrSameDeadline += nodeShards.shards();
            final long remainingMs = nodeShards.graceDeadlineMillis() - now;
            if (remainingMs <= 0) {
                // waiting less cannot help the shards of a deadline that has passed, but they still compete with the later deadlines
                continue;
            }
            final long shardsCompetingForTarget = shardsWithEarlierOrSameDeadline + 1;
            final var deadlineShare = new DeadlineShare(
                remainingMs,
                shardsCompetingForTarget,
                slotShareOfCompetingShards(slots, shardsCompetingForTarget, otherRelocationsToTarget)
            );
            if (deadlineShare.equalShareMs() < tightestDeadline.equalShareMs()) {
                tightestDeadline = deadlineShare;
            }
        }

        final double timePerShardMs = tightestDeadline.timePerShardMs();
        final double waitMs;
        if (previousPlan != null && previousPlan.isEqualSharePlan() && previousPlan.perShardShareMs() > 0) {
            waitMs = Math.max(0.0, timePerShardMs - previousPlan.perShardShareMs()) * tightestDeadline.slotShare();
        } else {
            waitMs = tightestDeadline.equalShareMs();
        }
        return new SearchRecoveryTimeout(
            TimeValue.timeValueMillis(Math.round(waitMs)),
            TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE,
            timePerShardMs
        );
    }

    private SearchRecoveryTimeout fallbackTimeout() {
        return new SearchRecoveryTimeout(
            searchRecoveryWarmingRelocationWithShutdownTimeout,
            TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT
        );
    }

    private Stream<ShuttingDownNodeShards> shardsOfShuttingDownNodes(final ClusterState state, final String targetNodeId) {
        return removalShutdownsOfNodesInCluster(state) //
            .map(
                shutdown -> new ShuttingDownNodeShards(
                    shutdownWarmingDeadline.deadlineMillis(shutdown),
                    SearchShardRelocations.countBetween(state, shutdown.getNodeId(), targetNodeId)
                )
            )
            .filter(nodeShards -> nodeShards.shards() > 0);
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

    /// The `shards` of a shutting-down node that are relocating to the target, which have to be through before the node's
    /// `graceDeadlineMillis`.
    private record ShuttingDownNodeShards(long graceDeadlineMillis, long shards) {}

    /// What one grace deadline leaves to the shards competing for the target: the time left until it, the shards competing for the target
    /// before it, and their share of the target's relocation limit.
    private record DeadlineShare(long remainingMs, long shardsCompetingForTarget, double slotShare) {

        double timePerShardMs() {
            return remainingMs / (double) shardsCompetingForTarget;
        }

        double equalShareMs() {
            return timePerShardMs() * slotShare;
        }
    }

    /// The shutdowns of the nodes marked for removal that are still in the cluster. Shutdown metadata outlives the node until it is
    /// deleted, and the stale entry of a node that has left must not count: its grace deadline is long past, and it would hold every
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
