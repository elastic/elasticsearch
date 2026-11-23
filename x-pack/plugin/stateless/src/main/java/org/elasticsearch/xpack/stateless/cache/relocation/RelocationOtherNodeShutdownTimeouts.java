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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Comparator.comparingDouble;
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
        final var deadlineShares = deadlineShares(state, targetNodeId);
        if (deadlineShares.stream().allMatch(DeadlineShare::hasSlotForEachCompetingShard)) {
            // no shutting-down node waits for this target before its deadline, or nobody queues behind anybody: treat this as if it were
            // not a shutting-down node
            return new SearchRecoveryTimeout(
                searchRecoveryWarmingRelocationWithShutdownTimeout,
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE
            );
        }

        // Use the deadline with the smallest equal share, so that no deadline is missed.
        final var tightestDeadline = Collections.min(deadlineShares, comparingDouble(DeadlineShare::equalShareMs));
        final double timePerRecoveryMs = tightestDeadline.timePerRecoveryMs();
        final double waitMs;
        if (previousPlan != null && previousPlan.isEqualSharePlan() && previousPlan.perShardShareMs() > 0) {
            waitMs = tightestDeadline.waitForTimeSavedSince(previousPlan.perShardShareMs());
        } else {
            waitMs = tightestDeadline.equalShareMs();
        }
        return new SearchRecoveryTimeout(
            TimeValue.timeValueMillis(Math.round(waitMs)),
            TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE,
            timePerRecoveryMs
        );
    }

    /// What each grace deadline leaves to the shards competing for the target, from the earliest deadline to the latest.
    /// A deadline that has passed leaves nothing: the warming cap does not free a slot, so its shards may still be queued for one.
    private List<DeadlineShare> deadlineShares(final ClusterState state, final String targetNodeId) {
        // All the non-critical recoveries on the target share its slots, also the ones of the shutting-down nodes with a later deadline, as
        // the recoveries are not scheduled by the deadline.
        final int recoveriesOnTarget = SearchShardRelocations.countNonCriticalInitializing(state, targetNodeId);
        final int slots = targetRelocationSlotsEstimator.slotsForNonCriticalRecoveries();
        final long now = threadPool.absoluteTimeInMillis();
        // the shards competing before each deadline rely on the order
        final var shardsOfShuttingDownNodes = shardsOfShuttingDownNodes(state, targetNodeId).sorted(
            comparingLong(ShuttingDownNodeShards::graceDeadlineMillis)
        ).toList();
        final var deadlineShares = new ArrayList<DeadlineShare>();
        long shardsWithEarlierOrSameDeadline = 0;
        for (final var nodeShards : shardsOfShuttingDownNodes) {
            shardsWithEarlierOrSameDeadline += nodeShards.shards();
            final long remainingMs = Math.max(0, nodeShards.graceDeadlineMillis() - now);
            final long shardsCompetingForTarget = shardsWithEarlierOrSameDeadline + 1;
            deadlineShares.add(
                new DeadlineShare(
                    remainingMs,
                    shardsCompetingForTarget,
                    recoveriesOnTarget,
                    slotShareOfCompetingShards(slots, shardsCompetingForTarget, recoveriesOnTarget)
                )
            );
        }
        return deadlineShares;
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

    /// The slots of the target are shared equally by all the non-critical recoveries on it. This is the part of them that the
    /// `shardsCompetingForTarget` shards get: `min(N, slots * N / recoveries)`. It can be fractional. A cluster state that does not show
    /// all the competing shards yet gives fewer recoveries than shards, so the recoveries are taken to be at least the competing shards.
    private static double slotShareOfCompetingShards(int slots, long shardsCompetingForTarget, int recoveriesOnTarget) {
        return Math.min(
            shardsCompetingForTarget,
            (double) slots * shardsCompetingForTarget / Math.max(shardsCompetingForTarget, recoveriesOnTarget)
        );
    }

    /// The `shards` of a shutting-down node that are relocating to the target, which have to be through before the node's
    /// `graceDeadlineMillis`.
    private record ShuttingDownNodeShards(long graceDeadlineMillis, long shards) {}

    /// What one grace deadline leaves to the shards competing for the target: the time left until it, the shards competing for the target
    /// before it, all the recoveries on the target, and the share of the target's relocation limit of the competing shards.
    private record DeadlineShare(long remainingMs, long shardsCompetingForTarget, long recoveriesOnTarget, double slotShare) {

        /// The time left per recovery that shares the target's slots with the competing shards. It is what a plan budgets, so that the
        /// recoveries of the shards with later deadlines that finish are taken into account.
        double timePerRecoveryMs() {
            return remainingMs / (double) Math.max(shardsCompetingForTarget, recoveriesOnTarget);
        }

        double equalShareMs() {
            return remainingMs / (double) shardsCompetingForTarget * slotShare;
        }

        /// The extension for the time that finished recoveries saved: the per-recovery share above the one the previous plan budgeted,
        /// scaled to the slots of the competing shards.
        double waitForTimeSavedSince(double budgetedTimePerRecoveryMs) {
            final double recoveriesPerCompetingShard = Math.max(shardsCompetingForTarget, recoveriesOnTarget)
                / (double) shardsCompetingForTarget;
            return Math.max(0.0, timePerRecoveryMs() - budgetedTimePerRecoveryMs) * slotShare * recoveriesPerCompetingShard;
        }

        /// Whether the shards competing before this deadline get a slot each, so nobody queues behind anybody.
        boolean hasSlotForEachCompetingShard() {
            return slotShare >= shardsCompetingForTarget;
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
