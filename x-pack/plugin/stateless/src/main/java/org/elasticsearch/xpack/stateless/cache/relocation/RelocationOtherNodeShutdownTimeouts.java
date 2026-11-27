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

    /// Returns the warming timeout for a shard relocating to `targetNodeId` from a source that is not shutting down, while some other nodes
    /// are. The shard must not use the slots of the target for longer than its fair share, or the shards of the shutting-down nodes may
    /// miss their grace deadlines. The fair share is worked out for each grace deadline:
    ///
    /// 1. The recoveries that compete for the slots up to the deadline are: this shard, the shards of the shutting-down nodes due by it and
    /// the other recoveries not related to shutting-down nodes on the target. The shards due later have time to spare, so they are not
    /// counted.
    /// 2. We estimate the wait of this shard as the time left until the deadline divided by the number of those recoveries, scaled to
    /// the slots they can use at once. The estimate is only for this shard: the other recoveries are not timed by it, but by their own
    /// plans.
    ///
    /// The smallest of these shares is the wait. It is a fair heuristic, not a guarantee: it makes it less likely that this shard delays
    /// the shards of the shutting-down nodes past their grace deadlines, but it does not rule it out.
    ///
    /// A re-evaluation (`previousPlan`) extends the wait only by the time that finished recoveries saved: the part of the fresh equal
    /// share above the one the previous plan gave.
    ///
    /// The plain relocation-with-shutdown timeout (fallback) applies when everything fits in the slots, and when
    /// [SearchRecoveryTimeoutCalculationService#OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING] is disabled or a
    /// shutting-down node still holds shards that have not begun to relocate, as then the relocations to the target are not all known.
    public SearchRecoveryTimeout timeout(ClusterState state, String targetNodeId, @Nullable SearchRecoveryTimeout previousPlan) {
        if (shareSlotsWithShuttingDownNodes == false || SearchShardRelocations.hasPendingRelocationsOnShuttingDownNodes(state)) {
            return fallbackTimeout();
        }
        final var deadlineShares = deadlineShares(state, targetNodeId);
        // The latest deadline counts every recovery on the target, so we check it as the worst case
        if (deadlineShares.isEmpty() || deadlineShares.getFirst().noShardWaitingForRelocationSlot()) {
            // no shutting-down node waits for this target before its deadline, or nobody queues behind anybody: treat this as if it were
            // not a shutting-down node
            return new SearchRecoveryTimeout(
                searchRecoveryWarmingRelocationWithShutdownTimeout,
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE
            );
        }

        // Use the deadline with the smallest equal share: as a fair heuristic, it is the one this shard is the most likely to delay.
        final var tightestDeadline = Collections.min(deadlineShares, comparingDouble(DeadlineShare::waitMs));
        if (tightestDeadline.remainingMs() == 0) {
            // the warming cap does not free a slot, so the shards of the passed deadline may still be queued for one
            return new SearchRecoveryTimeout(TimeValue.ZERO, TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_GRACE_ELAPSED);
        }
        // The previous share may come from another deadline. That is fine: the extension is capped by the share of the tightest one.
        final double waitMs;
        if (previousPlan != null && previousPlan.isEqualSharePlan()) {
            waitMs = tightestDeadline.extensionMs(previousPlan);
        } else {
            waitMs = tightestDeadline.waitMs();
        }
        return new SearchRecoveryTimeout(
            TimeValue.timeValueMillis(Math.round(waitMs)),
            TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE,
            tightestDeadline.perShardShareMs()
        );
    }

    /// What each grace deadline leaves to the shards competing for the target, from the latest deadline to the earliest.
    /// A deadline that has passed leaves nothing: the warming cap does not free a slot, so its shards may still be queued for one.
    private List<DeadlineShare> deadlineShares(final ClusterState state, final String targetNodeId) {
        final int recoveriesOnTarget = SearchShardRelocations.countNonCriticalInitializing(state, targetNodeId);
        final int slots = targetRelocationSlotsEstimator.slotsForNonCriticalRecoveries();
        final long now = threadPool.absoluteTimeInMillis();
        // the shards with a later deadline are counted while going from the latest deadline to the earliest
        final var shardsOfShuttingDownNodes = shardsOfShuttingDownNodes(state, targetNodeId).sorted(
            comparingLong(ShuttingDownNodeShards::graceDeadlineMillis).reversed()
        ).toList();
        final var deadlineShares = new ArrayList<DeadlineShare>(shardsOfShuttingDownNodes.size());
        long shardsWithLaterDeadline = 0;
        for (final var nodeShards : shardsOfShuttingDownNodes) {
            final long remainingMs = Math.max(0, nodeShards.graceDeadlineMillis() - now);
            // All the shards of the shutting-down nodes are recoveries on the target here, as none is pending. The shards with a later
            // deadline have time to spare, so they only count towards their own deadline, and the other recoveries, which include this
            // shard, compete for every deadline. The state may be newer than the routing this shard recovers with, and no longer have
            // the shard among the recoveries if its relocation was cancelled, in which case it is not counted. This does not matter for
            // the estimate.
            //
            // The recoveries of a deadline are never fewer than the shards of its own node, and there is at least one: they are among the
            // non-critical recoveries on the target, as every relocation is, and only the shards with a later deadline are taken off. So
            // the share, which divides by the recoveries, is well defined. The `max` only guards that, should a state ever break it.
            deadlineShares.add(new DeadlineShare(remainingMs, Math.max(1, recoveriesOnTarget - shardsWithLaterDeadline), slots));
            shardsWithLaterDeadline += nodeShards.shards();
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

    /// The `shards` of a shutting-down node that are relocating to the target, which have to be through before the node's
    /// `graceDeadlineMillis`.
    private record ShuttingDownNodeShards(long graceDeadlineMillis, long shards) {}

    /// What one grace deadline leaves to the shards competing for the target: the time left until it, the non-critical recoveries that
    /// share the target's slots with them, and the slots. The `recoveries` are the shards due by the deadline, this shard, and the other
    /// recoveries on the target, but not the shards with a later deadline.
    private record DeadlineShare(long remainingMs, long recoveries, long slots) {

        /// The time left per recovery that shares the target's slots with the competing shards. It is what a plan budgets, so that the
        /// other recoveries that finish are taken into account.
        double perShardShareMs() {
            return remainingMs / (double) recoveries;
        }

        /// How many of the recoveries can run at once: all of them, up to the slots.
        private long concurrentRecoveries() {
            return Math.min(recoveries, slots);
        }

        /// The per-shard share scaled to the recoveries that run at once, which is how long they can use the slots together.
        double waitMs() {
            return perShardShareMs() * concurrentRecoveries();
        }

        /// The extension for the time that finished recoveries saved: the per-recovery share above the one the previous plan budgeted,
        /// scaled to the recoveries that run at once.
        double extensionMs(SearchRecoveryTimeout previousPlan) {
            return Math.max(0.0, perShardShareMs() - previousPlan.perShardShareMs()) * concurrentRecoveries();
        }

        /// Whether all the recoveries get a slot each, so nobody queues behind anybody.
        boolean noShardWaitingForRelocationSlot() {
            return slots >= recoveries;
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
