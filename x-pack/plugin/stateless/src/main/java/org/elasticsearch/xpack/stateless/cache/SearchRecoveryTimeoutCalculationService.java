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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;

import java.util.Map;
import java.util.stream.Stream;

/// Computes how long search shard recovery should await offline warming (internal replicated-files path only), see
/// [#searchRecoveryTimeout].
public class SearchRecoveryTimeoutCalculationService {

    public static final String OFFLINE_WARMING_TIMEOUT_REEVALUATION_PREFIX =
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_timeout_reevaluation";

    /// Enabling causes offline warming timeouts to be reevaluated to see whether we can afford to continue warming before relocating and
    /// opening a shard. Enabling this setting should reduce blob store cache misses after shard relocations.
    public static final Setting<Boolean> OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING = Setting.boolSetting(
        OFFLINE_WARMING_TIMEOUT_REEVALUATION_PREFIX + ".enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /// Minimum re-evaluation slice that is worth rescheduling. When the remaining grace-period budget would produce a slice shorter than
    /// this value, the re-evaluation loop terminates and recovery resumes immediately. Setting this too low (approaching zero) risks a
    /// busy-reschedule loop; setting it too high causes the loop to abort earlier than necessary, reducing the warming window.
    public static final Setting<TimeValue> OFFLINE_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING = Setting.timeSetting(
        OFFLINE_WARMING_TIMEOUT_REEVALUATION_PREFIX + ".abort_threshold",
        TimeValue.timeValueMillis(300L),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /// Upper bound on the total time a recovering search shard may wait for warming, summed over the initial timeout and all
    /// re-evaluation extensions. When the relocation source is shutting down, the grace deadline bounds each slice as well, and the lower
    /// of the two applies. The initial timeout is never capped, so `0` disables extensions.
    public static final Setting<TimeValue> OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING = Setting.timeSetting(
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_total_timeout_cap",
        TimeValue.timeValueMinutes(14),
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /// Only shutdowns of search nodes that still hold shards shorten or skip the warming wait, since only their shards can relocate to
    /// a search node. When disabled, any removal-type shutdown does.
    public static final Setting<Boolean> OFFLINE_WARMING_IGNORE_NON_SEARCH_NODE_SHUTDOWNS_SETTING = Setting.boolSetting(
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_ignore_non_search_node_shutdowns",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /// When enabled, a shard relocating from a healthy source while a search node is shutting down shares the target's free relocation
    /// slots with the shards of the shutting-down node, so that it does not delay them past their grace period, see
    /// [#relocationWithOtherNodeShutdownTimeout]. When disabled, such a shard always waits for the plain relocation-with-shutdown timeout.
    public static final Setting<Boolean> OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING = Setting.boolSetting(
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_share_slots_with_shutting_down_nodes",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private volatile int maxConcurrentRelocationRecoveries;
    private volatile int masterConcurrentIncomingRecoveries;
    private volatile TimeValue searchRecoveryWarmingRelocationWithShutdownTimeout;
    private volatile TimeValue searchRecoveryWarmingRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingNonRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingReshardTargetTimeout;
    private volatile TimeValue searchRecoveryWarmingGracePeriodCap;
    private volatile TimeValue searchRecoveryWarmingTotalTimeoutCap;
    private volatile boolean searchRecoveryWarmingTimeoutReevaluationEnabled;
    private volatile TimeValue searchRecoveryReevaluationAbortThreshold;
    private volatile double searchRecoveryWarmingSourceShutdownShareFactor;
    private volatile double searchRecoveryWarmingCacheRatio;
    private volatile boolean searchRecoveryWarmingFilterSourceShardsByTarget;
    private volatile boolean ignoreNonSearchNodeShutdowns;
    private volatile boolean shareSlotsWithShuttingDownNodes;

    public SearchRecoveryTimeoutCalculationService(
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
            limits -> this.maxConcurrentRelocationRecoveries = limits.maxConcurrentRelocationRecoveries()
        );
        clusterSettings.initializeAndWatch(
            ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
            value -> this.masterConcurrentIncomingRecoveries = value
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
            OFFLINE_WARMING_TOTAL_TIMEOUT_CAP_SETTING,
            value -> this.searchRecoveryWarmingTotalTimeoutCap = value
        );
        clusterSettings.initializeAndWatch(
            OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING,
            value -> this.searchRecoveryWarmingTimeoutReevaluationEnabled = value
        );
        clusterSettings.initializeAndWatch(
            OFFLINE_WARMING_TIMEOUT_REEVALUATION_ABORT_THRESHOLD_SETTING,
            value -> this.searchRecoveryReevaluationAbortThreshold = value
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
        clusterSettings.initializeAndWatch(
            OFFLINE_WARMING_IGNORE_NON_SEARCH_NODE_SHUTDOWNS_SETTING,
            value -> this.ignoreNonSearchNodeShutdowns = value
        );
        clusterSettings.initializeAndWatch(
            OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING,
            value -> this.shareSlotsWithShuttingDownNodes = value
        );
    }

    /// Decides, on expiry of `latestPlan`, whether the wait continues and for how long: returns the re-evaluated plan with its timeout
    /// capped to the total budget left after `elapsed`, or [SearchRecoveryTimeout#skip()] when the wait must end.
    ///
    /// `totalBytesToWarm` is the total the warming was asked to fetch and `bytesWarmedSoFar` what it fetched since it started. The
    /// difference is approximate: `bytesWarmedSoFar` also counts bytes that are not part of `totalBytesToWarm` (e.g. header/footer
    /// reads), and regions that were already cached are never counted, so this can under- or overestimate the bytes still to warm.
    SearchRecoveryTimeout extendedTimeout(
        ClusterState state,
        IndexShard shard,
        long totalBytesToWarm,
        long bytesWarmedSoFar,
        SearchRecoveryTimeout latestPlan,
        TimeValue elapsed
    ) {
        if (searchRecoveryWarmingTimeoutReevaluationEnabled == false || latestPlan.extendable() == false) {
            return SearchRecoveryTimeout.skip();
        }
        final var remainingBytesToWarm = Math.max(0L, totalBytesToWarm - bytesWarmedSoFar);
        final var newPlan = searchRecoveryTimeout(state, shard, remainingBytesToWarm, latestPlan);
        if (newPlan.shouldExtendAfter(latestPlan) == false) {
            return SearchRecoveryTimeout.skip();
        }
        final var newTimeout = cappedToTotalBudget(newPlan.timeout(), elapsed);
        if (newTimeout.compareTo(searchRecoveryReevaluationAbortThreshold) < 0) {
            return SearchRecoveryTimeout.skip();
        }
        return new SearchRecoveryTimeout(newTimeout, newPlan.timeoutContext(), newPlan.perShardShareMs());
    }

    /// Caps `timeout` so that the total wait does not exceed the total timeout cap. The same cap applies whichever context the plan
    /// was computed for, so a wait cannot gain budget by switching contexts: when the relocation source starts shutting down, the
    /// plan's timeout is already bounded by the grace deadline and the lower of the two wins.
    private TimeValue cappedToTotalBudget(TimeValue timeout, TimeValue elapsed) {
        final var budgetLeft = TimeValue.timeValueMillis(Math.max(0L, searchRecoveryWarmingTotalTimeoutCap.millis() - elapsed.millis()));
        return TimeValue.min(budgetLeft, timeout);
    }

    /// When to await search recovery warming (internal replicated-files path only). Relocation targets use relocation-specific timeouts or
    /// a computed share when the source is shutting down. Non-relocation: wait only if another active search shard copy exists and no
    /// search node is shutting down, using [SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING].
    public SearchRecoveryTimeout searchRecoveryTimeout(
        ClusterState state,
        IndexShard indexShard,
        long totalBytesToWarm,
        @Nullable SearchRecoveryTimeout previousPlan
    ) {
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
                    totalBytesToWarm,
                    previousPlan
                );
            }
            if (hasShuttingDownSearchNodeWithShards(state)) {
                if (shareSlotsWithShuttingDownNodes == false) {
                    return new SearchRecoveryTimeout(
                        searchRecoveryWarmingRelocationWithShutdownTimeout,
                        TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT
                    );
                }
                return relocationWithOtherNodeShutdownTimeout(state, shardRouting.currentNodeId(), totalBytesToWarm);
            }
            return new SearchRecoveryTimeout(
                searchRecoveryWarmingRelocationTimeout,
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_NO_CLUSTER_SHUTDOWN
            );
        }
        if (hasAnotherActiveSearchShardCopy(state, indexShard) && hasShuttingDownSearchNodeWithShards(state) == false) {
            return new SearchRecoveryTimeout(searchRecoveryWarmingNonRelocationTimeout, TimeoutContext.NON_RELOCATION_ANOTHER_ACTIVE_COPY);
        }
        if (searchRecoveryWarmingReshardTargetTimeout.millis() > 0 && isReshardSplitTarget(state, indexShard.shardId())) {
            return new SearchRecoveryTimeout(searchRecoveryWarmingReshardTargetTimeout, TimeoutContext.RESHARD_SPLIT_TARGET);
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

    /// Returns the warming timeout for a shard relocating to `targetNodeId` from a source that is not shutting down, while other nodes are,
    /// so that it does not hold a relocation slot on the target that a shard of a shutting-down node needs.
    ///
    /// The demand of shutting-down nodes on the target is pessimistically every search shard already relocating to it from them plus every
    /// one still started on them, as the master throttles per target and we cannot know where those will go.
    ///
    /// - With enough free slots for all of that demand, the relocation-with-shutdown timeout applies in full.
    /// - Otherwise this shard is one more participant (`P`) and the free slots are shared: each gets `slots / P` of the time left until the
    /// earliest grace deadline, using the larger of the equal share and the data-volume share as in
    /// [#computeRelocationSourceShutdownWarmingTimeout]. The result never exceeds the relocation-with-shutdown timeout.
    ///
    /// The shares can add up to more than the time available. They are upper bounds, and shards usually finish warming earlier.
    private SearchRecoveryTimeout relocationWithOtherNodeShutdownTimeout(ClusterState state, String targetNodeId, long totalBytesToWarm) {
        final var targetNode = state.getRoutingNodes().node(targetNodeId);
        int incomingOnTarget = 0;
        int relocatingFromShuttingDown = 0;
        if (targetNode != null) {
            final var nodeShutdowns = state.metadata().nodeShutdowns();
            for (ShardRouting shard : targetNode.initializing()) {
                incomingOnTarget++;
                final String sourceNodeId = shard.relocatingNodeId();
                if (sourceNodeId != null && shard.isPromotableToPrimary() == false && nodeShutdowns.isNodeMarkedForRemoval(sourceNodeId)) {
                    relocatingFromShuttingDown++;
                }
            }
        }
        final long recoverySlotsNeeded = (long) relocatingFromShuttingDown + countPendingSearchShardsOnShuttingDownNodes(state);
        // everything on the target that is not from a shutting-down node, including this shard, holds a slot regardless
        final long freeRecoverySlots = (long) effectiveIncomingSlots() - (incomingOnTarget - relocatingFromShuttingDown);
        if (recoverySlotsNeeded <= freeRecoverySlots) {
            // we are below the demand, so we can treat this as if it were not a shutting-down node (longer timeout)
            return new SearchRecoveryTimeout(
                searchRecoveryWarmingRelocationWithShutdownTimeout,
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE
            );
        }
        final long timeRemaining = Math.max(0L, earliestGraceDeadline(state) - threadPool.absoluteTimeInMillis());
        // the share never exceeds the time left until the earliest grace deadline, nor the relocation-with-shutdown timeout
        final long maxShareMs = Math.min(timeRemaining, searchRecoveryWarmingRelocationWithShutdownTimeout.millis());
        final long slotsToShare = Math.max(1L, freeRecoverySlots);

        final double equalShareMs = (timeRemaining / (double) (recoverySlotsNeeded + 1)) * slotsToShare;
        final double dataVolumeMs = dataVolumeShareMs(totalBytesToWarm, timeRemaining) * slotsToShare;

        if (dataVolumeMs > equalShareMs) {
            return new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(Math.round(Math.min(maxShareMs, dataVolumeMs))),
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_DATA_VOLUME
            );
        } else {
            return new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(Math.round(Math.min(maxShareMs, equalShareMs))),
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT
            );
        }
    }

    /// The number of incoming recoveries a node accepts at once: the data node limit, which is unbounded unless configured, and the
    /// master's per node limit, which is what bounds relocations to a node when the data node limit is not configured.
    private int effectiveIncomingSlots() {
        return Math.min(maxConcurrentRelocationRecoveries, masterConcurrentIncomingRecoveries);
    }

    /// The data-volume-proportional heuristic: `remainingMs` scaled by the fraction of the warming cache budget that `totalBytesToWarm`
    /// takes, or `0` when there is no warming cache. The baseline (the warming cache budget) is fixed, since it's hard to account for
    /// the bytes warmed by all the shards relocating from a given shutting-down node.
    private double dataVolumeShareMs(long totalBytesToWarm, long remainingMs) {
        final long warmingCacheBytes = Math.round(cacheService.getCacheSize() * searchRecoveryWarmingCacheRatio);
        return warmingCacheBytes > 0 ? ((double) totalBytesToWarm / warmingCacheBytes) * remainingMs : 0;
    }

    private static long countPendingSearchShardsOnShuttingDownNodes(ClusterState state) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        long count = 0;
        for (String nodeId : nodeShutdowns.getAllNodeIds()) {
            if (nodeShutdowns.isNodeMarkedForRemoval(nodeId)) {
                count += countPendingSearchShardsOnNode(state, nodeId);
            }
        }
        return count;
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

    /// The earliest capped grace deadline of the nodes marked for removal that are still in the cluster.
    private long earliestGraceDeadline(ClusterState state) {
        return removalShutdownsOfNodesInCluster(state).mapToLong(this::graceDeadline).min().orElse(Long.MAX_VALUE);
    }

    private long graceDeadline(SingleNodeShutdownMetadata shutdown) {
        TimeValue grace = shutdown.getGracePeriod();
        if (grace == null) {
            grace = searchRecoveryWarmingGracePeriodCap;
        }
        return shutdown.getStartedAtMillis() + Math.min(grace.getMillis(), searchRecoveryWarmingGracePeriodCap.millis());
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

    /// Whether a search node still holding shards is shutting down for removal, i.e. may need this node's relocation slots. Any
    /// removal-type shutdown of a node in the cluster when [#OFFLINE_WARMING_IGNORE_NON_SEARCH_NODE_SHUTDOWNS_SETTING] is disabled.
    private boolean hasShuttingDownSearchNodeWithShards(ClusterState state) {
        for (Map.Entry<String, SingleNodeShutdownMetadata> entry : state.metadata().nodeShutdowns().getAll().entrySet()) {
            if (entry.getValue().getType().isRemovalType() == false) {
                continue;
            }
            final DiscoveryNode node = state.nodes().get(entry.getKey());
            if (node == null) {
                continue;
            }
            if (ignoreNonSearchNodeShutdowns == false) {
                return true;
            }
            if (node.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()) == false) {
                continue;
            }
            final RoutingNode routingNode = state.getRoutingNodes().node(node.getId());
            if (routingNode != null && !routingNode.isEmpty()) {
                return true;
            }
        }
        return false;
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
    /// `concurrentRelocations` is this source's share of the target's, split between all relocations to the target from
    /// any shutting-down source. It may be below one slot, so the timeout may round down to zero (skipping warming). This
    /// is a rough estimate that ignores other recoveries competing for the target's slots.
    ///
    /// The heuristics above decide the first plan of the shutdown phase. Only equal-share plans are
    /// [SearchRecoveryTimeout#extendable]. A re-evaluation of an equal-share plan (`previousPlan`) always yields an equal-share
    /// plan that only extends the wait by the time saved since then, regardless of which heuristic would win, see [#searchRecoveryTimeout].
    /// When all shards fit in the target's recovery slots, nobody waits behind this one, so the extension is all the remaining grace.
    private SearchRecoveryTimeout computeRelocationSourceShutdownWarmingTimeout(
        ClusterState state,
        String sourceNodeId,
        String targetNodeId,
        long totalBytesToWarm,
        @Nullable SearchRecoveryTimeout previousPlan
    ) {
        final var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        assert shutdown != null;
        final long now = threadPool.absoluteTimeInMillis();
        final long deadline = graceDeadline(shutdown);
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

        // Re-evaluations pass the bytes still to warm (an approximation, see #extendedTimeout), the first calculation passes all of them.
        final double dataVolumeMs = dataVolumeShareMs(totalBytesToWarm, remaining);

        // The current shard is itself one such relocation, floor at 1 in case it is not yet visible on the source's RoutingNode.
        final int relocationsFromSourceToTarget = Math.max(1, countRelocationsBetween(state, sourceNodeId, targetNodeId));
        final int allRelocationsToTarget = Math.max(1, countRelocationsFromShuttingDownNodesTo(state, targetNodeId));

        // Share the target's cap between shutting-down sources, preserving fractional shares below one slot.
        final double concurrentRelocations = Math.min(
            relocationsFromSourceToTarget,
            (double) maxConcurrentRelocationRecoveries * relocationsFromSourceToTarget / allRelocationsToTarget
        );

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

    /// Counts relocations whose source is `sourceNodeId` and whose target is `targetNodeId` (i.e. shards relocating
    /// from `sourceNodeId` to `targetNodeId`, as seen from the source's `RoutingNode`).
    private static int countRelocationsBetween(ClusterState state, String sourceNodeId, String targetNodeId) {
        final var sourceNode = state.getRoutingNodes().node(sourceNodeId);
        if (sourceNode == null) {
            return 0;
        }
        int count = 0;
        for (ShardRouting shard : sourceNode.relocating()) {
            if (shard.isPromotableToPrimary() == false && targetNodeId.equals(shard.relocatingNodeId())) {
                count++;
            }
        }
        return count;
    }

    /// Counts the shards relocating from nodes marked for removal to `targetNodeId`.
    private static int countRelocationsFromShuttingDownNodesTo(ClusterState state, String targetNodeId) {
        final var nodeShutdowns = state.metadata().nodeShutdowns();
        int count = 0;
        for (String nodeId : nodeShutdowns.getAllNodeIds()) {
            if (nodeShutdowns.isNodeMarkedForRemoval(nodeId)) {
                count += countRelocationsBetween(state, nodeId, targetNodeId);
            }
        }
        return count;
    }
}
