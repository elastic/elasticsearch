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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;

import java.util.Map;

/// Computes how long search shard recovery should await offline warming (internal replicated-files path only), see
/// [#searchRecoveryTimeout].
public class SearchRecoveryTimeoutCalculationService {

    public static final String SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_TOTAL_METRIC =
        "es.blob_cache_warming.search_recovery.drain_timeout_heuristic.total";
    public static final String SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_ATTRIBUTE_KEY = "es_drain_timeout_heuristic";

    public static final String OFFLINE_WARMING_TIMEOUT_REEVALUATION_PREFIX =
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_timeout_reevaluation";

    /// When true, drain-path search recovery warming timeouts may use per-shard warm volumes fetched from the
    /// shutting-down source node.
    public static final Setting<Boolean> SEARCH_OFFLINE_WARMING_WARM_VOLUMES_ENABLED_SETTING = Setting.boolSetting(
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".warm_volumes.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

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

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private final ShardWarmVolumes shardWarmVolumes;
    private final LongCounter drainTimeoutHeuristicTotalMetric;
    private volatile int maxConcurrentRelocationRecoveries;
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

    public SearchRecoveryTimeoutCalculationService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        Settings nodeSettings,
        ByteSizeValue maxHeap
    ) {
        this(cacheService, threadPool, clusterSettings, nodeSettings, maxHeap, ShardWarmVolumes.NOOP, TelemetryProvider.NOOP);
    }

    public SearchRecoveryTimeoutCalculationService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        Settings nodeSettings,
        ByteSizeValue maxHeap,
        ShardWarmVolumes shardWarmVolumes,
        TelemetryProvider telemetryProvider
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        this.shardWarmVolumes = shardWarmVolumes;
        this.drainTimeoutHeuristicTotalMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter(
                SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_TOTAL_METRIC,
                "Drain-path search recovery warming timeouts, broken down by the ["
                    + SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_ATTRIBUTE_KEY
                    + "] heuristic that produced the timeout",
                "count"
            );
        DataNodeRecoveryThrottlingSettings.watchIncomingRecoveryLimits(
            clusterSettings,
            nodeSettings,
            maxHeap,
            limits -> this.maxConcurrentRelocationRecoveries = limits.maxConcurrentRelocationRecoveries()
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
                    indexShard.shardId(),
                    totalBytesToWarm,
                    previousPlan
                );
            }
            if (hasShuttingDownSearchNodeWithShards(state)) {
                return new SearchRecoveryTimeout(
                    searchRecoveryWarmingRelocationWithShutdownTimeout,
                    TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT
                );
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

    /// Returns the warming timeout for a shard whose relocation source is shutting down. One per-shard heuristic is
    /// chosen, multiplied by `concurrentRelocations`, and capped at the remaining grace period. Warm-volume is chosen
    /// only when it is strictly larger than both of the others; otherwise data-volume is chosen when it is strictly
    /// larger than equal-share, and equal-share wins the remaining ties:
    ///
    /// 1. _Equal-share_: `factor * (deadline - now) / shardsOnSource * concurrentRelocations`, ensuring every
    /// shard on the shutting-down source gets a fair slice of the remaining grace period.
    /// 2. _Data-volume-proportional_ (contributes only when `totalBytesToWarm` is greater than zero): the fraction of the
    /// node's warming cache budget consumed by this shard's data multiplied by the remaining time,
    /// i.e. `(totalBytesToWarm / (cacheSize * cacheRatio)) * remaining * concurrentRelocations`.
    /// 3. _Warm-volume share_ (when [ShardWarmVolumes.CollectedWarmVolumes] exists for the current shutdown signal):
    /// `(warm volume / sum of warm volumes still competing for this target) * remaining * concurrentRelocations`.
    /// Copies already relocating to a different node are omitted from the sum. STARTED copies
    /// (no target yet) stay in it. An unknown shard contributes 0.
    ///
    /// with `deadline = start + min(metadata grace, cap)`. The timeout is capped at the remaining grace period.
    ///
    /// `concurrentRelocations` is this source's share of the target's, split between all relocations to the target from
    /// any shutting-down source. It may be below one slot, so the timeout may round down to zero (skipping warming). This
    /// is a rough estimate that ignores other recoveries competing for the target's slots.
    ///
    /// The heuristics above decide the first plan of the shutdown phase. Only equal-share plans are
    /// [SearchRecoveryTimeout#extendable]. Data-volume and warm-volume plans are not. A re-evaluation of an equal-share plan
    /// (`previousPlan`) always yields an equal-share plan that only extends the wait by the time saved since then, regardless of which
    /// heuristic would win, see [#searchRecoveryTimeout]. When all shards fit in the target's recovery slots, nobody waits behind this
    /// one, so each re-evaluation extends by at most one minute of the remaining grace.
    private SearchRecoveryTimeout computeRelocationSourceShutdownWarmingTimeout(
        ClusterState state,
        String sourceNodeId,
        String targetNodeId,
        ShardId shardId,
        long totalBytesToWarm,
        @Nullable SearchRecoveryTimeout previousPlan
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
        // Re-evaluations pass the bytes still to warm (an approximation, see #extendedTimeout),
        // the first calculation passes all of them. The baseline (the warming cache budget) is still fixed, since it's hard to do the
        // accounting of the bytes warmed for shards for all the relocations of a given node shutting down.
        final double dataVolumeMs = warmingCacheBytes > 0 ? ((double) totalBytesToWarm / warmingCacheBytes) * remaining : 0;

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
            // early. When none did, the fresh share is not larger and there is nothing to extend by. The data-volume and warm-volume
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

        // Warm-volume shares use the source's current-commit prefixes; they can differ from this target's WarmTarget plan.
        final double warmVolumeMs = warmVolumeShareMs(state, sourceNodeId, targetNodeId, shardId, remaining);

        // The decision below is per-shard whereas the three heuristics above assume all shards opt with the same heuristic.
        // This is an inherent problem of the fact that, during relocation, we don't know apriori all the shards that are going
        // to be relocated between two given nodes, so we can't know which of the three heuristics is more suitable overall.
        // Though the per-shard local decision here is OKish, because it's all relative to the remaining deadline and shards,
        // so the impact of currently choosing a different heuristic from previous (or future) relocating shards is partially mitigated.
        final double timeoutHeuristicMs;
        final TimeoutContext context;
        final String heuristic;
        final double perShardShareMs;
        if (warmVolumeMs > equalShareMs && warmVolumeMs > dataVolumeMs) {
            timeoutHeuristicMs = warmVolumeMs;
            heuristic = "warm_volume";
            context = TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_WARM_VOLUME;
            perShardShareMs = 0;
        } else if (dataVolumeMs > equalShareMs) {
            timeoutHeuristicMs = dataVolumeMs;
            heuristic = "data_volume";
            context = TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME;
            perShardShareMs = 0;
        } else {
            timeoutHeuristicMs = equalShareMs;
            heuristic = "equal_share";
            context = TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE;
            perShardShareMs = equalShareMs;
        }
        drainTimeoutHeuristicTotalMetric.incrementBy(1, Map.of(SEARCH_RECOVERY_DRAIN_TIMEOUT_HEURISTIC_ATTRIBUTE_KEY, heuristic));
        return new SearchRecoveryTimeout(
            TimeValue.timeValueMillis(Math.round(Math.min(remaining, timeoutHeuristicMs * concurrentRelocations))),
            context,
            perShardShareMs
        );
    }

    /// Per-shard warm-volume share of `remaining`, or `0` when the map cannot be used for this shard.
    /// Copies already relocating from `sourceNodeId` to a node other than `targetNodeId` are left out of
    /// the denominator: they are committed elsewhere. STARTED copies remain in the sum until they get a target.
    private double warmVolumeShareMs(ClusterState state, String sourceNodeId, String targetNodeId, ShardId shardId, long remaining) {
        var entry = shardWarmVolumes.get(state, sourceNodeId);
        if (entry == null) {
            return 0;
        }
        final var sourceNode = state.getRoutingNodes().node(sourceNodeId);
        if (sourceNode == null) {
            return 0;
        }
        long sourceWarmVolumeSum = 0L;
        Long thisShardVolume = null;
        for (ShardRouting routing : sourceNode) {
            assert routing.isSearchable();
            // Skip shards that are relocating to a different target node than the one we're computing for.
            if (routing.relocating() && targetNodeId.equals(routing.relocatingNodeId()) == false) {
                continue;
            }
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
            return 0;
        }
        return (thisShardVolume / (double) sourceWarmVolumeSum) * remaining;
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
