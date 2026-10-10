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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;

import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;

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

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
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
    }

    TimeValue totalTimeoutCap() {
        return searchRecoveryWarmingTotalTimeoutCap;
    }

    boolean reevaluationEnabled() {
        return searchRecoveryWarmingTimeoutReevaluationEnabled;
    }

    TimeValue reevaluationAbortThreshold() {
        return searchRecoveryReevaluationAbortThreshold;
    }

    /// When to await search recovery warming (internal replicated-files path only). Relocation targets use relocation-specific timeouts or
    /// a computed share when the source is shutting down. Non-relocation: wait only if another active search shard copy exists and there
    /// is no cluster shutdown metadata, using [SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING].
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
            if (hasActiveShutdownForRemovalNodes(state)) {
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
        if (hasAnotherActiveSearchShardCopy(state, indexShard) && hasActiveShutdownForRemovalNodes(state) == false) {
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

    /// Counts shards in [ShardRoutingState#STARTED] state on `nodeId`: those that have not begun relocating yet, and are therefore
    /// still waiting for their turn to get a slice of the grace period.
    private static int countPendingShardsOnNode(ClusterState clusterState, String nodeId) {
        final RoutingNode node = clusterState.getRoutingNodes().node(nodeId);
        if (node == null) {
            return 0;
        }
        int count = 0;
        for (ShardRouting shard : node) {
            if (shard.state() == ShardRoutingState.STARTED) {
                count++;
            }
        }
        return count;
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
    ///
    /// The heuristics above decide the first plan of the shutdown phase. Only equal-share plans are
    /// [SearchRecoveryTimeout#extendable]. A re-evaluation of an equal-share plan (`previousPlan`) always yields an equal-share
    /// plan that only extends the wait by the time saved since then, regardless of which heuristic would win, see [#searchRecoveryTimeout].
    /// When no shard on the source is pending (not relocating yet), nobody waits behind this one, so the extension is all the remaining
    /// grace instead.
    private SearchRecoveryTimeout computeRelocationSourceShutdownWarmingTimeout(
        ClusterState state,
        String sourceNodeId,
        String targetNodeId,
        long totalBytesToWarm,
        @Nullable SearchRecoveryTimeout previousPlan
    ) {
        final var shutdown = state.metadata().nodeShutdowns().get(sourceNodeId);
        assert shutdown != null;
        TimeValue grace = shutdown.getGracePeriod();
        if (grace == null) {
            grace = searchRecoveryWarmingGracePeriodCap;
        }
        final long effectiveGraceMillis = min(grace.getMillis(), searchRecoveryWarmingGracePeriodCap.millis());
        final long now = threadPool.absoluteTimeInMillis();
        final long deadline = shutdown.getStartedAtMillis() + effectiveGraceMillis;
        final long remaining = deadline - now;
        if (remaining <= 0) {
            return new SearchRecoveryTimeout(TimeValue.ZERO, TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_GRACE_ELAPSED);
        }
        int shardsOnSource = countShardsOnNode(state, sourceNodeId);
        if (shardsOnSource <= 0) {
            shardsOnSource = 1;
        }
        final double equalShareMs = (remaining / (double) shardsOnSource) * searchRecoveryWarmingSourceShutdownShareFactor;

        // Data-volume-proportional heuristic: scale remaining time by the fraction of the warming cache this shard occupies.
        final long warmingCacheBytes = round(cacheService.getCacheSize() * searchRecoveryWarmingCacheRatio);
        // Re-evaluations pass the bytes still to warm (an approximation, see SharedBlobCacheWarmingService.ReevaluatingTimeoutTask#run),
        // the first calculation passes all of them. The baseline (the warming cache budget) is still fixed, since it's hard to do the
        // accounting of the bytes warmed for shards for all the relocations of a given node shutting down.
        final double dataVolumeMs = warmingCacheBytes > 0 ? ((double) totalBytesToWarm / warmingCacheBytes) * remaining : 0;
        int ongoingRelocations = countOngoingRelocationsBetween(state, sourceNodeId, targetNodeId);
        // The current shard is itself one such relocation; floor at 1 in case it is not yet visible on the source's RoutingNode.
        if (ongoingRelocations <= 0) {
            ongoingRelocations = 1;
        }

        if (previousPlan != null && previousPlan.isEqualSharePlan() && previousPlan.perShardShareMs() > 0) {
            // Re-evaluation of an equal-share plan: every shard still on the source was already budgeted previousPlan.perShardShareMs()
            // when the previous plan was computed, so only the part of the fresh share above that is time saved by shards that finished
            // early. When none did, the fresh share is not larger and there is nothing to extend by. The data-volume heuristic is
            // deliberately not consulted: it only decides the first plan of the shutdown phase, afterwards only saved time is handed out.
            // The exception is when no shard is pending on the source: nobody is waiting for a slice behind this one and the shards that
            // are already relocating wait concurrently, so the remaining grace would be wasted if not used.
            final double extensionMs = countPendingShardsOnNode(state, sourceNodeId) == 0
                ? remaining
                : max(0.0, equalShareMs - previousPlan.perShardShareMs()) * ongoingRelocations;
            return new SearchRecoveryTimeout(
                TimeValue.timeValueMillis(round(min(remaining, extensionMs))),
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
                TimeValue.timeValueMillis(round(min(remaining, dataVolumeMs * ongoingRelocations))),
                TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_DATA_VOLUME
            );
        }
        return new SearchRecoveryTimeout(
            TimeValue.timeValueMillis(round(min(remaining, equalShareMs * ongoingRelocations))),
            TimeoutContext.RELOCATION_SOURCE_SHUTTING_DOWN_EQUAL_SHARE,
            equalShareMs
        );
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
