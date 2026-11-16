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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;

import java.util.Map;

/// Computes how long search shard recovery should await offline warming (internal replicated-files path only), see
/// [#searchRecoveryTimeout].
public class SearchRecoveryTimeoutCalculationService {

    public static final String OFFLINE_WARMING_TIMEOUT_REEVALUATION_PREFIX =
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_timeout_reevaluation";

    /// Enabling causes offline warming timeouts to be reevaluated to see whether we can afford to continue warming before relocating and
    /// opening a shard. Enabling this setting should reduce blob store cache misses after shard relocations.
    public static final Setting<Boolean> OFFLINE_WARMING_TIMEOUT_REEVALUATION_ENABLED_SETTING = Setting.boolSetting(
        OFFLINE_WARMING_TIMEOUT_REEVALUATION_PREFIX + ".enabled",
        true,
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

    private final RelocationSourceShutdownTimeouts relocationSourceShutdownTimeouts;
    private final RelocationOtherNodeShutdownTimeouts relocationOtherNodeShutdownTimeouts;
    private volatile TimeValue searchRecoveryWarmingRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingNonRelocationTimeout;
    private volatile TimeValue searchRecoveryWarmingReshardTargetTimeout;
    private volatile TimeValue searchRecoveryWarmingTotalTimeoutCap;
    private volatile boolean searchRecoveryWarmingTimeoutReevaluationEnabled;
    private volatile TimeValue searchRecoveryReevaluationAbortThreshold;
    private volatile boolean ignoreNonSearchNodeShutdowns;

    public SearchRecoveryTimeoutCalculationService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        Settings nodeSettings,
        ByteSizeValue maxHeap
    ) {
        this.relocationSourceShutdownTimeouts = new RelocationSourceShutdownTimeouts(
            cacheService,
            threadPool,
            clusterSettings,
            nodeSettings,
            maxHeap
        );
        this.relocationOtherNodeShutdownTimeouts = new RelocationOtherNodeShutdownTimeouts(clusterSettings);
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
            final String targetNodeId = shardRouting.currentNodeId();
            if (state.metadata().nodeShutdowns().isNodeMarkedForRemoval(sourceNodeId)) {
                return relocationSourceShutdownTimeouts.timeout(state, sourceNodeId, targetNodeId, totalBytesToWarm, previousPlan);
            }
            if (hasShuttingDownSearchNodeWithShards(state)) {
                return relocationOtherNodeShutdownTimeouts.timeout();
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
}
