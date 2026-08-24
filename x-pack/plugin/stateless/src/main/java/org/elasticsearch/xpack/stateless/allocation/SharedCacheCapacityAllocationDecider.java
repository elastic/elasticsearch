/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.FrequencyCappedAction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Map;

import static org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements.NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT;

/**
 * Deprioritizes allocation of search shards to a node whose shared cache is already, or would become, over-subscribed, by returning
 * {@link Decision#NOT_PREFERRED} from {@link #canAllocate}, and deprioritizes leaving a search shard on a node whose shared cache is
 * already over-subscribed by returning {@link Decision#NOT_PREFERRED} from {@link #canRemain}. The decider reasons about the
 * boosted/unboosted cache commitment data recorded in {@link org.elasticsearch.cluster.ClusterInfo#getShardCacheRequirements()} and
 * {@link org.elasticsearch.cluster.ClusterInfo#getNodeCacheSizeAndCommitments()}. The decider as a whole is disabled by default via
 * {@link #ENABLED_SETTING}.
 */
public class SharedCacheCapacityAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(SharedCacheCapacityAllocationDecider.class);
    public static final String NAME = "shared_cache_capacity";
    private static final FeatureFlag SHARED_CACHE_CAPACITY_DECIDER_FEATURE_FLAG = new FeatureFlag("shared_cache_capacity_decider");

    /**
     * Whether the decider considers only boosted cache commitment, or the combined boosted and unboosted commitment, when comparing
     * a node's cache usage against the configured watermarks.
     */
    public enum CacheAccountingMode {
        BOOSTED,
        TOTAL;

        public long getCurrentCommitmentBytes(NodeCacheSizeAndCommitments nodeCacheSizeAndCommitments) {
            return switch (this) {
                case BOOSTED -> nodeCacheSizeAndCommitments.boostedCacheCommitmentInBytes();
                case TOTAL -> Math.addExact(
                    nodeCacheSizeAndCommitments.boostedCacheCommitmentInBytes(),
                    nodeCacheSizeAndCommitments.unboostedCacheCommitmentInBytes()
                );
            };
        }

        public long getShardRequirementBytes(BoostedAndUnboostedCacheRequirements requirement) {
            return switch (this) {
                case BOOSTED -> getRequirementWithFallback(requirement.boostedCacheRequirementInBytes());
                case TOTAL -> Math.addExact(
                    getRequirementWithFallback(requirement.boostedCacheRequirementInBytes()),
                    getRequirementWithFallback(requirement.unboostedCacheRequirementInBytes())
                );
            };
        }

        private static long getRequirementWithFallback(long requirementInBytes) {
            return requirementInBytes == NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT ? 0L : requirementInBytes;
        }
    }

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.shared_cache_capacity.enabled",
        SHARED_CACHE_CAPACITY_DECIDER_FEATURE_FLAG.isEnabled(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<CacheAccountingMode> ACCOUNTING_MODE_SETTING = Setting.enumSetting(
        CacheAccountingMode.class,
        "cluster.routing.allocation.shared_cache_capacity.accounting_mode",
        CacheAccountingMode.BOOSTED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The {@code canAllocate} threshold. Above this, the decider returns {@link Decision#NOT_PREFERRED} for new allocations.
     */
    public static final Setting<RatioValue> LOW_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.shared_cache_capacity.watermark.low",
        "99%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The {@code canRemain} threshold. Above this, the decider returns {@link Decision#NOT_PREFERRED} for shards already allocated to
     * the node.
     */
    public static final Setting<RatioValue> HIGH_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.shared_cache_capacity.watermark.high",
        "100%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * A dynamic override for {@link #canRemain} specifically. When this is disabled, {@link #canRemain} always returns
     * {@link Decision#YES}, even when the decider as a whole ({@link #ENABLED_SETTING}) is enabled.
     */
    public static final Setting<Boolean> CAN_REMAIN_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.shared_cache_capacity.can_remain.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Rate-limits how often the debug-level explanation message is logged for a given canAllocate decision path.
     */
    public static final Setting<TimeValue> MINIMUM_LOGGING_INTERVAL = Setting.timeSetting(
        "cluster.routing.allocation.shared_cache_capacity.log_interval",
        TimeValue.timeValueMinutes(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Rate-limits how often {@link SharedCacheCapacityMonitor} retries a reroute for nodes still over the high watermark. A newly
     * observed watermark transition always reroutes immediately and is never subject to this interval.
     */
    public static final Setting<TimeValue> REROUTE_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.shared_cache_capacity.reroute_interval",
        TimeValue.timeValueSeconds(15),
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Decision YES_SHARED_CACHE_CAPACITY_DECIDER_DISABLED = Decision.single(
        Decision.Type.YES,
        NAME,
        "shared cache capacity decider is disabled"
    );

    private static final Decision YES_SHARED_CACHE_CAPACITY_FOR_SEARCH_NODE_ONLY = Decision.single(
        Decision.Type.YES,
        NAME,
        "shared cache capacity decider is applicable only to search nodes"
    );

    private static final Decision YES_CAN_REMAIN_DISABLED = Decision.single(
        Decision.Type.YES,
        NAME,
        "shared cache capacity decider's canRemain check is disabled"
    );

    private final FrequencyCappedAction logCanAllocateMessage;
    private final FrequencyCappedAction logCanRemainMessage;
    private volatile boolean enabled;
    private volatile boolean canRemainEnabled;
    private volatile CacheAccountingMode accountingMode;
    private volatile RatioValue lowWatermark;
    private volatile RatioValue highWatermark;

    public SharedCacheCapacityAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(ENABLED_SETTING, value -> this.enabled = value);
        clusterSettings.initializeAndWatch(CAN_REMAIN_ENABLED_SETTING, value -> this.canRemainEnabled = value);
        clusterSettings.initializeAndWatch(ACCOUNTING_MODE_SETTING, value -> this.accountingMode = value);
        clusterSettings.initializeAndWatch(LOW_WATERMARK_SETTING, value -> this.lowWatermark = value);
        clusterSettings.initializeAndWatch(HIGH_WATERMARK_SETTING, value -> this.highWatermark = value);
        logCanAllocateMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        logCanRemainMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        clusterSettings.initializeAndWatch(MINIMUM_LOGGING_INTERVAL, value -> {
            logCanAllocateMessage.setMinInterval(value);
            logCanRemainMessage.setMinInterval(value);
        });
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (enabled == false) {
            return YES_SHARED_CACHE_CAPACITY_DECIDER_DISABLED;
        }

        if (node.node().getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE) == false) {
            return YES_SHARED_CACHE_CAPACITY_FOR_SEARCH_NODE_ONLY;
        }

        final Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments = allocation.clusterInfo()
            .getNodeCacheSizeAndCommitments();
        assert nodeCacheSizeAndCommitments != null;
        final NodeCacheSizeAndCommitments nodeCommitments = nodeCacheSizeAndCommitments.get(node.nodeId());
        if (nodeCommitments == null) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "no cache size and commitment data available for node [%s]",
                node.getShortNodeDescription()
            );
        }

        // Snapshot the accounting mode once so a concurrent settings update can't mix boosted and total values within a single decision.
        final CacheAccountingMode accountingMode = this.accountingMode;

        final long currentCommitmentBytes = accountingMode.getCurrentCommitmentBytes(nodeCommitments);
        final long thresholdBytes = (long) (nodeCommitments.cacheSizeInBytes() * lowWatermark.getAsRatio());

        final boolean isDebugEnabled = logger.isDebugEnabled();
        if (currentCommitmentBytes > thresholdBytes) {
            if (isDebugEnabled || allocation.debugDecision()) {
                final String message = Strings.format(
                    "node [%s] cache commitment [%d] bytes already exceeds the low watermark [%d] bytes (accounting mode [%s])",
                    node.getShortNodeDescription(),
                    currentCommitmentBytes,
                    thresholdBytes,
                    accountingMode
                );
                if (isDebugEnabled) {
                    logCanAllocateMessage.maybeExecute(() -> logger.debug(message));
                }
                return allocation.decision(Decision.NOT_PREFERRED, NAME, message);
            } else {
                return Decision.NOT_PREFERRED;
            }
        }

        final Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements = allocation.clusterInfo()
            .getShardCacheRequirements();
        assert shardCacheRequirements != null;
        final BoostedAndUnboostedCacheRequirements shardCacheRequirement = shardCacheRequirements.get(shardRouting.shardId());
        if (shardCacheRequirement == null) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "no cache requirement data available for shard [%s], node [%s] cache commitment [%d] bytes is below the low watermark "
                    + "[%d] bytes",
                shardRouting.shardId(),
                node.getShortNodeDescription(),
                currentCommitmentBytes,
                thresholdBytes
            );
        }

        final long shardRequirementBytes = accountingMode.getShardRequirementBytes(shardCacheRequirement);
        final long newCommitmentBytes = Math.addExact(currentCommitmentBytes, shardRequirementBytes);

        if (newCommitmentBytes > thresholdBytes) {
            if (isDebugEnabled || allocation.debugDecision()) {
                final String message = Strings.format(
                    "allocating shard [%s] to node [%s] would raise its cache commitment from [%d] to [%d] bytes, exceeding the low "
                        + "watermark [%d] bytes (accounting mode [%s])",
                    shardRouting.shardId(),
                    node.getShortNodeDescription(),
                    currentCommitmentBytes,
                    newCommitmentBytes,
                    thresholdBytes,
                    accountingMode
                );
                if (isDebugEnabled) {
                    logCanAllocateMessage.maybeExecute(() -> logger.debug(message));
                }
                return allocation.decision(Decision.NOT_PREFERRED, NAME, message);
            } else {
                return Decision.NOT_PREFERRED;
            }
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "allocating shard [%s] to node [%s] would raise its cache commitment from [%d] to [%d] bytes, which remains below the low "
                + "watermark [%d] bytes (accounting mode [%s])",
            shardRouting.shardId(),
            node.getShortNodeDescription(),
            currentCommitmentBytes,
            newCommitmentBytes,
            thresholdBytes,
            accountingMode
        );
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (enabled == false) {
            return YES_SHARED_CACHE_CAPACITY_DECIDER_DISABLED;
        }

        if (canRemainEnabled == false) {
            return YES_CAN_REMAIN_DISABLED;
        }

        if (node.node().getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE) == false) {
            return YES_SHARED_CACHE_CAPACITY_FOR_SEARCH_NODE_ONLY;
        }

        final Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments = allocation.clusterInfo()
            .getNodeCacheSizeAndCommitments();
        assert nodeCacheSizeAndCommitments != null;
        final NodeCacheSizeAndCommitments nodeCommitments = nodeCacheSizeAndCommitments.get(node.nodeId());
        if (nodeCommitments == null) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "no cache size and commitment data available for node [%s]",
                node.getShortNodeDescription()
            );
        }

        // The shard being evaluated is already allocated to this node, so its cost is already reflected in nodeCommitments. If a different
        // shard move alleviates the pressure, the simulation updates nodeCommitments accordingly before the node is re-evaluated for
        // any remaining shards.

        // Snapshot the dynamic settings once so a concurrent settings update can't mix values from different accounting modes (or
        // watermarks) within a single decision.
        final CacheAccountingMode accountingMode = this.accountingMode;
        final RatioValue highWatermark = this.highWatermark;
        final long currentCommitmentBytes = accountingMode.getCurrentCommitmentBytes(nodeCommitments);
        final long thresholdBytes = (long) (nodeCommitments.cacheSizeInBytes() * highWatermark.getAsRatio());

        if (currentCommitmentBytes > thresholdBytes) {
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String message = Strings.format(
                    "node [%s] cache commitment [%d] bytes exceeds the high watermark [%d] bytes ([%.2f%%], accounting mode [%s])",
                    node.getShortNodeDescription(),
                    currentCommitmentBytes,
                    thresholdBytes,
                    highWatermark.getAsPercent(),
                    accountingMode
                );
                if (logger.isDebugEnabled()) {
                    logCanRemainMessage.maybeExecute(() -> logger.debug(message));
                }
                return allocation.decision(Decision.NOT_PREFERRED, NAME, message);
            } else {
                return Decision.NOT_PREFERRED;
            }
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "node [%s] cache commitment [%d] bytes is below the high watermark [%d] bytes ([%.2f%%], accounting mode [%s])",
            node.getShortNodeDescription(),
            currentCommitmentBytes,
            thresholdBytes,
            highWatermark.getAsPercent(),
            accountingMode
        );
    }
}
