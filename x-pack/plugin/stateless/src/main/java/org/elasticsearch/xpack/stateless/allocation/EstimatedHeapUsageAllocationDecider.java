/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.InternalClusterInfoService;
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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * The allocation decider is similar to {@link org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider}
 * but for the estimated heap usage. The dynamic setting <code>cluster.routing.allocation.estimated_heap.watermark.low</code>
 * prevents shard allocation to a node if the estimated heap usage exceeds the configured value. The decider as a whole can be disabled by
 * setting <code>cluster.routing.allocation.estimated_heap.threshold_enabled</code> to <code>false</code>.
 */
public class EstimatedHeapUsageAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(EstimatedHeapUsageAllocationDecider.class);
    private static final String NAME = "estimated_heap";

    /**
     * Below the specified heap size the decider will not intervene
     */
    public static final Setting<ByteSizeValue> MINIMUM_HEAP_SIZE_FOR_ENABLEMENT = Setting.byteSizeSetting(
        "cluster.routing.allocation.estimated_heap.minimum_heap_size_for_enablement",
        ByteSizeValue.ofGb(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<RatioValue> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK = new Setting<>(
        "cluster.routing.allocation.estimated_heap.watermark.low",
        "100%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<RatioValue> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK = new Setting<>(
        "cluster.routing.allocation.estimated_heap.watermark.high",
        "100%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED = Setting.boolSetting(
        "cluster.routing.allocation.estimated_heap.watermark.high.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MINIMUM_LOGGING_INTERVAL = Setting.timeSetting(
        "cluster.routing.allocation.estimated_heap.log_interval",
        TimeValue.timeValueMinutes(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Decision YES_ESTIMATED_HEAP_USAGE_DECIDER_DISABLED = Decision.single(
        Decision.Type.YES,
        NAME,
        "estimated heap allocation decider is disabled"
    );

    private static final Decision YES_ESTIMATED_HEAP_USAGE_FOR_INDEX_NODE_ONLY = Decision.single(
        Decision.Type.YES,
        NAME,
        "estimated heap allocation decider is applicable only to index nodes"
    );

    private final FrequencyCappedAction logCanRemainMessage;
    private final FrequencyCappedAction logCanAllocateMessage;
    private volatile boolean enabled;
    private volatile boolean highWatermarkEnabled;
    private volatile RatioValue estimatedHeapLowWatermark;
    private volatile RatioValue estimatedHeapHighWatermark;
    private volatile ByteSizeValue minimumHeapSizeForEnabled;

    public EstimatedHeapUsageAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
            this::setEnabled
        );
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
            value -> highWatermarkEnabled = value
        );
        logCanRemainMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        logCanAllocateMessage = new FrequencyCappedAction(System::currentTimeMillis, TimeValue.ZERO);
        clusterSettings.initializeAndWatch(MINIMUM_LOGGING_INTERVAL, timeValue -> {
            logCanRemainMessage.setMinInterval(timeValue);
            logCanAllocateMessage.setMinInterval(timeValue);
        });
        clusterSettings.initializeAndWatch(CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK, this::setEstimatedHeapLowWatermark);
        clusterSettings.initializeAndWatch(CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK, this::setEstimatedHeapHighWatermark);
        clusterSettings.initializeAndWatch(
            MINIMUM_HEAP_SIZE_FOR_ENABLEMENT,
            byteSizeValue -> this.minimumHeapSizeForEnabled = byteSizeValue
        );
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setEstimatedHeapLowWatermark(RatioValue estimatedHeapLowWatermark) {
        this.estimatedHeapLowWatermark = estimatedHeapLowWatermark;
    }

    private void setEstimatedHeapHighWatermark(RatioValue estimatedHeapHighWatermark) {
        this.estimatedHeapHighWatermark = estimatedHeapHighWatermark;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        var heapUsageDisabledForNodeDecision = heapUsageDisabledForNode(node, allocation);
        if (heapUsageDisabledForNodeDecision != null) {
            return heapUsageDisabledForNodeDecision;
        }
        final var nodeHeapUsageForNode = allocation.clusterInfo().getEstimatedHeapUsages().get(node.nodeId());
        assert nodeHeapUsageForNode != null : "expected to have a valid heap usage estimate for the node";

        final double heapUsedPercentageForNode = nodeHeapUsageForNode.estimatedUsageAsPercentage();
        final double lowWaterMarkAsPercentage = estimatedHeapLowWatermark.getAsPercent();
        if (heapUsedPercentageForNode > lowWaterMarkAsPercentage) {
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String message = Strings.format(
                    "insufficient estimated heap available on node [%s]: heap usage percentage [%.2f] exceeds low watermark [%.2f]",
                    node.nodeId(),
                    heapUsedPercentageForNode,
                    lowWaterMarkAsPercentage
                );

                if (logger.isDebugEnabled()) {
                    logCanAllocateMessage.maybeExecute(() -> logger.debug(message));
                }

                return allocation.decision(Decision.NO, NAME, message);
            } else {
                return Decision.NO;
            }
        }

        final var shardHeapUsages = allocation.clusterInfo().getEstimatedShardHeapUsages();
        final var shardAndIndexHeapUsage = shardHeapUsages.get(shardRouting.shardId());
        if (shardAndIndexHeapUsage == null) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "sufficient estimated heap available on node [%s]: heap usage percentage [%.2f] is below low watermark [%.2f]",
                node.nodeId(),
                heapUsedPercentageForNode,
                lowWaterMarkAsPercentage
            );
        }

        final long additionalHeapUsageBytes = shardAndIndexHeapUsage.shardHeapUsageBytes() + (node.hasIndex(shardRouting.index())
            ? 0
            : shardAndIndexHeapUsage.indexHeapUsageBytes());

        final var newNodeHeapUsageForNode = nodeHeapUsageForNode.updateEstimatedUsage(additionalHeapUsageBytes);
        if (newNodeHeapUsageForNode.estimatedUsageAsPercentage() > lowWaterMarkAsPercentage) {
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String message = Strings.format(
                    "insufficient estimated heap available on node [%s]: shard [%s] would add [%d] bytes, increasing the node used heap "
                        + "percentage from [%.2f] to [%.2f], which exceeds low watermark [%.2f]",
                    node.nodeId(),
                    shardRouting.shardId(),
                    additionalHeapUsageBytes,
                    heapUsedPercentageForNode,
                    newNodeHeapUsageForNode.estimatedUsageAsPercentage(),
                    lowWaterMarkAsPercentage
                );

                if (logger.isDebugEnabled()) {
                    logCanAllocateMessage.maybeExecute(() -> logger.debug(message));
                }

                return allocation.decision(Decision.NO, NAME, message);
            } else {
                return Decision.NO;
            }
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "sufficient estimated heap available on node [%s]: updated heap usage percentage [%.2f] is below low watermark [%.2f]",
            node.nodeId(),
            newNodeHeapUsageForNode.estimatedUsageAsPercentage(),
            lowWaterMarkAsPercentage
        );
    }

    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        var heapUsageDisabledForNodeDecision = heapUsageDisabledForNode(node, allocation);
        if (heapUsageDisabledForNodeDecision != null) {
            return heapUsageDisabledForNodeDecision;
        }

        if (highWatermarkEnabled == false) {
            return allocation.decision(Decision.YES, NAME, "heap decider can remain disabled");
        }

        final var nodeHeapUsageForNode = allocation.clusterInfo().getEstimatedHeapUsages().get(node.nodeId());
        assert nodeHeapUsageForNode != null : "expected to have a valid heap usage estimate for the node";

        final double heapUsedPercentage = nodeHeapUsageForNode.estimatedUsageAsPercentage();
        final double highWaterMarkPercentage = estimatedHeapHighWatermark.getAsPercent();
        if (heapUsedPercentage > highWaterMarkPercentage) {
            if (logger.isDebugEnabled() || allocation.debugDecision()) {
                final String message = Strings.format(
                    "insufficient estimated heap available on node [%s]: used percentage [%.2f] exceeds high watermark [%.2f]",
                    node.nodeId(),
                    heapUsedPercentage,
                    highWaterMarkPercentage
                );

                if (logger.isDebugEnabled()) {
                    logCanRemainMessage.maybeExecute(() -> logger.debug(message));
                }

                return allocation.decision(Decision.NO, NAME, message);
            } else {
                return Decision.NO;
            }
        }

        return allocation.decision(
            Decision.YES,
            NAME,
            "sufficient estimated heap available on node [%s]: used percentage [%.2f] is below high watermark [%.2f]",
            node.nodeId(),
            heapUsedPercentage,
            highWaterMarkPercentage
        );
    }

    /**
     * Checks whether
     * <ul>
     *   <li>the heap decider is enabled</li>
     *   <li>the node type supports heap usage checks</li>
     *   <li>the heap usage for the node is available</li>
     *   <li>the node's heap is larger than {@link #MINIMUM_HEAP_SIZE_FOR_ENABLEMENT}</li>
     * </ul>
     * @return a Decision if one of the above failed and the caller should return early, otherwise null (meaning the decider is enabled).
     */
    private @Nullable Decision heapUsageDisabledForNode(RoutingNode node, RoutingAllocation allocation) {
        if (enabled == false) {
            return YES_ESTIMATED_HEAP_USAGE_DECIDER_DISABLED;
        }

        if (node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
            return YES_ESTIMATED_HEAP_USAGE_FOR_INDEX_NODE_ONLY;
        }

        final EstimatedHeapUsage estimatedHeapUsage = allocation.clusterInfo().getEstimatedHeapUsages().get(node.nodeId());
        if (estimatedHeapUsage == null) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "no estimated heap estimation available for node [%s], either a new or restarted node",
                node.nodeId()
            );
        }

        if (estimatedHeapUsage.totalBytes() < minimumHeapSizeForEnabled.getBytes()) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "estimated heap decider will not intervene if heap size is below [%s]",
                minimumHeapSizeForEnabled
            );
        }

        return null;
    }
}
