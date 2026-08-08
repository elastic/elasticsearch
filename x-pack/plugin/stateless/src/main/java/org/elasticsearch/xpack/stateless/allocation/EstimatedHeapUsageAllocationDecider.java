/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;

/**
 * An allocation decider that prevents shard allocation to index nodes where the estimated total JVM heap usage
 * would exceed a configured watermark. Uses {@code totalHeapUsage / totalBytes} as the utilisation metric.
 * <p>
 * The dynamic setting {@code cluster.routing.allocation.estimated_heap.watermark.low} prevents new shard allocation
 * when the node's estimated heap usage exceeds the configured value. The decider as a whole can be disabled by
 * setting {@code cluster.routing.allocation.estimated_heap.threshold_enabled} to {@code false}.
 *
 */
public class EstimatedHeapUsageAllocationDecider extends AbstractEstimatedHeapAllocationDecider {

    private static final String NAME = "estimated_heap";
    private static final String DESCRIPTION = "estimated heap";

    public static final Setting<RatioValue> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK = new Setting<>(
        "cluster.routing.allocation.estimated_heap.watermark.low",
        "95%",
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

    private volatile boolean enabled;
    private volatile boolean highWatermarkEnabled;
    private volatile RatioValue estimatedHeapLowWatermark;
    private volatile RatioValue estimatedHeapHighWatermark;

    public EstimatedHeapUsageAllocationDecider(ClusterSettings clusterSettings) {
        super(NAME, DESCRIPTION, clusterSettings);
        clusterSettings.initializeAndWatch(
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
            value -> enabled = value
        );
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
            value -> highWatermarkEnabled = value
        );
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
            value -> estimatedHeapLowWatermark = value
        );
        clusterSettings.initializeAndWatch(
            CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK,
            value -> estimatedHeapHighWatermark = value
        );
    }

    @Override
    protected boolean isEnabled() {
        return enabled;
    }

    @Override
    protected double getLowWatermarkPercent() {
        return estimatedHeapLowWatermark.getAsPercent();
    }

    @Override
    protected double getHighWatermarkPercent() {
        return estimatedHeapHighWatermark.getAsPercent();
    }

    @Override
    protected boolean isHighWatermarkEnabled() {
        return highWatermarkEnabled;
    }

    @Override
    protected Long resolveCapacityBytes(NodeHeapMetrics metrics, RoutingNode node, RoutingAllocation allocation) {
        return metrics.totalBytes();
    }

    @Override
    protected long getCurrentUsageBytes(NodeHeapMetrics metrics) {
        return metrics.nodeHeapEstimates().totalHeapUsage();
    }

    /**
     * Returns the bytes that allocating this shard would add to total heap on {@code node}.
     * Index-metadata bytes are included only when the node does not yet host the index (to avoid double-counting).
     */
    @Override
    protected long getProjectedAdditionalBytes(ShardRouting shard, RoutingNode node, ShardAndIndexHeapUsage usage) {
        return (node.hasIndex(shard.index()) ? 0L : usage.indexHeapUsageBytes()) + usage.shardHeapUsageBytes();
    }
}
