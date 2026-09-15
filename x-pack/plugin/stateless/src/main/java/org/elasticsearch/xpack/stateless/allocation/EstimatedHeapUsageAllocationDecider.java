/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.xpack.stateless.EstimatedHeapSettings;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An allocation decider that prevents shard allocation to index nodes where the estimated total JVM heap usage
 * would exceed a configured watermark. Uses {@code totalHeapUsage / totalBytes} as the utilisation metric.
 * <p>
 * The dynamic setting {@code cluster.routing.allocation.estimated_heap.watermark.low} prevents new shard allocation
 * when the node's estimated heap usage exceeds the configured value. The decider as a whole can be disabled by
 * setting {@code cluster.routing.allocation.estimated_heap.threshold_enabled} to {@code false}.
 */
public class EstimatedHeapUsageAllocationDecider extends AbstractEstimatedHeapAllocationDecider {

    private static final String NAME = "estimated_heap";
    private static final String DESCRIPTION = "estimated heap";

    public static final Setting<RatioValue> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK = Setting.ratioSetting(
        "cluster.routing.allocation.estimated_heap.watermark.low",
        RatioValue.ofPercent(95),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<RatioValue> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK = Setting.ratioSetting(
        "cluster.routing.allocation.estimated_heap.watermark.high",
        RatioValue.ofPercent(100),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED = Setting.boolSetting(
        "cluster.routing.allocation.estimated_heap.watermark.high.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final EstimatedHeapSettings estimatedHeapSettings;

    public EstimatedHeapUsageAllocationDecider(EstimatedHeapSettings estimatedHeapSettings, ClusterSettings clusterSettings) {
        super(NAME, DESCRIPTION, Set.of(DiscoveryNodeRole.INDEX_ROLE), clusterSettings);
        this.estimatedHeapSettings = estimatedHeapSettings;
    }

    @Override
    protected boolean isEnabled() {
        return estimatedHeapSettings.enabled();
    }

    @Override
    protected double getLowWatermarkPercent() {
        return estimatedHeapSettings.lowWatermarkPercent();
    }

    @Override
    protected double getHighWatermarkPercent() {
        return estimatedHeapSettings.highWatermarkPercent();
    }

    @Override
    protected boolean isHighWatermarkEnabled() {
        return estimatedHeapSettings.highWatermarkEnabled();
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
     * Returns the configuration for the estimated heap usage monitor.
     */
    public static EstimatedHeapUsageMonitor.Configuration estimatedHeapConfiguration() {
        return new EstimatedHeapUsageMonitor.Configuration(
            "estimated heap",
            InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
            EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK,
            (clusterInfo, clusterState) -> clusterInfo.getNodeHeapMetrics()
                .entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().estimatedUsageAsPercentage()))
        );
    }
}
