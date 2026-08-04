/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.Nullable;

/**
 * An allocation decider that prevents shard allocation to index nodes where the estimated heap used by hosted shards
 * would exceed a configured fraction of the node's hosted-shards heap partition.
 * <p>
 * Unlike {@link EstimatedHeapUsageAllocationDecider}, which measures estimated total heap usage against total JVM heap,
 * this decider measures only the heap attributable to hosted shards against the dedicated hosted-shards partition size
 * reported in {@link org.elasticsearch.cluster.ClusterInfo#getHostedShardsPartitionSizeByNodeId()}.
 * <p>
 * When no partition size is available for a node (e.g. during a rolling upgrade), this decider yields YES and defers
 * to the other deciders.
 */
public class HostedShardsPartitionHeapAllocationDecider extends AbstractEstimatedHeapAllocationDecider {

    private static final String NAME = "hosted_shards_partition_heap";
    private static final String DESCRIPTION = "hosted shards partition heap";

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.hosted_shards_partition_heap.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<RatioValue> LOW_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.hosted_shards_partition_heap.watermark.low",
        "95%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<RatioValue> HIGH_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.hosted_shards_partition_heap.watermark.high",
        "100%",
        RatioValue::parseRatioValue,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> HIGH_WATERMARK_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.hosted_shards_partition_heap.watermark.high.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean enabled;
    private volatile boolean highWatermarkEnabled;
    private volatile RatioValue lowWatermark;
    private volatile RatioValue highWatermark;

    public HostedShardsPartitionHeapAllocationDecider(ClusterSettings clusterSettings) {
        super(NAME, DESCRIPTION, clusterSettings);
        clusterSettings.initializeAndWatch(ENABLED_SETTING, value -> enabled = value);
        clusterSettings.initializeAndWatch(HIGH_WATERMARK_ENABLED_SETTING, value -> highWatermarkEnabled = value);
        clusterSettings.initializeAndWatch(LOW_WATERMARK_SETTING, value -> lowWatermark = value);
        clusterSettings.initializeAndWatch(HIGH_WATERMARK_SETTING, value -> highWatermark = value);
    }

    @Override
    protected boolean isEnabled() {
        return enabled;
    }

    @Override
    protected double getLowWatermarkPercent() {
        return lowWatermark.getAsPercent();
    }

    @Override
    protected double getHighWatermarkPercent() {
        return highWatermark.getAsPercent();
    }

    @Override
    protected boolean isHighWatermarkEnabled() {
        return highWatermarkEnabled;
    }

    /**
     * Returns the partition size for this node, or {@code null} if absent or zero (decider yields YES).
     */
    @Override
    protected @Nullable Long resolveCapacityBytes(NodeHeapMetrics metrics, RoutingNode node, RoutingAllocation allocation) {
        final Long partitionSize = allocation.clusterInfo().getHostedShardsPartitionSizeByNodeId().get(node.nodeId());
        return (partitionSize == null || partitionSize == 0L) ? null : partitionSize;
    }

    @Override
    protected long getCurrentUsageBytes(NodeHeapMetrics metrics) {
        return metrics.nodeHeapEstimates().hostedShardsHeapUsage();
    }

    /**
     * Returns only the shard's own heap cost; index-metadata bytes are excluded because
     * {@code hostedShardsHeapUsage} already excludes index metadata.
     */
    @Override
    protected long getProjectedAdditionalBytes(ShardRouting shard, RoutingNode node, ShardAndIndexHeapUsage usage) {
        return usage.shardHeapUsageBytes();
    }
}
