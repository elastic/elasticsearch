/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory.partition;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.OptionalLong;

/**
 * Reserves heap for the indexing infrastructure of shards hosted on each node.
 *
 * <p>This is the only partition that contributes to the tier estimate (the total migratable
 * workload memory across the cluster). It drives two autoscaling signals:
 * <ul>
 *   <li>The tier estimate ({@link #tierEstimateBytes}) — the sum of all shard hosting costs
 *       in the cluster, which grows horizontally as more shards are added.</li>
 *   <li>A node estimate floor ({@link #nodeHeapRequirementBytes}) — derived from the largest
 *       single shard cost, ensuring every node can host at least one shard of that size.
 *       This prevents the autoscaler from choosing nodes too small to host the largest shard.</li>
 * </ul>
 */
public class HostedShardsPartition implements MemoryPartition {

    public static final String NAME = "hosted_shards";
    public static final double DEFAULT_FRACTION = 0.30;
    public static final Setting<Double> FRACTION_SETTING = Setting.doubleSetting(
        "memory_metrics.partition.hosted_shards.fraction",
        DEFAULT_FRACTION,
        0.001,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double fraction;

    public HostedShardsPartition(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(FRACTION_SETTING, v -> this.fraction = v);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public double fraction() {
        return fraction;
    }

    @Override
    public OptionalLong nodeHeapRequirementBytes(PartitionContext ctx) {
        long largestShard = ctx.largestShardCostBytes();
        if (largestShard == 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of((long) (largestShard / fraction));
    }

    @Override
    public OptionalLong tierEstimateBytes(PartitionContext ctx) {
        return OptionalLong.of(ctx.totalShardCostBytes());
    }

    @Override
    public boolean hasTierEstimate() {
        return true;
    }
}
