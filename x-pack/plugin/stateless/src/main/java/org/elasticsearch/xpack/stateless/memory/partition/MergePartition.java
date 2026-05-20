/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory.partition;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.RatioValue;

import java.util.OptionalLong;

/**
 * Reserves heap for concurrent Lucene segment merges. Merges that would exceed the
 * partition are delayed. The autoscaling signal is the largest active or queued merge
 * size, published by nodes via
 * {@code TransportPublishMergeMemoryEstimate} and tracked in
 * {@code StatelessMemoryMetricsService}.
 */
public class MergePartition implements MemoryPartition {

    public static final String NAME = "merge";
    public static final Setting<RatioValue> FRACTION_SETTING = new Setting<>(
        "memory_metrics.partition.merge.fraction",
        "10%",
        RatioValue::parseRatioValue,
        Setting.Property.NodeScope
    );

    private final double fraction;

    public MergePartition(ClusterSettings clusterSettings) {
        this.fraction = clusterSettings.get(FRACTION_SETTING).getAsRatio();
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
        long largestMerge = ctx.largestMergeEstimateBytes();
        if (largestMerge == 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of((long) (largestMerge / fraction));
    }
}
