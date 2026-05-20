/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory.partition;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService;

import java.util.OptionalLong;

/**
 * Reserves heap for index metadata held in cluster state on every node (~350 KB per index).
 * Index creation is not back-pressured, so this partition drives autoscaling by publishing
 * {@code totalIndices * INDEX_MEMORY_OVERHEAD / fraction} as its implied node heap requirement.
 */
public class IndexMetadataPartition implements MemoryPartition {

    public static final String NAME = "index_metadata";
    public static final double DEFAULT_FRACTION = 0.10;
    public static final Setting<Double> FRACTION_SETTING = Setting.doubleSetting(
        "memory_metrics.partition.index_metadata.fraction",
        DEFAULT_FRACTION,
        0.001,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double fraction;

    public IndexMetadataPartition(ClusterSettings clusterSettings) {
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
        if (ctx.totalIndices() == 0) {
            return OptionalLong.empty();
        }
        long workload = (long) ctx.totalIndices() * StatelessMemoryMetricsService.INDEX_MEMORY_OVERHEAD;
        return OptionalLong.of((long) (workload / fraction));
    }
}
