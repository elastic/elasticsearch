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
 * Reserves a fixed slice of heap as headroom for JVM overhead and small untracked workloads.
 * Does not drive autoscaling. May be reduced over time as the other partitions are refined
 * and cover more of the actual heap consumers.
 */
public class HeadroomPartition implements MemoryPartition {

    public static final String NAME = "headroom";
    public static final double DEFAULT_FRACTION = 0.20;
    public static final Setting<Double> FRACTION_SETTING = Setting.doubleSetting(
        "memory_metrics.partition.headroom.fraction",
        DEFAULT_FRACTION,
        0.001,
        1.0,
        Setting.Property.NodeScope
    );

    private final double fraction;

    public HeadroomPartition(ClusterSettings clusterSettings) {
        this.fraction = clusterSettings.get(FRACTION_SETTING);
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
        return OptionalLong.empty();
    }
}
