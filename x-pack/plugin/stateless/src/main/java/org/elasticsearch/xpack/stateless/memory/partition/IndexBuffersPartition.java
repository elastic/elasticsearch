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
 * Reserves a fixed slice of heap for Lucene indexing buffers, managed by
 * {@code IndexingMemoryController} which flushes to disk as the buffer fills.
 * Because this is a fixed reservation with no observable workload signal, it
 * does not drive autoscaling.
 */
public class IndexBuffersPartition implements MemoryPartition {

    public static final String NAME = "index_buffers";
    public static final double DEFAULT_FRACTION = 0.15;
    public static final Setting<Double> FRACTION_SETTING = Setting.doubleSetting(
        "memory_metrics.partition.index_buffers.fraction",
        DEFAULT_FRACTION,
        0.001,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double fraction;

    public IndexBuffersPartition(ClusterSettings clusterSettings) {
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
        return OptionalLong.empty();
    }
}
