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
 * Reserves heap for the working memory used to process ingestion requests, aligned with
 * the existing {@code IndexingPressure} limits.
 *
 * <p>The autoscaling signal is {@code minimumRequiredHeapForIndexingOpsBytes} from
 * {@link org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService}, which is
 * already expressed as a heap requirement derived from the IndexingPressure 10%
 * single-document constraint ({@code largest_doc / 0.10}). This value is used directly
 * as the node heap requirement — not divided by this partition's own fraction — because
 * the 10% constraint is the operative limit, not the 15% reservation.
 */
public class IndexingPressurePartition implements MemoryPartition {

    public static final String NAME = "indexing_pressure";
    public static final double DEFAULT_FRACTION = 0.15;
    public static final Setting<Double> FRACTION_SETTING = Setting.doubleSetting(
        "memory_metrics.partition.indexing_pressure.fraction",
        DEFAULT_FRACTION,
        0.001,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double fraction;

    public IndexingPressurePartition(ClusterSettings clusterSettings) {
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
        long requirement = ctx.minimumRequiredHeapForIndexingOpsBytes();
        if (requirement == 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(requirement);
    }
}
