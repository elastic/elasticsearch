/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.memory.partition.MemoryPartition;
import org.elasticsearch.xpack.stateless.memory.partition.PartitionContext;
import org.elasticsearch.xpack.stateless.memory.partition.PartitionedMemoryModel;

import java.util.ArrayList;
import java.util.List;

/**
 * APM gauges for the indexing-tier partitioned memory model.
 * Registers one {@code node_heap_requirement} gauge per partition (the minimum node heap
 * implied by that partition's workload, or 0 for fixed-size partitions) plus aggregate
 * {@code node_estimate} and {@code tier_estimate} gauges.
 */
public record IndexingTierPartitionMetrics(List<LongGauge> partitionGauges, LongGauge nodeEstimateGauge, LongGauge tierEstimateGauge) {

    static final String METRIC_PREFIX = "es.stateless.memory.partition.";
    static final String NODE_ESTIMATE_METRIC = "es.stateless.memory.node_estimate.bytes";
    static final String TIER_ESTIMATE_METRIC = "es.stateless.memory.tier_estimate.bytes";

    public static IndexingTierPartitionMetrics create(
        MeterRegistry meterRegistry,
        PartitionedMemoryModel model,
        StatelessMemoryMetricsService memoryMetricsService
    ) {
        List<LongGauge> gauges = new ArrayList<>(model.partitions().size());
        for (MemoryPartition partition : model.partitions()) {
            gauges.add(
                meterRegistry.registerLongGauge(
                    METRIC_PREFIX + partition.name() + ".node_heap_requirement.bytes",
                    "Minimum node heap implied by the " + partition.name() + " partition",
                    "bytes",
                    () -> {
                        PartitionContext ctx = memoryMetricsService.buildPartitionContext();
                        return new LongWithAttributes(partition.nodeHeapRequirementBytes(ctx).orElse(0L));
                    }
                )
            );
        }
        var nodeEstimateGauge = meterRegistry.registerLongGauge(
            NODE_ESTIMATE_METRIC,
            "Partitioned memory model node heap estimate: MAX across all partition requirements",
            "bytes",
            () -> new LongWithAttributes(model.nodeEstimateBytes(memoryMetricsService.buildPartitionContext()))
        );
        var tierEstimateGauge = meterRegistry.registerLongGauge(
            TIER_ESTIMATE_METRIC,
            "Partitioned memory model tier estimate: total hosted-shard memory across the cluster",
            "bytes",
            () -> new LongWithAttributes(model.tierEstimateBytes(memoryMetricsService.buildPartitionContext()))
        );
        return new IndexingTierPartitionMetrics(List.copyOf(gauges), nodeEstimateGauge, tierEstimateGauge);
    }
}
