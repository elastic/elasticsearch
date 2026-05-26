/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory.partition;

import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;

/**
 * Orchestrates a set of {@link MemoryPartition} instances to produce the three values needed
 * for the partitioned autoscaler payload:
 * <ul>
 *   <li>{@link #nodeEstimateBytes} – the minimum node heap implied by the current workload,
 *       computed as the MAX across all partitions' {@link MemoryPartition#nodeHeapRequirementBytes}.</li>
 *   <li>{@link #tierEstimateBytes} – total migratable workload memory across the cluster.</li>
 *   <li>{@link #maxTierPercent} – the percentage of node heap available for the tier workload,
 *       equal to the sum of fractions of tier-contributing partitions.</li>
 * </ul>
 */
public final class PartitionedMemoryModel<C> {

    private static final String METRIC_PREFIX = "es.stateless.memory.partition.";
    private static final int NO_VALUE = -1;

    private final MemoryPartition<C>[] partitions;
    private final AtomicLongArray lastEstimatedHeapValues;
    private final AtomicLong lastNodeHeapRequirementBytes = new AtomicLong(NO_VALUE);
    private final AtomicLong lastTierHeapRequirementBytes = new AtomicLong(NO_VALUE);
    private final Supplier<C> contextSupplier;

    @SafeVarargs
    public PartitionedMemoryModel(
        String tierIdentifier,
        MeterRegistry meterRegistry,
        Supplier<C> contextSupplier,
        MemoryPartition<C>... partitions
    ) {
        validatePartitionDefinitions(partitions);
        this.partitions = Arrays.copyOf(partitions, partitions.length);
        this.lastEstimatedHeapValues = new AtomicLongArray(partitions.length);
        this.contextSupplier = contextSupplier;
        initializeAndRegisterHeapRequirementMetrics(meterRegistry, tierIdentifier, this.partitions, lastEstimatedHeapValues);
        registerConsumingBytesGauge(
            METRIC_PREFIX + tierIdentifier + ".node_estimate.current",
            "The node estimate for the " + tierIdentifier + " tier, as calculated by the partitioned memory model",
            meterRegistry,
            lastNodeHeapRequirementBytes
        );
        registerConsumingBytesGauge(
            METRIC_PREFIX + tierIdentifier + ".tier_estimate.current",
            "The tier estimate for the " + tierIdentifier + " tier, as calculated by the partitioned memory model",
            meterRegistry,
            lastTierHeapRequirementBytes
        );
    }

    private static void registerConsumingBytesGauge(
        String metricName,
        String metricDescription,
        MeterRegistry meterRegistry,
        AtomicLong lastNodeHeapRequirementBytes
    ) {
        meterRegistry.registerLongsGauge(metricName, metricDescription, "bytes", () -> {
            long lastValue = lastNodeHeapRequirementBytes.getAndSet(NO_VALUE);
            return lastValue == NO_VALUE ? List.of() : List.of(new LongWithAttributes(lastValue));
        });
    }

    private static <C> void initializeAndRegisterHeapRequirementMetrics(
        MeterRegistry meterRegistry,
        String tierIdentifier,
        MemoryPartition<C>[] partitions,
        AtomicLongArray lastEstimatedHeapValues
    ) {
        for (int i = 0; i < partitions.length; i++) {
            lastEstimatedHeapValues.set(i, NO_VALUE);
            final int partitionIndex = i;
            meterRegistry.registerLongsGauge(
                METRIC_PREFIX + tierIdentifier + "." + partitions[i].name() + ".required_heap.current",
                "The amount of heap required by the " + tierIdentifier + "/" + partitions[i] + " partition",
                "bytes",
                () -> {
                    final long lastValue = lastEstimatedHeapValues.getAndSet(partitionIndex, NO_VALUE);
                    return lastValue == NO_VALUE ? List.of() : List.of(new LongWithAttributes(lastValue));
                }
            );
        }
    }

    @SafeVarargs
    private static <C> void validatePartitionDefinitions(MemoryPartition<C>... partitions) {
        double sum = Arrays.stream(partitions).mapToDouble(MemoryPartition::fraction).sum();
        if (Math.abs(sum - 1.0) > 1e-9) {
            throw new IllegalArgumentException("Memory partition fractions must sum to 1.0 but sum to " + sum);
        }
    }

    public AutoscalerEstimate calculateAutoscalerEstimate() {
        C context = contextSupplier.get();
        final long nodeHeapRequirementBytes = nodeEstimateBytes(context);
        final long tierEstimateBytes = tierEstimateBytes(context);
        final int maxTierPercent = maxTierPercent();
        return new AutoscalerEstimate(nodeHeapRequirementBytes, tierEstimateBytes, maxTierPercent);
    }

    /** Minimum node heap in bytes: {@code MAX(partition.nodeHeapRequirementBytes())} across all partitions. */
    public long nodeEstimateBytes(C ctx) {
        long largestHeapRequirement = 0;
        for (int i = 0; i < partitions.length; i++) {
            final var optionalHeapRequirement = partitions[i].nodeHeapRequirementBytes(ctx);
            if (optionalHeapRequirement.isPresent()) {
                final long heapRequirement = optionalHeapRequirement.getAsLong();
                lastEstimatedHeapValues.set(i, heapRequirement);
                largestHeapRequirement = Math.max(optionalHeapRequirement.getAsLong(), largestHeapRequirement);
            }
        }
        lastNodeHeapRequirementBytes.set(largestHeapRequirement);
        return largestHeapRequirement;
    }

    /** Total tier workload bytes: sum of {@link MemoryPartition#tierEstimateBytes} across all partitions. */
    public long tierEstimateBytes(C ctx) {
        final long totalTierEstimate = Arrays.stream(partitions).mapToLong(p -> p.tierEstimateBytes(ctx).orElse(0L)).sum();
        lastTierHeapRequirementBytes.set(totalTierEstimate);
        return totalTierEstimate;
    }

    /**
     * Percentage of node heap available for the tier workload, as an integer 0–100.
     * Equal to the sum of {@link MemoryPartition#fraction()} for tier-contributing partitions,
     * multiplied by 100 and rounded.
     */
    public int maxTierPercent() {
        double sum = Arrays.stream(partitions).filter(MemoryPartition::hasTierEstimate).mapToDouble(MemoryPartition::fraction).sum();
        return (int) Math.round(sum * 100);
    }

    public record AutoscalerEstimate(long nodeHeapRequirementBytes, long tierEstimateBytes, int maxTierPercent) {}
}
