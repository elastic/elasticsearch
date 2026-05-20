/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory.partition;

import java.util.List;

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
public final class PartitionedMemoryModel {

    private final List<MemoryPartition> partitions;

    public PartitionedMemoryModel(List<MemoryPartition> partitions) {
        this.partitions = List.copyOf(partitions);
    }

    public List<MemoryPartition> partitions() {
        return partitions;
    }

    /** Minimum node heap in bytes: {@code MAX(partition.nodeHeapRequirementBytes())} across all partitions. */
    public long nodeEstimateBytes(PartitionContext ctx) {
        return partitions.stream()
            .map(p -> p.nodeHeapRequirementBytes(ctx))
            .flatMapToLong(o -> o.isPresent() ? java.util.stream.LongStream.of(o.getAsLong()) : java.util.stream.LongStream.empty())
            .max()
            .orElse(0L);
    }

    /** Total tier workload bytes: sum of {@link MemoryPartition#tierEstimateBytes} across all partitions. */
    public long tierEstimateBytes(PartitionContext ctx) {
        return partitions.stream().mapToLong(p -> p.tierEstimateBytes(ctx).orElse(0L)).sum();
    }

    /**
     * Percentage of node heap available for the tier workload, as an integer 0–100.
     * Equal to the sum of {@link MemoryPartition#fraction()} for tier-contributing partitions,
     * multiplied by 100 and rounded.
     */
    public int maxTierPercent() {
        double sum = partitions.stream().filter(MemoryPartition::hasTierEstimate).mapToDouble(MemoryPartition::fraction).sum();
        return (int) Math.round(sum * 100);
    }
}
