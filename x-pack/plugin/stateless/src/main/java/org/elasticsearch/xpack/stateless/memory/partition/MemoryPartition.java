/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory.partition;

import java.util.OptionalLong;

/**
 * A named slice of the available JVM heap reserved for a specific workload category.
 * Each partition is allocated a fixed fraction of the heap and may optionally publish
 * the minimum node size implied by its current workload (driving autoscaling) and/or
 * a tier-level memory estimate (for migratable workloads distributed across nodes).
 */
public interface MemoryPartition {

    /** Unique name used in metric names and logging, e.g. {@code "index_metadata"}. */
    String name();

    /** Fraction of total heap reserved for this partition, e.g. {@code 0.10} for 10%. */
    double fraction();

    /**
     * The minimum node heap (in bytes) implied by this partition's current workload.
     * Derived as {@code workload_requirement / fraction()} unless the partition has
     * its own constraint (e.g. the indexing pressure 10% single-document rule).
     *
     * <p>Returns {@link OptionalLong#empty()} for fixed-size partitions that reserve a
     * slice of heap without an observable workload signal, such as index buffers and headroom.
     */
    OptionalLong nodeHeapRequirementBytes(PartitionContext ctx);

    /**
     * The bytes this partition contributes to the tier estimate (the total migratable
     * workload memory distributed across nodes). Non-empty only for partitions whose
     * workload can be spread by adding more nodes, i.e. {@code HostedShardsPartition}.
     */
    default OptionalLong tierEstimateBytes(PartitionContext ctx) {
        return OptionalLong.empty();
    }

    /**
     * Whether this partition contributes to the tier estimate regardless of context.
     * Used by {@link PartitionedMemoryModel#maxTierPercent()} to compute the
     * {@code max_tier_percentage} field in the autoscaler payload.
     */
    default boolean hasTierEstimate() {
        return false;
    }
}
