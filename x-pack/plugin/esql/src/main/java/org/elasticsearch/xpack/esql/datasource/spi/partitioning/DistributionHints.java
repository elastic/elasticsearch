/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi.partitioning;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.esql.datasource.spi.DataSource;

import java.util.OptionalLong;
import java.util.Set;

/**
 * Hints for partition planning in {@link DataSource#planPartitions}.
 *
 * <h2>Call Sites</h2>
 *
 * <p><b>Created by:</b> Physical planner on the coordinator. The planner gathers cluster state,
 * query settings, and available resources to construct hints.
 *
 * <p><b>Passed to:</b> {@link DataSource#planPartitions} to guide partitioning decisions.
 *
 * <p>Data sources should treat these as guidance, not strict requirements. For example,
 * a data source may create fewer partitions if the data is small, or more if files are numerous.
 *
 * @param targetPartitions Desired number of partitions (derived from available data nodes and query parallelism setting)
 * @param availableNodes Data node IDs that can execute partitions (from cluster state)
 * @param maxPartitionBytes Suggested maximum bytes per partition (from memory settings)
 * @param preferDataLocality Whether to honor soft ({@link NodeAffinity#prefer}) node affinity.
 *     When true, splits with preferred affinity are grouped by node (best-effort).
 *     When false, preferred affinity is ignored. Has no effect on required
 *     ({@link NodeAffinity#require}) affinity, which is always enforced.
 */
public record DistributionHints(
    int targetPartitions,
    Set<String> availableNodes,
    OptionalLong maxPartitionBytes,
    boolean preferDataLocality
) {

    /**
     * Create hints with just target parallelism.
     */
    public DistributionHints(int targetPartitions) {
        this(targetPartitions, Set.of(), OptionalLong.empty(), false);
    }

    /**
     * Create hints with target parallelism and available nodes.
     */
    public DistributionHints(int targetPartitions, Set<String> availableNodes) {
        this(targetPartitions, availableNodes, OptionalLong.empty(), false);
    }

    // =========================================================================
    // BUILDER
    // =========================================================================

    /**
     * Create a builder for distribution hints.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for DistributionHints.
     */
    public static class Builder {
        private int targetPartitions = 1;
        private Set<String> availableNodes = Set.of();
        private OptionalLong maxPartitionBytes = OptionalLong.empty();
        private boolean preferDataLocality = false;

        /**
         * Set the target number of partitions.
         */
        public Builder targetPartitions(int targetPartitions) {
            this.targetPartitions = targetPartitions;
            return this;
        }

        /**
         * Set the available data nodes.
         */
        public Builder availableNodes(Set<String> availableNodes) {
            this.availableNodes = availableNodes;
            return this;
        }

        /**
         * Set the maximum bytes per partition.
         */
        public Builder maxPartitionBytes(long maxPartitionBytes) {
            this.maxPartitionBytes = OptionalLong.of(maxPartitionBytes);
            return this;
        }

        /**
         * Set the maximum bytes per partition.
         */
        public Builder maxPartitionBytes(ByteSizeValue maxPartitionBytes) {
            this.maxPartitionBytes = OptionalLong.of(maxPartitionBytes.getBytes());
            return this;
        }

        /**
         * Set whether to prefer data locality.
         */
        public Builder preferDataLocality(boolean preferDataLocality) {
            this.preferDataLocality = preferDataLocality;
            return this;
        }

        /**
         * Build the hints.
         */
        public DistributionHints build() {
            return new DistributionHints(targetPartitions, availableNodes, maxPartitionBytes, preferDataLocality);
        }
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    /**
     * Get the number of available data nodes, or 1 if not specified.
     */
    public int nodeCount() {
        return availableNodes.isEmpty() ? 1 : availableNodes.size();
    }

    /**
     * Get the max partition bytes or a default.
     */
    public long maxPartitionBytesOr(long defaultValue) {
        return maxPartitionBytes.orElse(defaultValue);
    }

    /**
     * Default hints for coordinator-only execution.
     */
    public static DistributionHints coordinatorOnly() {
        return new DistributionHints(1, Set.of(), OptionalLong.empty(), false);
    }
}
