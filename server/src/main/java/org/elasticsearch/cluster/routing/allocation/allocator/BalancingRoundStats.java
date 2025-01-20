/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

/**
 * Data structure to pass allocation statistics between the desired balance classes.
 */
public record BalancingRoundStats(
    long unassignedShards,
    long totalAllocations,
    long undesiredAllocationsExcludingShuttingDownNodes,
    boolean rebalancing
) {

    /**
     * Tracks in-progress balancing round statistics.
     */
    public static class Builder {
        long unassignedShards = 0;
        long totalAllocations = 0;
        long undesiredAllocationsExcludingShuttingDownNodes = 0;
        boolean rebalancing = false;

        public BalancingRoundStats build() {
            return new BalancingRoundStats(unassignedShards, totalAllocations, undesiredAllocationsExcludingShuttingDownNodes, rebalancing);
        }

        public void setUnassignedShards(long numUnassignedShards) {
            this.unassignedShards = numUnassignedShards;
        }

        public void incTotalAllocations() {
            ++this.totalAllocations;
        }

        public void incUndesiredAllocationsExcludingShuttingDownNodes() {
            ++this.undesiredAllocationsExcludingShuttingDownNodes;
        }

        public long getTotalAllocations() {
            return this.totalAllocations;
        }

        public long getUndesiredAllocationsExcludingShuttingDownNodes() {
            return this.undesiredAllocationsExcludingShuttingDownNodes;
        }

        public void setNoRebalancing() {
            this.rebalancing = false;
        }

    }
};
