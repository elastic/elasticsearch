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
    long startTime,
    long endTime,
    long newlyAssignedShards,
    long unassignedShards,
    long totalAllocations,
    long undesiredAllocationsExcludingShuttingDownNodes,
    boolean executedReconciliation
) {

    public static final BalancingRoundStats EMPTY_BALANCING_ROUND_STATS = new BalancingRoundStats(-1, -1, -1, -1, -1, -1, false);

    public static class Builder {
        private long startTime = 0;
        private long endTime = 0;
        private long newlyAssignedShards = 0;
        private long unassignedShards = 0;
        long totalAllocations = 0;
        long undesiredAllocationsExcludingShuttingDownNodes = 0;
        boolean executedReconciliation = false;

        public BalancingRoundStats build() {
            return new BalancingRoundStats(
                startTime,
                endTime,
                newlyAssignedShards,
                unassignedShards,
                totalAllocations,
                undesiredAllocationsExcludingShuttingDownNodes,

                executedReconciliation
            );
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public void incNewlyAssignedShards() {
            ++this.newlyAssignedShards;
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

        public long getStartTime() {
            return this.startTime;
        }

        public long getEndTime() {
            return this.endTime;
        }

        public long getTotalAllocations() {
            return this.totalAllocations;
        }

        public long getUndesiredAllocationsExcludingShuttingDownNodes() {
            return this.undesiredAllocationsExcludingShuttingDownNodes;
        }

        public void setExecutedReconciliation() {
            this.executedReconciliation = executedReconciliation;
        }

    };

};
