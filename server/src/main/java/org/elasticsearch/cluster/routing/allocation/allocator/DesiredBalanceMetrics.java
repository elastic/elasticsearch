/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;

public class DesiredBalanceMetrics {

    public static final DesiredBalanceMetrics NOOP = new DesiredBalanceMetrics(MeterRegistry.NOOP);
    public static final String UNASSIGNED_SHARDS_METRIC_NAME = "es.allocator.desired_balance.shards.unassigned.current";
    public static final String TOTAL_SHARDS_METRIC_NAME = "es.allocator.desired_balance.shards.current";
    public static final String UNDESIRED_ALLOCATION_COUNT_METRIC_NAME = "es.allocator.desired_balance.allocations.undesired.current";
    public static final String UNDESIRED_ALLOCATION_RATIO_METRIC_NAME = "es.allocator.desired_balance.allocations.undesired.ratio";

    private volatile boolean nodeIsMaster = false;

    /**
     * Number of unassigned shards during last reconciliation
     */
    private volatile long unassignedShards;
    /**
     * Total number of assigned shards during last reconciliation
     */
    private volatile long totalAllocations;
    /**
     * Number of assigned shards during last reconciliation that are not allocated on desired node and need to be moved
     */
    private volatile long undesiredAllocations;

    public void updateMetrics(long unassignedShards, long totalAllocations, long undesiredAllocations) {
        this.unassignedShards = unassignedShards;
        this.totalAllocations = totalAllocations;
        this.undesiredAllocations = undesiredAllocations;
    }

    public DesiredBalanceMetrics(MeterRegistry meterRegistry) {
        meterRegistry.registerLongsGauge(
            UNASSIGNED_SHARDS_METRIC_NAME,
            "Current number of unassigned shards",
            "{shard}",
            this::getUnassignedShardsMetrics
        );
        meterRegistry.registerLongsGauge(TOTAL_SHARDS_METRIC_NAME, "Total number of shards", "{shard}", this::getTotalAllocationsMetrics);
        meterRegistry.registerLongsGauge(
            UNDESIRED_ALLOCATION_COUNT_METRIC_NAME,
            "Total number of shards allocated on undesired nodes excluding shutting down nodes",
            "{shard}",
            this::getUndesiredAllocationsMetrics
        );
        meterRegistry.registerDoublesGauge(
            UNDESIRED_ALLOCATION_RATIO_METRIC_NAME,
            "Ratio of undesired allocations to shard count excluding shutting down nodes",
            "1",
            this::getUndesiredAllocationsRatioMetrics
        );
    }

    public void setNodeIsMaster(boolean nodeIsMaster) {
        this.nodeIsMaster = nodeIsMaster;
    }

    public long unassignedShards() {
        return unassignedShards;
    }

    public long totalAllocations() {
        return totalAllocations;
    }

    public long undesiredAllocations() {
        return undesiredAllocations;
    }

    private List<LongWithAttributes> getUnassignedShardsMetrics() {
        return getIfPublishing(unassignedShards);
    }

    private List<LongWithAttributes> getTotalAllocationsMetrics() {
        return getIfPublishing(totalAllocations);
    }

    private List<LongWithAttributes> getUndesiredAllocationsMetrics() {
        return getIfPublishing(undesiredAllocations);
    }

    private List<LongWithAttributes> getIfPublishing(long value) {
        if (nodeIsMaster) {
            return List.of(new LongWithAttributes(value));
        }
        return List.of();
    }

    private List<DoubleWithAttributes> getUndesiredAllocationsRatioMetrics() {
        if (nodeIsMaster) {
            var total = totalAllocations;
            var undesired = undesiredAllocations;
            return List.of(new DoubleWithAttributes(total != 0 ? (double) undesired / total : 0.0));
        }
        return List.of();
    }

    public void zeroAllMetrics() {
        unassignedShards = 0;
        totalAllocations = 0;
        undesiredAllocations = 0;
    }
}
