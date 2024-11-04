/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class DesiredBalanceMetrics {

    public record AllocationStats(long unassignedShards, long totalAllocations, long undesiredAllocationsExcludingShuttingDownNodes) {}

    public record NodeWeightStats(long shardCount, double diskUsageInBytes, double writeLoad, double nodeWeight) {}

    public static final DesiredBalanceMetrics NOOP = new DesiredBalanceMetrics(MeterRegistry.NOOP);
    public static final String UNASSIGNED_SHARDS_METRIC_NAME = "es.allocator.desired_balance.shards.unassigned.current";
    public static final String TOTAL_SHARDS_METRIC_NAME = "es.allocator.desired_balance.shards.current";
    public static final String UNDESIRED_ALLOCATION_COUNT_METRIC_NAME = "es.allocator.desired_balance.allocations.undesired.current";
    public static final String UNDESIRED_ALLOCATION_RATIO_METRIC_NAME = "es.allocator.desired_balance.allocations.undesired.ratio";
    public static final String DESIRED_BALANCE_NODE_WEIGHT_METRIC_NAME = "es.allocator.desired_balance.allocations.node_weight.current";
    public static final String DESIRED_BALANCE_NODE_SHARD_COUNT_METRIC_NAME =
        "es.allocator.desired_balance.allocations.node_shard_count.current";
    public static final String DESIRED_BALANCE_NODE_WRITE_LOAD_METRIC_NAME =
        "es.allocator.desired_balance.allocations.node_write_load.current";
    public static final String DESIRED_BALANCE_NODE_DISK_USAGE_METRIC_NAME =
        "es.allocator.desired_balance.allocations.node_disk_usage_bytes.current";
    public static final AllocationStats EMPTY_ALLOCATION_STATS = new AllocationStats(-1, -1, -1);

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

    private final AtomicReference<Map<DiscoveryNode, NodeWeightStats>> weightStatsPerNodeRef = new AtomicReference<>(Map.of());

    public void updateMetrics(AllocationStats allocationStats, Map<DiscoveryNode, NodeWeightStats> weightStatsPerNode) {
        assert allocationStats != null : "allocation stats cannot be null";
        assert weightStatsPerNode != null : "node balance weight stats cannot be null";
        if (allocationStats != EMPTY_ALLOCATION_STATS) {
            this.unassignedShards = allocationStats.unassignedShards;
            this.totalAllocations = allocationStats.totalAllocations;
            this.undesiredAllocations = allocationStats.undesiredAllocationsExcludingShuttingDownNodes;
        }
        weightStatsPerNodeRef.set(weightStatsPerNode);
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
        meterRegistry.registerDoublesGauge(
            DESIRED_BALANCE_NODE_WEIGHT_METRIC_NAME,
            "Weight of nodes in the computed desired balance",
            "unit",
            this::getDesiredBalanceNodeWeightMetrics
        );
        meterRegistry.registerDoublesGauge(
            DESIRED_BALANCE_NODE_WRITE_LOAD_METRIC_NAME,
            "Write load of nodes in the computed desired balance",
            "threads",
            this::getDesiredBalanceNodeWriteLoadMetrics
        );
        meterRegistry.registerDoublesGauge(
            DESIRED_BALANCE_NODE_DISK_USAGE_METRIC_NAME,
            "Disk usage of nodes in the computed desired balance",
            "bytes",
            this::getDesiredBalanceNodeDiskUsageMetrics
        );
        meterRegistry.registerLongsGauge(
            DESIRED_BALANCE_NODE_SHARD_COUNT_METRIC_NAME,
            "Shard count of nodes in the computed desired balance",
            "unit",
            this::getDesiredBalanceNodeShardCountMetrics
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

    private List<DoubleWithAttributes> getDesiredBalanceNodeWeightMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = weightStatsPerNodeRef.get();
        List<DoubleWithAttributes> doubles = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            var stat = stats.get(node);
            doubles.add(new DoubleWithAttributes(stat.nodeWeight(), getNodeAttributes(node)));
        }
        return doubles;
    }

    private List<DoubleWithAttributes> getDesiredBalanceNodeWriteLoadMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = weightStatsPerNodeRef.get();
        List<DoubleWithAttributes> doubles = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            doubles.add(new DoubleWithAttributes(stats.get(node).writeLoad(), getNodeAttributes(node)));
        }
        return doubles;
    }

    private List<DoubleWithAttributes> getDesiredBalanceNodeDiskUsageMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = weightStatsPerNodeRef.get();
        List<DoubleWithAttributes> doubles = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            doubles.add(new DoubleWithAttributes(stats.get(node).diskUsageInBytes(), getNodeAttributes(node)));
        }
        return doubles;
    }

    private List<LongWithAttributes> getDesiredBalanceNodeShardCountMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = weightStatsPerNodeRef.get();
        List<LongWithAttributes> values = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            values.add(new LongWithAttributes(stats.get(node).shardCount(), getNodeAttributes(node)));
        }
        return values;
    }

    private Map<String, Object> getNodeAttributes(DiscoveryNode node) {
        return Map.of("node_id", node.getId(), "node_name", node.getName());
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
        weightStatsPerNodeRef.set(Map.of());
    }
}
