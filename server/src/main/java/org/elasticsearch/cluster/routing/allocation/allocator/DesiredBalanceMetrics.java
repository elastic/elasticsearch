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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStatsAndWeightsCalculator.NodeAllocationStatsAndWeight;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Maintains balancer metrics and makes them accessible to the {@link MeterRegistry} and APM reporting. Metrics are updated
 * ({@link #updateMetrics}) or cleared ({@link #zeroAllMetrics}) as a result of cluster events and the metrics will be pulled for reporting
 * via the MeterRegistry implementation. Only the master node reports metrics: see {@link #setNodeIsMaster}. When
 * {@link #nodeIsMaster} is false, empty values are returned such that MeterRegistry ignores the metrics for reporting purposes.
 */
public class DesiredBalanceMetrics {

    /**
     * @param unassignedShards Shards that are not assigned to any node.
     * @param allocationStatsByRole A breakdown of the allocations stats by {@link ShardRouting.Role}
     */
    public record AllocationStats(long unassignedShards, Map<ShardRouting.Role, RoleAllocationStats> allocationStatsByRole) {

        public AllocationStats(long unassignedShards, long totalAllocations, long undesiredAllocationsExcludingShuttingDownNodes) {
            this(
                unassignedShards,
                Map.of(ShardRouting.Role.DEFAULT, new RoleAllocationStats(totalAllocations, undesiredAllocationsExcludingShuttingDownNodes))
            );
        }

        public long totalAllocations() {
            return allocationStatsByRole.values().stream().mapToLong(RoleAllocationStats::totalAllocations).sum();
        }

        public long undesiredAllocationsExcludingShuttingDownNodes() {
            return allocationStatsByRole.values()
                .stream()
                .mapToLong(RoleAllocationStats::undesiredAllocationsExcludingShuttingDownNodes)
                .sum();
        }

        /**
         * Return the ratio of undesired allocations to the total number of allocations.
         *
         * @return a value in [0.0, 1.0]
         */
        public double undesiredAllocationsRatio() {
            final long totalAllocations = totalAllocations();
            if (totalAllocations == 0) {
                return 0;
            }
            return undesiredAllocationsExcludingShuttingDownNodes() / (double) totalAllocations;
        }
    }

    /**
     * @param totalAllocations Shards that are assigned to a node.
     * @param undesiredAllocationsExcludingShuttingDownNodes Shards that are assigned to a node but must move to alleviate a resource
     *                                                       constraint per the {@link AllocationDeciders}. Excludes shards that must move
     *                                                       because of a node shutting down.
     */
    public record RoleAllocationStats(long totalAllocations, long undesiredAllocationsExcludingShuttingDownNodes) {
        public static final RoleAllocationStats EMPTY = new RoleAllocationStats(0L, 0L);

        /**
         * Return the ratio of undesired allocations to the total number of allocations.
         *
         * @return a value in [0.0, 1.0]
         */
        public double undesiredAllocationsRatio() {
            if (totalAllocations == 0) {
                return 0.0;
            }
            return undesiredAllocationsExcludingShuttingDownNodes / (double) totalAllocations;
        }
    }

    public record NodeWeightStats(long shardCount, double diskUsageInBytes, double writeLoad, double nodeWeight) {
        public static final NodeWeightStats ZERO = new NodeWeightStats(0, 0, 0, 0);
    }

    // Reconciliation metrics.
    /** See {@link #unassignedShards} */
    public static final String UNASSIGNED_SHARDS_METRIC_NAME = "es.allocator.desired_balance.shards.unassigned.current";
    /** See {@link #totalAllocations} */
    public static final String TOTAL_SHARDS_METRIC_NAME = "es.allocator.desired_balance.shards.current";
    /** See {@link #undesiredAllocations} */
    public static final String UNDESIRED_ALLOCATION_COUNT_METRIC_NAME = "es.allocator.desired_balance.allocations.undesired.current";
    /** {@link #UNDESIRED_ALLOCATION_COUNT_METRIC_NAME} / {@link #TOTAL_SHARDS_METRIC_NAME} */
    public static final String UNDESIRED_ALLOCATION_RATIO_METRIC_NAME = "es.allocator.desired_balance.allocations.undesired.ratio";

    // Desired balance node metrics.
    public static final String DESIRED_BALANCE_NODE_WEIGHT_METRIC_NAME = "es.allocator.desired_balance.allocations.node_weight.current";
    public static final String DESIRED_BALANCE_NODE_SHARD_COUNT_METRIC_NAME =
        "es.allocator.desired_balance.allocations.node_shard_count.current";
    public static final String DESIRED_BALANCE_NODE_WRITE_LOAD_METRIC_NAME =
        "es.allocator.desired_balance.allocations.node_write_load.current";
    public static final String DESIRED_BALANCE_NODE_DISK_USAGE_METRIC_NAME =
        "es.allocator.desired_balance.allocations.node_disk_usage_bytes.current";

    // Node weight metrics.
    public static final String CURRENT_NODE_WEIGHT_METRIC_NAME = "es.allocator.allocations.node.weight.current";
    public static final String CURRENT_NODE_SHARD_COUNT_METRIC_NAME = "es.allocator.allocations.node.shard_count.current";
    public static final String CURRENT_NODE_WRITE_LOAD_METRIC_NAME = "es.allocator.allocations.node.write_load.current";
    public static final String CURRENT_NODE_DISK_USAGE_METRIC_NAME = "es.allocator.allocations.node.disk_usage_bytes.current";
    public static final String CURRENT_NODE_UNDESIRED_SHARD_COUNT_METRIC_NAME =
        "es.allocator.allocations.node.undesired_shard_count.current";
    public static final String CURRENT_NODE_FORECASTED_DISK_USAGE_METRIC_NAME =
        "es.allocator.allocations.node.forecasted_disk_usage_bytes.current";

    // Decider metrics
    public static final String WRITE_LOAD_DECIDER_MAX_LATENCY_VALUE = "es.allocator.deciders.write_load.max_latency_value.current";

    public static final AllocationStats EMPTY_ALLOCATION_STATS = new AllocationStats(0, Map.of());
    public static final DesiredBalanceMetrics NOOP = new DesiredBalanceMetrics(MeterRegistry.NOOP);

    private final MeterRegistry meterRegistry;
    private volatile boolean nodeIsMaster = false;

    /**
     * The stats from the most recent reconciliation
     */
    private volatile AllocationStats lastReconciliationAllocationStats = EMPTY_ALLOCATION_STATS;

    private final AtomicReference<Map<DiscoveryNode, NodeWeightStats>> weightStatsPerNodeRef = new AtomicReference<>(Map.of());
    private final AtomicReference<Map<DiscoveryNode, NodeAllocationStatsAndWeight>> allocationStatsPerNodeRef = new AtomicReference<>(
        Map.of()
    );

    public void updateMetrics(
        AllocationStats allocationStats,
        Map<DiscoveryNode, NodeWeightStats> weightStatsPerNode,
        Map<DiscoveryNode, NodeAllocationStatsAndWeight> nodeAllocationStats
    ) {
        assert allocationStats != null : "allocation stats cannot be null";
        assert weightStatsPerNode != null : "node balance weight stats cannot be null";
        if (allocationStats != EMPTY_ALLOCATION_STATS) {
            this.lastReconciliationAllocationStats = allocationStats;
        }
        weightStatsPerNodeRef.set(weightStatsPerNode);
        allocationStatsPerNodeRef.set(nodeAllocationStats);
    }

    public DesiredBalanceMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
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
            this::getUndesiredAllocationsExcludingShuttingDownNodesMetrics
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

        meterRegistry.registerDoublesGauge(
            CURRENT_NODE_WEIGHT_METRIC_NAME,
            "The weight of nodes based on the current allocation state",
            "unit",
            this::getCurrentNodeWeightMetrics
        );
        meterRegistry.registerDoublesGauge(
            CURRENT_NODE_WRITE_LOAD_METRIC_NAME,
            "The current write load of nodes",
            "threads",
            this::getCurrentNodeWriteLoadMetrics
        );
        meterRegistry.registerLongsGauge(
            CURRENT_NODE_DISK_USAGE_METRIC_NAME,
            "The current disk usage of nodes",
            "bytes",
            this::getCurrentNodeDiskUsageMetrics
        );
        meterRegistry.registerLongsGauge(
            CURRENT_NODE_SHARD_COUNT_METRIC_NAME,
            "The current shard count of nodes",
            "unit",
            this::getCurrentNodeShardCountMetrics
        );
        meterRegistry.registerLongsGauge(
            CURRENT_NODE_FORECASTED_DISK_USAGE_METRIC_NAME,
            "The current forecasted disk usage of nodes",
            "bytes",
            this::getCurrentNodeForecastedDiskUsageMetrics
        );
        meterRegistry.registerLongsGauge(
            CURRENT_NODE_UNDESIRED_SHARD_COUNT_METRIC_NAME,
            "The current undesired shard count of nodes",
            "unit",
            this::getCurrentNodeUndesiredShardCountMetrics
        );
    }

    /**
     * When {@link #nodeIsMaster} is set to true, the server will report APM metrics registered in this file. When set to false, empty
     * values will be returned such that no APM metrics are sent from this server.
     */
    public void setNodeIsMaster(boolean nodeIsMaster) {
        this.nodeIsMaster = nodeIsMaster;
    }

    public long unassignedShards() {
        return lastReconciliationAllocationStats.unassignedShards();
    }

    public long totalAllocations() {
        return lastReconciliationAllocationStats.totalAllocations();
    }

    public long undesiredAllocations() {
        return lastReconciliationAllocationStats.undesiredAllocationsExcludingShuttingDownNodes();
    }

    public AllocationStats allocationStats() {
        return lastReconciliationAllocationStats;
    }

    public void registerWriteLoadDeciderMaxLatencyGauge(Supplier<Collection<LongWithAttributes>> maxLatencySupplier) {
        meterRegistry.registerLongsGauge(
            WRITE_LOAD_DECIDER_MAX_LATENCY_VALUE,
            "max latency for write load decider",
            "ms",
            maxLatencySupplier
        );
    }

    private List<LongWithAttributes> getUnassignedShardsMetrics() {
        return getIfPublishing(AllocationStats::unassignedShards);
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

    private List<LongWithAttributes> getCurrentNodeDiskUsageMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = allocationStatsPerNodeRef.get();
        List<LongWithAttributes> values = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            values.add(new LongWithAttributes(stats.get(node).currentDiskUsage(), getNodeAttributes(node)));
        }
        return values;
    }

    private List<DoubleWithAttributes> getCurrentNodeWriteLoadMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = allocationStatsPerNodeRef.get();
        List<DoubleWithAttributes> doubles = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            doubles.add(new DoubleWithAttributes(stats.get(node).forecastedIngestLoad(), getNodeAttributes(node)));
        }
        return doubles;
    }

    private List<LongWithAttributes> getCurrentNodeShardCountMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = allocationStatsPerNodeRef.get();
        List<LongWithAttributes> values = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            values.add(new LongWithAttributes(stats.get(node).shards(), getNodeAttributes(node)));
        }
        return values;
    }

    private List<LongWithAttributes> getCurrentNodeForecastedDiskUsageMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = allocationStatsPerNodeRef.get();
        List<LongWithAttributes> values = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            values.add(new LongWithAttributes(stats.get(node).forecastedDiskUsage(), getNodeAttributes(node)));
        }
        return values;
    }

    private List<LongWithAttributes> getCurrentNodeUndesiredShardCountMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = allocationStatsPerNodeRef.get();
        List<LongWithAttributes> values = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            values.add(new LongWithAttributes(stats.get(node).undesiredShards(), getNodeAttributes(node)));
        }
        return values;
    }

    private List<DoubleWithAttributes> getCurrentNodeWeightMetrics() {
        if (nodeIsMaster == false) {
            return List.of();
        }
        var stats = allocationStatsPerNodeRef.get();
        List<DoubleWithAttributes> doubles = new ArrayList<>(stats.size());
        for (var node : stats.keySet()) {
            doubles.add(new DoubleWithAttributes(stats.get(node).currentNodeWeight(), getNodeAttributes(node)));
        }
        return doubles;
    }

    private Map<String, Object> getNodeAttributes(DiscoveryNode node) {
        return Map.of("node_id", node.getId(), "node_name", node.getName());
    }

    private List<LongWithAttributes> getTotalAllocationsMetrics() {
        return getIfPublishing(AllocationStats::totalAllocations);
    }

    private List<LongWithAttributes> getUndesiredAllocationsExcludingShuttingDownNodesMetrics() {
        return getIfPublishing(AllocationStats::undesiredAllocationsExcludingShuttingDownNodes);
    }

    private List<LongWithAttributes> getIfPublishing(ToLongFunction<AllocationStats> value) {
        var currentStats = lastReconciliationAllocationStats;
        if (nodeIsMaster && currentStats != EMPTY_ALLOCATION_STATS) {
            return List.of(new LongWithAttributes(value.applyAsLong(currentStats)));
        }
        return List.of();
    }

    private List<DoubleWithAttributes> getUndesiredAllocationsRatioMetrics() {
        var currentStats = lastReconciliationAllocationStats;
        if (nodeIsMaster && currentStats != EMPTY_ALLOCATION_STATS) {
            return List.of(new DoubleWithAttributes(currentStats.undesiredAllocationsRatio()));
        }
        return List.of();
    }

    /**
     * Sets all the internal class fields to zero/empty. Typically used in conjunction with {@link #setNodeIsMaster}.
     * This is best-effort because it is possible for {@link #updateMetrics} to race with this method.
     */
    public void zeroAllMetrics() {
        lastReconciliationAllocationStats = EMPTY_ALLOCATION_STATS;
        weightStatsPerNodeRef.set(Map.of());
        allocationStatsPerNodeRef.set(Map.of());
    }
}
