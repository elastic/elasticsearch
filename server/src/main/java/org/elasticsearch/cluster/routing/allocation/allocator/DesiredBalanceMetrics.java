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
import org.elasticsearch.telemetry.metric.DoubleAsyncMeasurement;
import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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

    // Desired balance computation metrics (from {@link DesiredBalanceStats}).
    public static final String COMPUTATIONS_SUBMITTED_METRIC_NAME = "es.allocator.desired_balance.computations.submitted.total";
    public static final String COMPUTATIONS_EXECUTED_METRIC_NAME = "es.allocator.desired_balance.computations.executed.total";
    public static final String COMPUTATIONS_CONVERGED_METRIC_NAME = "es.allocator.desired_balance.computations.converged.total";
    public static final String COMPUTATIONS_ITERATIONS_METRIC_NAME = "es.allocator.desired_balance.computations.iterations.total";
    public static final String COMPUTATIONS_TIME_METRIC_NAME = "es.allocator.desired_balance.computations.time";
    public static final String RECONCILIATIONS_TIME_METRIC_NAME = "es.allocator.desired_balance.reconciliations.time";

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
    private final LongGauge writeLoadDeciderMaxQueueLatencyGauge;

    private volatile DesiredBalanceStats desiredBalanceStats = DesiredBalanceStats.ZERO;

    public void updateMetrics(
        AllocationStats allocationStats,
        Map<DiscoveryNode, NodeWeightStats> weightStatsPerNode,
        Map<DiscoveryNode, NodeAllocationStatsAndWeight> nodeAllocationStats,
        DesiredBalanceStats desiredBalanceStats
    ) {
        assert allocationStats != null : "allocation stats cannot be null";
        assert weightStatsPerNode != null : "node balance weight stats cannot be null";
        if (allocationStats != EMPTY_ALLOCATION_STATS) {
            this.lastReconciliationAllocationStats = allocationStats;
        }
        weightStatsPerNodeRef.set(weightStatsPerNode);
        allocationStatsPerNodeRef.set(nodeAllocationStats);
        this.desiredBalanceStats = desiredBalanceStats;
    }

    public DesiredBalanceMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.writeLoadDeciderMaxQueueLatencyGauge = meterRegistry.registerLongGauge(
            WRITE_LOAD_DECIDER_MAX_LATENCY_VALUE,
            "max latency for write load decider",
            "ms"
        );
        meterRegistry.registerLongAsyncGauge(
            UNASSIGNED_SHARDS_METRIC_NAME,
            "Current number of unassigned shards",
            "{shard}",
            this::recordUnassignedShardsMetrics
        );
        meterRegistry.registerLongAsyncGauge(
            TOTAL_SHARDS_METRIC_NAME,
            "Total number of shards",
            "{shard}",
            this::recordTotalAllocationsMetrics
        );
        meterRegistry.registerLongAsyncGauge(
            UNDESIRED_ALLOCATION_COUNT_METRIC_NAME,
            "Total number of shards allocated on undesired nodes excluding shutting down nodes",
            "{shard}",
            this::recordUndesiredAllocationsExcludingShuttingDownNodesMetrics
        );
        meterRegistry.registerDoubleAsyncGauge(
            UNDESIRED_ALLOCATION_RATIO_METRIC_NAME,
            "Ratio of undesired allocations to shard count excluding shutting down nodes",
            "1",
            this::recordUndesiredAllocationsRatioMetrics
        );

        meterRegistry.registerLongAsyncCounter(
            COMPUTATIONS_SUBMITTED_METRIC_NAME,
            "Total number of desired balance computations submitted on this elected master",
            "unit",
            this::recordComputationSubmittedMetrics
        );
        meterRegistry.registerLongAsyncCounter(
            COMPUTATIONS_EXECUTED_METRIC_NAME,
            "Total number of desired balance computations executed on this elected master",
            "unit",
            this::recordComputationExecutedMetrics
        );
        meterRegistry.registerLongAsyncCounter(
            COMPUTATIONS_CONVERGED_METRIC_NAME,
            "Total number of desired balance computations that converged on this elected master",
            "unit",
            this::recordComputationConvergedMetrics
        );
        meterRegistry.registerLongAsyncCounter(
            COMPUTATIONS_ITERATIONS_METRIC_NAME,
            "Total iterations across desired balance computations on this elected master",
            "unit",
            this::recordComputationIterationsMetrics
        );
        meterRegistry.registerLongAsyncCounter(
            COMPUTATIONS_TIME_METRIC_NAME,
            "Cumulative wall-clock time spent in desired balance computation on this elected master",
            "ms",
            this::recordCumulativeComputationTimeMillisMetrics
        );
        meterRegistry.registerLongAsyncCounter(
            RECONCILIATIONS_TIME_METRIC_NAME,
            "Cumulative wall-clock time spent reconciling toward the desired balance on this elected master",
            "ms",
            this::recordCumulativeReconciliationTimeMillisMetrics
        );

        meterRegistry.registerDoubleAsyncGauge(
            DESIRED_BALANCE_NODE_WEIGHT_METRIC_NAME,
            "Weight of nodes in the computed desired balance",
            "unit",
            this::recordDesiredBalanceNodeWeightMetrics
        );
        meterRegistry.registerDoubleAsyncGauge(
            DESIRED_BALANCE_NODE_WRITE_LOAD_METRIC_NAME,
            "Write load of nodes in the computed desired balance",
            "threads",
            this::recordDesiredBalanceNodeWriteLoadMetrics
        );
        meterRegistry.registerDoubleAsyncGauge(
            DESIRED_BALANCE_NODE_DISK_USAGE_METRIC_NAME,
            "Disk usage of nodes in the computed desired balance",
            "bytes",
            this::recordDesiredBalanceNodeDiskUsageMetrics
        );
        meterRegistry.registerLongAsyncGauge(
            DESIRED_BALANCE_NODE_SHARD_COUNT_METRIC_NAME,
            "Shard count of nodes in the computed desired balance",
            "unit",
            this::recordDesiredBalanceNodeShardCountMetrics
        );

        meterRegistry.registerDoubleAsyncGauge(
            CURRENT_NODE_WEIGHT_METRIC_NAME,
            "The weight of nodes based on the current allocation state",
            "unit",
            this::recordCurrentNodeWeightMetrics
        );
        meterRegistry.registerDoubleAsyncGauge(
            CURRENT_NODE_WRITE_LOAD_METRIC_NAME,
            "The current write load of nodes",
            "threads",
            this::recordCurrentNodeWriteLoadMetrics
        );
        meterRegistry.registerLongAsyncGauge(
            CURRENT_NODE_DISK_USAGE_METRIC_NAME,
            "The current disk usage of nodes",
            "bytes",
            this::recordCurrentNodeDiskUsageMetrics
        );
        meterRegistry.registerLongAsyncGauge(
            CURRENT_NODE_SHARD_COUNT_METRIC_NAME,
            "The current shard count of nodes",
            "unit",
            this::recordCurrentNodeShardCountMetrics
        );
        meterRegistry.registerLongAsyncGauge(
            CURRENT_NODE_FORECASTED_DISK_USAGE_METRIC_NAME,
            "The current forecasted disk usage of nodes",
            "bytes",
            this::recordCurrentNodeForecastedDiskUsageMetrics
        );
        meterRegistry.registerLongAsyncGauge(
            CURRENT_NODE_UNDESIRED_SHARD_COUNT_METRIC_NAME,
            "The current undesired shard count of nodes",
            "unit",
            this::recordCurrentNodeUndesiredShardCountMetrics
        );
    }

    public LongGauge getWriteLoadDeciderMaxQueueLatencyGauge() {
        return writeLoadDeciderMaxQueueLatencyGauge;
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

    DesiredBalanceStats desiredBalanceStats() {
        return desiredBalanceStats;
    }

    private void recordUnassignedShardsMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishing(AllocationStats::unassignedShards, measurement);
    }

    private void recordDesiredBalanceNodeWeightMetrics(DoubleAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = weightStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).nodeWeight(), getNodeAttributes(node));
        }
    }

    private void recordDesiredBalanceNodeWriteLoadMetrics(DoubleAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = weightStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).writeLoad(), getNodeAttributes(node));
        }
    }

    private void recordDesiredBalanceNodeDiskUsageMetrics(DoubleAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = weightStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).diskUsageInBytes(), getNodeAttributes(node));
        }
    }

    private void recordDesiredBalanceNodeShardCountMetrics(LongAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = weightStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).shardCount(), getNodeAttributes(node));
        }
    }

    private void recordCurrentNodeDiskUsageMetrics(LongAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = allocationStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).currentDiskUsage(), getNodeAttributes(node));
        }
    }

    private void recordCurrentNodeWriteLoadMetrics(DoubleAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = allocationStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).forecastedIngestLoad(), getNodeAttributes(node));
        }
    }

    private void recordCurrentNodeShardCountMetrics(LongAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = allocationStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).shards(), getNodeAttributes(node));
        }
    }

    private void recordCurrentNodeForecastedDiskUsageMetrics(LongAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = allocationStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).forecastedDiskUsage(), getNodeAttributes(node));
        }
    }

    private void recordCurrentNodeUndesiredShardCountMetrics(LongAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = allocationStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).undesiredShards(), getNodeAttributes(node));
        }
    }

    private void recordCurrentNodeWeightMetrics(DoubleAsyncMeasurement measurement) {
        if (nodeIsMaster == false) {
            return;
        }
        var stats = allocationStatsPerNodeRef.get();
        for (var node : stats.keySet()) {
            measurement.record(stats.get(node).currentNodeWeight(), getNodeAttributes(node));
        }
    }

    private Map<String, Object> getNodeAttributes(DiscoveryNode node) {
        return Map.of("node_id", node.getId(), "node_name", node.getName());
    }

    private void recordTotalAllocationsMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishing(AllocationStats::totalAllocations, measurement);
    }

    private void recordUndesiredAllocationsExcludingShuttingDownNodesMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishing(AllocationStats::undesiredAllocationsExcludingShuttingDownNodes, measurement);
    }

    private void recordIfPublishing(ToLongFunction<AllocationStats> value, LongAsyncMeasurement measurement) {
        var currentStats = lastReconciliationAllocationStats;
        if (nodeIsMaster && currentStats != EMPTY_ALLOCATION_STATS) {
            measurement.record(value.applyAsLong(currentStats));
        }
    }

    private void recordUndesiredAllocationsRatioMetrics(DoubleAsyncMeasurement measurement) {
        var currentStats = lastReconciliationAllocationStats;
        if (nodeIsMaster && currentStats != EMPTY_ALLOCATION_STATS) {
            measurement.record(currentStats.undesiredAllocationsRatio());
        }
    }

    private void recordComputationSubmittedMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishingDesiredBalanceStats(DesiredBalanceStats::computationSubmitted, measurement);
    }

    private void recordComputationExecutedMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishingDesiredBalanceStats(DesiredBalanceStats::computationExecuted, measurement);
    }

    private void recordComputationConvergedMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishingDesiredBalanceStats(DesiredBalanceStats::computationConverged, measurement);
    }

    private void recordComputationIterationsMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishingDesiredBalanceStats(DesiredBalanceStats::computationIterations, measurement);
    }

    private void recordCumulativeComputationTimeMillisMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishingDesiredBalanceStats(DesiredBalanceStats::cumulativeComputationTime, measurement);
    }

    private void recordCumulativeReconciliationTimeMillisMetrics(LongAsyncMeasurement measurement) {
        recordIfPublishingDesiredBalanceStats(DesiredBalanceStats::cumulativeReconciliationTime, measurement);
    }

    private void recordIfPublishingDesiredBalanceStats(ToLongFunction<DesiredBalanceStats> value, LongAsyncMeasurement measurement) {
        if (nodeIsMaster && lastReconciliationAllocationStats != EMPTY_ALLOCATION_STATS) {
            measurement.record(value.applyAsLong(desiredBalanceStats));
        }
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
