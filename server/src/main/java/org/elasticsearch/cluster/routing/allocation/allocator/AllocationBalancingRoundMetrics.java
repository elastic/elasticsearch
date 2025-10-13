/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.allocation.allocator.BalancingRoundSummary.NodesWeightsChanges;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A telemetry metrics sender for {@link BalancingRoundSummary.CombinedBalancingRoundSummary}
 */
public class AllocationBalancingRoundMetrics {

    // counters that measure rounds and moves from the last balancing round
    public static final String NUMBER_OF_BALANCING_ROUNDS_METRIC_NAME = "es.allocator.balancing_round.balancing_rounds";
    public static final String NUMBER_OF_SHARD_MOVES_METRIC_NAME = "es.allocator.balancing_round.shard_moves";

    // gauges that measure current utilization
    public static final String NUMBER_OF_SHARDS_METRIC_NAME = "es.allocator.balancing_round.shard_count";
    public static final String DISK_USAGE_BYTES_METRIC_NAME = "es.allocator.balancing_round.disk_usage_bytes";
    public static final String WRITE_LOAD_METRIC_NAME = "es.allocator.balancing_round.write_load";
    public static final String TOTAL_WEIGHT_METRIC_NAME = "es.allocator.balancing_round.total_weight";

    private final LongCounter balancingRoundCounter;
    private final LongCounter shardMovesCounter;

    /**
     * The current view of the last period's summary
     */
    private final AtomicReference<Map<String, NodesWeightsChanges>> nodeNameToWeightChangesRef = new AtomicReference<>();

    /**
     * Whether metrics sending is enabled
     */
    private volatile boolean enableSending = false;

    public static final AllocationBalancingRoundMetrics NOOP = new AllocationBalancingRoundMetrics(MeterRegistry.NOOP);

    private final MeterRegistry meterRegistry;

    public AllocationBalancingRoundMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        this.balancingRoundCounter = meterRegistry.registerLongCounter(
            NUMBER_OF_BALANCING_ROUNDS_METRIC_NAME,
            "Current number of balancing rounds",
            "unit"
        );
        this.shardMovesCounter = meterRegistry.registerLongCounter(
            NUMBER_OF_SHARD_MOVES_METRIC_NAME,
            "Current number of shard moves",
            "{shard}"
        );
        this.nodeNameToWeightChangesRef.set(Map.of());

        meterRegistry.registerLongsGauge(NUMBER_OF_SHARDS_METRIC_NAME, "Current number of shards", "unit", this::getShardCount);
        meterRegistry.registerDoublesGauge(DISK_USAGE_BYTES_METRIC_NAME, "Disk usage in bytes", "unit", this::getDiskUsage);
        meterRegistry.registerDoublesGauge(WRITE_LOAD_METRIC_NAME, "Write load", "1.0", this::getWriteLoad);
        meterRegistry.registerDoublesGauge(TOTAL_WEIGHT_METRIC_NAME, "Total weight", "1.0", this::getTotalWeight);
    }

    public void setEnableSending(boolean enableSending) {
        this.enableSending = enableSending;
    }

    public void updateBalancingRoundMetrics(BalancingRoundSummary.CombinedBalancingRoundSummary summary) {
        assert summary != null : "balancing round metrics cannot be null";

        nodeNameToWeightChangesRef.set(summary.nodeNameToWeightChanges());
        if (enableSending) {
            balancingRoundCounter.incrementBy(summary.numberOfBalancingRounds());
            shardMovesCounter.incrementBy(summary.numberOfShardMoves());
        }
    }

    public void clearBalancingRoundMetrics() {
        nodeNameToWeightChangesRef.set(Map.of());
    }

    private Map<String, Object> getNodeAttributes(String nodeId) {
        return Map.of("node_id", nodeId);
    }

    private List<LongWithAttributes> getShardCount() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = nodeNameToWeightChangesRef.get();
        List<LongWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            NodesWeightsChanges nodeWeightChanges = nodeWeights.getValue();
            long shardCount = nodeWeightChanges.baseWeights().shardCount() + nodeWeightChanges.weightsDiff().shardCountDiff();
            metrics.add(new LongWithAttributes(shardCount, getNodeAttributes(nodeWeights.getKey())));
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getDiskUsage() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = nodeNameToWeightChangesRef.get();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            NodesWeightsChanges nodeWeightChanges = nodeWeights.getValue();
            double diskUsage = nodeWeightChanges.baseWeights().diskUsageInBytes() + nodeWeightChanges.weightsDiff().diskUsageInBytesDiff();
            metrics.add(new DoubleWithAttributes(diskUsage, getNodeAttributes(nodeWeights.getKey())));
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getWriteLoad() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = nodeNameToWeightChangesRef.get();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            NodesWeightsChanges nodeWeightChanges = nodeWeights.getValue();
            double writeLoad = nodeWeightChanges.baseWeights().writeLoad() + nodeWeightChanges.weightsDiff().writeLoadDiff();
            metrics.add(new DoubleWithAttributes(writeLoad, getNodeAttributes(nodeWeights.getKey())));
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getTotalWeight() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = nodeNameToWeightChangesRef.get();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            NodesWeightsChanges nodeWeightChanges = nodeWeights.getValue();
            double totalWeight = nodeWeightChanges.baseWeights().nodeWeight() + nodeWeightChanges.weightsDiff().totalWeightDiff();
            metrics.add(new DoubleWithAttributes(totalWeight, getNodeAttributes(nodeWeights.getKey())));
        }
        return metrics;
    }
}
