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

    public static final String NUMBER_OF_BALANCING_ROUNDS_METRIC_NAME = "es.allocator.balancing_round.balancing_rounds";

    public static final String NUMBER_OF_SHARD_MOVES_METRIC_NAME = "es.allocator.balancing_round.shard_moves";

    public static final String NUMBER_OF_SHARDS_METRIC_NAME = "es.allocator.balancing_round.shard_count";
    public static final String NUMBER_OF_SHARDS_DELTA_METRIC_NAME = "es.allocator.balancing_round.shard_count_delta";

    public static final String DISK_USAGE_BYTES_METRIC_NAME = "es.allocator.balancing_round.disk_usage_bytes";
    public static final String DISK_USAGE_BYTES_DELTA_METRIC_NAME = "es.allocator.balancing_round.disk_usage_bytes_delta";

    public static final String WRITE_LOAD_METRIC_NAME = "es.allocator.balancing_round.write_load";
    public static final String WRITE_LOAD_DELTA_METRIC_NAME = "es.allocator.balancing_round.write_load_delta";

    public static final String TOTAL_WEIGHT_METRIC_NAME = "es.allocator.balancing_round.total_weight";
    public static final String TOTAL_WEIGHT_DELTA_METRIC_NAME = "es.allocator.balancing_round.total_weight_delta";

    /**
     * The current view of the last period's summary
     */
    private final AtomicReference<BalancingRoundSummary.CombinedBalancingRoundSummary> combinedSummariesRef = new AtomicReference<>();

    /**
     * Whether metrics sending is enabled
     */
    private volatile boolean enableSending = false;

    public static final AllocationBalancingRoundMetrics NOOP = new AllocationBalancingRoundMetrics(MeterRegistry.NOOP);

    private final MeterRegistry meterRegistry;

    public AllocationBalancingRoundMetrics(MeterRegistry meterRegistry) {
        this.combinedSummariesRef.set(BalancingRoundSummary.CombinedBalancingRoundSummary.EMPTY_RESULTS);
        this.meterRegistry = meterRegistry;

        meterRegistry.registerLongsGauge(
            NUMBER_OF_BALANCING_ROUNDS_METRIC_NAME,
            "Current number of balancing rounds",
            "unit",
            this::getBalancingRounds
        );

        meterRegistry.registerLongsGauge(
            NUMBER_OF_SHARD_MOVES_METRIC_NAME,
            "Current number of shard moves",
            "{shard}",
            this::getShardMoves
        );

        meterRegistry.registerLongsGauge(NUMBER_OF_SHARDS_METRIC_NAME, "Current number of shards", "unit", this::getShardCount);
        meterRegistry.registerLongsGauge(
            NUMBER_OF_SHARDS_DELTA_METRIC_NAME,
            "Current number of shards delta",
            "{shard}",
            this::getShardCountDelta
        );

        meterRegistry.registerDoublesGauge(DISK_USAGE_BYTES_METRIC_NAME, "Disk usage in bytes", "unit", this::getDiskUsage);
        meterRegistry.registerDoublesGauge(
            DISK_USAGE_BYTES_DELTA_METRIC_NAME,
            "Disk usage delta in bytes",
            "{shard}",
            this::getDiskUsageDelta
        );

        meterRegistry.registerDoublesGauge(WRITE_LOAD_METRIC_NAME, "Write load", "1.0", this::getWriteLoad);
        meterRegistry.registerDoublesGauge(WRITE_LOAD_DELTA_METRIC_NAME, "Write load delta", "1.0", this::getWriteLoadDelta);

        meterRegistry.registerDoublesGauge(TOTAL_WEIGHT_METRIC_NAME, "Total weight", "1.0", this::getTotalWeight);
        meterRegistry.registerDoublesGauge(TOTAL_WEIGHT_DELTA_METRIC_NAME, "Total weight delta", "1.0", this::getTotalWeightDelta);
    }

    public void setEnableSending(boolean enableSending) {
        this.enableSending = enableSending;
    }

    public void updateBalancingRoundMetrics(BalancingRoundSummary.CombinedBalancingRoundSummary summary) {
        assert summary != null : "balancing round metrics cannot be null";
        combinedSummariesRef.set(summary);
    }

    public void clearBalancingRoundMetrics() {
        combinedSummariesRef.set(BalancingRoundSummary.CombinedBalancingRoundSummary.EMPTY_RESULTS);
    }

    private Map<String, Object> getNodeAttributes(String nodeId) {
        return Map.of("node_id", nodeId);
    }

    private List<LongWithAttributes> getBalancingRounds() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        LongWithAttributes result = new LongWithAttributes(combinedSummary.numberOfShardMoves());
        return List.of(result);
    }

    private List<LongWithAttributes> getShardMoves() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        LongWithAttributes result = new LongWithAttributes(combinedSummary.numberOfShardMoves());
        return List.of(result);
    }

    private List<LongWithAttributes> getShardCount() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = combinedSummary.nodeNameToWeightChanges();
        List<LongWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            metrics.add(new LongWithAttributes(nodeWeights.getValue().baseWeights().shardCount(), getNodeAttributes(nodeWeights.getKey())));
        }
        return metrics;
    }

    private List<LongWithAttributes> getShardCountDelta() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = combinedSummary.nodeNameToWeightChanges();
        List<LongWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            metrics.add(
                new LongWithAttributes(nodeWeights.getValue().weightsDiff().shardCountDiff(), getNodeAttributes(nodeWeights.getKey()))
            );
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getDiskUsage() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = combinedSummary.nodeNameToWeightChanges();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            metrics.add(
                new DoubleWithAttributes(nodeWeights.getValue().baseWeights().diskUsageInBytes(), getNodeAttributes(nodeWeights.getKey()))
            );
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getDiskUsageDelta() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = combinedSummary.nodeNameToWeightChanges();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            metrics.add(
                new DoubleWithAttributes(
                    nodeWeights.getValue().weightsDiff().diskUsageInBytesDiff(),
                    getNodeAttributes(nodeWeights.getKey())
                )
            );
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getWriteLoad() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = combinedSummary.nodeNameToWeightChanges();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            metrics.add(
                new DoubleWithAttributes(nodeWeights.getValue().baseWeights().writeLoad(), getNodeAttributes(nodeWeights.getKey()))
            );
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getWriteLoadDelta() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = combinedSummary.nodeNameToWeightChanges();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            metrics.add(
                new DoubleWithAttributes(nodeWeights.getValue().weightsDiff().writeLoadDiff(), getNodeAttributes(nodeWeights.getKey()))
            );
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getTotalWeight() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = combinedSummary.nodeNameToWeightChanges();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            metrics.add(
                new DoubleWithAttributes(nodeWeights.getValue().baseWeights().nodeWeight(), getNodeAttributes(nodeWeights.getKey()))
            );
        }
        return metrics;
    }

    private List<DoubleWithAttributes> getTotalWeightDelta() {
        if (enableSending == false) {
            return Collections.emptyList();
        }

        final BalancingRoundSummary.CombinedBalancingRoundSummary combinedSummary = combinedSummariesRef.get();
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges = combinedSummary.nodeNameToWeightChanges();
        List<DoubleWithAttributes> metrics = new ArrayList<>(nodeNameToWeightChanges.size());
        for (var nodeWeights : nodeNameToWeightChanges.entrySet()) {
            metrics.add(
                new DoubleWithAttributes(nodeWeights.getValue().weightsDiff().totalWeightDiff(), getNodeAttributes(nodeWeights.getKey()))
            );
        }
        return metrics;
    }
}
