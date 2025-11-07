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
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * A telemetry metrics sender for {@link BalancingRoundSummary}
 */
public class AllocationBalancingRoundMetrics {

    // counters that measure rounds and moves from the last balancing round
    public static final String NUMBER_OF_BALANCING_ROUNDS_METRIC_NAME = "es.allocator.balancing_round.balancing_rounds";
    public static final String NUMBER_OF_SHARD_MOVES_METRIC_NAME = "es.allocator.balancing_round.shard_moves";
    public static final String NUMBER_OF_SHARD_MOVES_HISTOGRAM_METRIC_NAME = "es.allocator.balancing_round.shard_moves_histogram";

    // histograms that measure current utilization
    public static final String NUMBER_OF_SHARDS_METRIC_NAME = "es.allocator.balancing_round.shard_count";
    public static final String DISK_USAGE_BYTES_METRIC_NAME = "es.allocator.balancing_round.disk_usage_bytes";
    public static final String WRITE_LOAD_METRIC_NAME = "es.allocator.balancing_round.write_load";
    public static final String TOTAL_WEIGHT_METRIC_NAME = "es.allocator.balancing_round.total_weight";

    private final LongCounter balancingRoundCounter;
    private final LongCounter shardMovesCounter;
    private final LongHistogram shardMovesHistogram;

    private final LongHistogram shardCountHistogram;
    private final DoubleHistogram diskUsageHistogram;
    private final DoubleHistogram writeLoadHistogram;
    private final DoubleHistogram totalWeightHistogram;

    public static AllocationBalancingRoundMetrics NOOP = new AllocationBalancingRoundMetrics(MeterRegistry.NOOP);

    public AllocationBalancingRoundMetrics(MeterRegistry meterRegistry) {
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

        this.shardMovesHistogram = meterRegistry.registerLongHistogram(NUMBER_OF_SHARD_MOVES_HISTOGRAM_METRIC_NAME,
            "Histogram of shard moves", "unit");
        this.shardCountHistogram = meterRegistry.registerLongHistogram(NUMBER_OF_SHARDS_METRIC_NAME, "Current number of shards", "unit");
        this.diskUsageHistogram = meterRegistry.registerDoubleHistogram(DISK_USAGE_BYTES_METRIC_NAME, "Disk usage in bytes", "unit");
        this.writeLoadHistogram = meterRegistry.registerDoubleHistogram(WRITE_LOAD_METRIC_NAME, "Write load", "1.0");
        this.totalWeightHistogram = meterRegistry.registerDoubleHistogram(TOTAL_WEIGHT_METRIC_NAME, "Total weight", "1.0");
    }

    public void addBalancingRoundSummary(BalancingRoundSummary summary) {
        balancingRoundCounter.increment();
        shardMovesCounter.incrementBy(summary.numberOfShardsToMove());
        shardMovesHistogram.record(summary.numberOfShardsToMove());

        for (Map.Entry<String, NodesWeightsChanges> changesEntry : summary.nodeNameToWeightChanges().entrySet()) {
            String nodeName = changesEntry.getKey();
            NodesWeightsChanges weightChanges = changesEntry.getValue();
            BalancingRoundSummary.NodeWeightsDiff weightsDiff = weightChanges.weightsDiff();

            shardCountHistogram.record(Math.abs(weightsDiff.shardCountDiff()), getNodeAttributes(nodeName));
            diskUsageHistogram.record(Math.abs(weightsDiff.diskUsageInBytesDiff()), getNodeAttributes(nodeName));
            writeLoadHistogram.record(Math.abs(weightsDiff.writeLoadDiff()), getNodeAttributes(nodeName));
            totalWeightHistogram.record(Math.abs(weightsDiff.totalWeightDiff()), getNodeAttributes(nodeName));
        }
    }

    private Map<String, Object> getNodeAttributes(String nodeId) {
        return Map.of("node_name", nodeId);
    }
}
