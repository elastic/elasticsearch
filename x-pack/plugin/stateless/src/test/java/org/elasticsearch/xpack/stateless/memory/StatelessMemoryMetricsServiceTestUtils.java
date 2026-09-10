/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.common.unit.ByteSizeValue;

public class StatelessMemoryMetricsServiceTestUtils {

    private StatelessMemoryMetricsServiceTestUtils() {}

    public static ByteSizeValue getFixedShardMemoryOverhead(StatelessMemoryMetricsService service) {
        return service.getFixedShardMemoryOverhead();
    }

    public static long getLastMaxTotalPostingsInMemoryBytes(StatelessMemoryMetricsService service) {
        return service.getLastMaxTotalPostingsInMemoryBytes();
    }

    public static StatelessMemoryMetricsService.ShardMemoryMetrics newUninitialisedShardMemoryMetrics(
        StatelessMemoryMetricsService service,
        long updateTimestampNanos
    ) {
        return service.newUninitialisedShardMemoryMetrics(updateTimestampNanos);
    }

    /// Calculates the shard heap estimate excluding postings and ignoring any self-reported overhead
    /// even if the service is configured to use it
    public static long estimateShardHeapUsageExcludingPostingsAndIgnoringSelfReportedOverheads(
        StatelessMemoryMetricsService statelessMemoryMetricsService,
        StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics
    ) {
        return statelessMemoryMetricsService.createShardHeapEstimator(StatelessMemoryMetricsService.SelfReportedShardOverhead.DISABLE)
            .computeShardHeapUsage(shardMemoryMetrics);
    }

    /// Convenience method for computing the shard estimate excluding postings
    /// and the current settings active on the [StatelessMemoryMetricsService]
    public static ShardAndIndexHeapEstimate estimateHeapUsageExcludingPostings(
        StatelessMemoryMetricsService statelessMemoryMetricsService,
        StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics
    ) {
        ShardHeapEstimator shardHeapEstimator = statelessMemoryMetricsService.createShardHeapEstimator();
        return new ShardAndIndexHeapEstimate(
            shardHeapEstimator.computeShardHeapUsage(shardMemoryMetrics),
            shardHeapEstimator.computeIndexHeapUsage(shardMemoryMetrics),
            shardHeapEstimator.getEffectiveShardPostingsInBytes(shardMemoryMetrics)
        );
    }

    public record ShardAndIndexHeapEstimate(long shardHeapEstimate, long indexHeapEstimate, long shardPostingsHeapEstimate) {

    }
}
