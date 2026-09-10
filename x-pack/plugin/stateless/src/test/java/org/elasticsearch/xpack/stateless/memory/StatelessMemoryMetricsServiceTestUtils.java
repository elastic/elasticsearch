/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

public class StatelessMemoryMetricsServiceTestUtils {

    private StatelessMemoryMetricsServiceTestUtils() {}

    public static long getLastMaxTotalPostingsInMemoryBytes(StatelessMemoryMetricsService service) {
        return service.getLastMaxTotalPostingsInMemoryBytes();
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
