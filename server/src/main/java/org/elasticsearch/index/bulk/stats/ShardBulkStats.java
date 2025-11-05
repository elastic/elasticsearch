/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.index.shard.IndexShard;

import java.util.concurrent.TimeUnit;

/**
 * Internal class that maintains relevant shard bulk statistics and metrics.
 * Tracks bulk operation counts, timings, and sizes using exponentially weighted moving averages.
 *
 * @see IndexShard
 */
public class ShardBulkStats implements BulkOperationListener {

    private final StatsHolder totalStats = new StatsHolder();
    /** Alpha value for exponentially weighted moving average calculation */
    private static final double ALPHA = 0.1;

    /**
     * Retrieves a snapshot of the current bulk statistics.
     *
     * @return a BulkStats object containing aggregated statistics
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ShardBulkStats shardStats = new ShardBulkStats();
     * // ... bulk operations occur ...
     * BulkStats stats = shardStats.stats();
     * long totalOps = stats.getTotalOperations();
     * }</pre>
     */
    public BulkStats stats() {
        return totalStats.stats();
    }

    /**
     * Called after a bulk operation completes.
     * Updates all metrics including counts, sizes, and moving averages.
     *
     * @param shardBulkSizeInBytes the size of the bulk operation in bytes
     * @param tookInNanos the time taken for the bulk operation in nanoseconds
     */
    @Override
    public void afterBulk(long shardBulkSizeInBytes, long tookInNanos) {
        totalStats.totalSizeInBytes.inc(shardBulkSizeInBytes);
        totalStats.shardBulkMetric.inc(tookInNanos);
        totalStats.timeInMillis.addValue(tookInNanos);
        totalStats.sizeInBytes.addValue(shardBulkSizeInBytes);
    }

    static final class StatsHolder {
        final MeanMetric shardBulkMetric = new MeanMetric();
        final CounterMetric totalSizeInBytes = new CounterMetric();
        ExponentiallyWeightedMovingAverage timeInMillis = new ExponentiallyWeightedMovingAverage(ALPHA, 0.0);
        ExponentiallyWeightedMovingAverage sizeInBytes = new ExponentiallyWeightedMovingAverage(ALPHA, 0.0);

        BulkStats stats() {
            return new BulkStats(
                shardBulkMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(shardBulkMetric.sum()),
                totalSizeInBytes.count(),
                TimeUnit.NANOSECONDS.toMillis((long) timeInMillis.getAverage()),
                (long) sizeInBytes.getAverage()
            );
        }
    }
}
