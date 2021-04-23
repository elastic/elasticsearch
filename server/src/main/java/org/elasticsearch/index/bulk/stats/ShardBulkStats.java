/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.index.shard.IndexShard;

import java.util.concurrent.TimeUnit;

/**
 * Internal class that maintains relevant shard bulk statistics / metrics.
 * @see IndexShard
 */
public class ShardBulkStats implements BulkOperationListener {

    private final StatsHolder totalStats = new StatsHolder();
    private static final double ALPHA = 0.1;

    public BulkStats stats() {
        return totalStats.stats();
    }

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
                (long) sizeInBytes.getAverage());
        }
    }
}
