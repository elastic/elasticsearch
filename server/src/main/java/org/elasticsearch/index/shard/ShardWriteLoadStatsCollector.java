/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.RunningTimeRecorder;
import org.elasticsearch.common.metrics.DoubleMean;
import org.elasticsearch.common.metrics.DoubleMeanMetric;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.stats.ShardWriteLoadStats;

import java.util.function.LongSupplier;

class ShardWriteLoadStatsCollector {
    static final ShardWriteLoadStatsCollector NO_OP = new ShardWriteLoadStatsCollector(RunningTimeRecorder.NO_OP) {
        @Override
        public ShardWriteLoadStats getStats() {
            return new ShardWriteLoadStats(DoubleMean.ZERO);
        }

        @Override
        synchronized void recordWriteLoad(long samplingTimeInNanos) {}
    };

    private final DoubleMeanMetric indexingLoadMean = new DoubleMeanMetric();
    private final RunningTimeRecorder bulkTimeRecorder;
    private long lastTotalIndexingTimeSample;

    ShardWriteLoadStatsCollector(LongSupplier relativeTimeSupplier) {
        this(new RunningTimeRecorder(relativeTimeSupplier));
    }

    private ShardWriteLoadStatsCollector(RunningTimeRecorder bulkTimeRecorder) {
        this.bulkTimeRecorder = bulkTimeRecorder;
    }

    Releasable startTrackingBulkOperationTime() {
        return bulkTimeRecorder.trackRunningTime();
    }

    long totalBulkTimeInNanos() {
        return bulkTimeRecorder.totalRunningTimeInNanos();
    }

    synchronized void recordWriteLoad(long samplingTimeInNanos) {
        // Even though we're using a MONOTONIC clock (at least System.nanoTime() relies on clock_gettime with CLOCK_MONOTONIC in linux)
        // it's possible that the clock do not have enough granularity, in that case we bail out early just to be cautious.
        if (samplingTimeInNanos <= 0) {
            return;
        }

        final long totalIndexingTimeInNanosSample = totalBulkTimeInNanos();

        final long indexingTimeDeltaInNanos = totalIndexingTimeInNanosSample - lastTotalIndexingTimeSample;
        lastTotalIndexingTimeSample = totalIndexingTimeInNanosSample;

        final double indexingCPUs = indexingTimeDeltaInNanos / (double) samplingTimeInNanos;

        indexingLoadMean.inc(indexingCPUs);
    }

    // Visible for testing
    int inFlightOps() {
        return bulkTimeRecorder.inFlightOps();
    }

    ShardWriteLoadStats getStats() {
        return new ShardWriteLoadStats(indexingLoadMean.mean());
    }
}
