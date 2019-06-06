/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.diagnostics;

import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.utils.Intervals;

import java.util.Date;

/**
 * A moving window of buckets that allow keeping
 * track of some statistics like the bucket count,
 * empty or sparse buckets, etc.
 *
 * The counts are stored in an array that functions as a
 * circular buffer. When time is advanced, all buckets
 * out of the window are flushed.
 */
class BucketDiagnostics {

    private static final int MIN_BUCKETS = 10;

    private final long bucketSpanMs;
    private final long latencyMs;
    private final int maxSize;
    private final long[] buckets;
    private long movingBucketCount = 0;
    private long latestBucketStartMs = -1;
    private int latestBucketIndex;
    private long earliestBucketStartMs = -1;
    private int earliestBucketIndex;
    private long latestFlushedBucketStartMs = -1;
    private final BucketFlushListener bucketFlushListener;

    BucketDiagnostics(Job job, DataCounts dataCounts, BucketFlushListener bucketFlushListener) {
        bucketSpanMs = job.getAnalysisConfig().getBucketSpan().millis();
        latencyMs = job.getAnalysisConfig().getLatency() == null ? 0 : job.getAnalysisConfig().getLatency().millis();
        maxSize = Math.max((int) (Intervals.alignToCeil(latencyMs, bucketSpanMs) / bucketSpanMs), MIN_BUCKETS);
        buckets = new long[maxSize];
        this.bucketFlushListener = bucketFlushListener;

        Date latestRecordTimestamp = dataCounts.getLatestRecordTimeStamp();
        if (latestRecordTimestamp != null) {
            addRecord(latestRecordTimestamp.getTime());
        }
    }

    void addRecord(long recordTimestampMs) {
        long bucketStartMs = Intervals.alignToFloor(recordTimestampMs, bucketSpanMs);

        // Initialize earliest/latest times
        if (latestBucketStartMs < 0) {
            latestBucketStartMs = bucketStartMs;
            earliestBucketStartMs = bucketStartMs;
        }

        advanceTime(bucketStartMs);
        addToBucket(bucketStartMs);
    }

    private void advanceTime(long bucketStartMs) {
        while (bucketStartMs > latestBucketStartMs) {
            int flushBucketIndex = (latestBucketIndex + 1) % maxSize;

            if (flushBucketIndex == earliestBucketIndex) {
                flush(flushBucketIndex);
                movingBucketCount -= buckets[flushBucketIndex];
                earliestBucketStartMs += bucketSpanMs;
                earliestBucketIndex = (earliestBucketIndex + 1) % maxSize;
            }
            buckets[flushBucketIndex] = 0L;

            latestBucketStartMs += bucketSpanMs;
            latestBucketIndex = flushBucketIndex;
        }
    }

    private void addToBucket(long bucketStartMs) {
        int offsetToLatest = (int) ((bucketStartMs - latestBucketStartMs) / bucketSpanMs);
        int bucketIndex = (latestBucketIndex + offsetToLatest) % maxSize;
        if (bucketIndex < 0) {
            bucketIndex = maxSize + bucketIndex;
        }

        ++buckets[bucketIndex];
        ++movingBucketCount;

        if (bucketStartMs < earliestBucketStartMs) {
            earliestBucketStartMs = bucketStartMs;
            earliestBucketIndex = bucketIndex;
        }
    }

    private void flush(int bucketIndex) {
        long bucketStartMs = getTimestampMs(bucketIndex);
        if (bucketStartMs > latestFlushedBucketStartMs) {
            bucketFlushListener.onBucketFlush(bucketStartMs, buckets[bucketIndex]);
            latestFlushedBucketStartMs = bucketStartMs;
        }
    }

    private long getTimestampMs(int bucketIndex) {
        int offsetToLatest = latestBucketIndex - bucketIndex;
        if (offsetToLatest < 0) {
            offsetToLatest = maxSize + offsetToLatest;
        }
        return latestBucketStartMs - offsetToLatest * bucketSpanMs;
    }

    void flush() {
        if (latestBucketStartMs < 0) {
            return;
        }

        int bucketIndex = earliestBucketIndex;
        while (bucketIndex != latestBucketIndex) {
            flush(bucketIndex);
            bucketIndex = (bucketIndex + 1) % maxSize;
        }
    }

    double averageBucketCount() {
        return (double) movingBucketCount / size();
    }

    private int size() {
        if (latestBucketStartMs < 0) {
            return 0;
        }
        return (int) ((latestBucketStartMs - earliestBucketStartMs) / bucketSpanMs) + 1;
    }

    interface BucketFlushListener {
        void onBucketFlush(long bucketStartMs, long bucketCounts);
    }
}
