/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Counter;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Date;
import java.util.SortedMap;
import java.util.TreeMap;

public class DataStreamDiagnostics {

    /**
     * Minimum window to take into consideration for bucket count histogram.
     */
    private static final int MIN_BUCKET_WINDOW = 10;

    /**
     * Threshold to report potential sparsity problems.
     * 
     * Sparsity score is calculated: log(average) - log(current)
     * 
     * If score is above the threshold, bucket is reported as sparse bucket.
     */
    private static final int DATA_SPARSITY_THRESHOLD = 2;
    private static final long MS_IN_SECOND = 1000;

    private static final Logger LOGGER = Loggers.getLogger(DataStreamDiagnostics.class);
    /**
     * Container for the histogram
     *
     * Note: Using a sorted map in order to iterate in order when consuming the
     * data. The counter is lazily initialized and potentially missing in case
     * of empty buckets.
     *
     * The container gets pruned along the data streaming based on the bucket
     * window, so it should not contain more than max(MIN_BUCKET_WINDOW,
     * 'buckets_required_by_latency') + 1 items at any time.
     * 
     * Sparsity can only be calculated after the window has been filled. Currently
     * this window is lost if a job gets closed and re-opened. We might fix this 
     * in future.
     */
    private final SortedMap<Long, Counter> movingBucketHistogram = new TreeMap<>();

    private final long bucketSpan;
    private final long latency;
    private long movingBucketCount = 0;
    private long latestReportedBucket = -1;

    private long bucketCount = 0;
    private long emptyBucketCount = 0;
    private long latestEmptyBucketTime = -1;
    private long sparseBucketCount = 0;
    private long latestSparseBucketTime = -1;
    
    public DataStreamDiagnostics(Job job) {
        bucketSpan = job.getAnalysisConfig().getBucketSpan().seconds();
        latency = job.getAnalysisConfig().getLatency() == null ? 0 : job.getAnalysisConfig().getLatency().seconds();
    }

    /**
     * Check record
     * 
     * @param recordTimestampInMs
     *            The record timestamp in milliseconds since epoch
     */

    public void checkRecord(long recordTimestampInMs) {
        checkBucketing(recordTimestampInMs);
    }

    /**
     * Flush all counters, should be called at the end of the data stream
     */
    public void flush() {
        // flush all we know
        if (movingBucketHistogram.isEmpty() == false) {
            flush(movingBucketHistogram.lastKey());
        }
    }

    /**
     * Check bucketing of record. Report empty and sparse buckets.
     * 
     * @param recordTimestampInMs
     *            The record timestamp in milliseconds since epoch
     */
    private void checkBucketing(long recordTimestampInMs) {
        long bucket = (recordTimestampInMs / MS_IN_SECOND) / bucketSpan;
        long bucketHistogramStartBucket = ((recordTimestampInMs / MS_IN_SECOND) - latency) / bucketSpan;

        bucketHistogramStartBucket = Math.min(bucket - MIN_BUCKET_WINDOW, bucketHistogramStartBucket);

        movingBucketHistogram.computeIfAbsent(bucket, l -> Counter.newCounter()).addAndGet(1);
        ++movingBucketCount;

        // find the very first bucket
        if (latestReportedBucket == -1) {
            latestReportedBucket = bucket - 1;
        }

        // flush all bucket out of the window
        flush(bucketHistogramStartBucket);
    }

    /**
     * Flush Bucket reporting till the given bucket.
     * 
     * @param bucketNumber
     *            The number of the last bucket that can be flushed.
     */
    private void flush(long bucketNumber) {

        // check for a longer period of empty buckets
        long emptyBuckets = movingBucketHistogram.firstKey() - latestReportedBucket - 1;
        if (emptyBuckets > 0) {
            bucketCount += emptyBuckets;
            emptyBucketCount += emptyBuckets;
            latestEmptyBucketTime = (movingBucketHistogram.firstKey() - 1) * bucketSpan * MS_IN_SECOND;
            latestReportedBucket = movingBucketHistogram.firstKey() - 1;
        }

        // calculate the average number of data points in a bucket based on the
        // current history
        double averageBucketSize = (float) movingBucketCount / movingBucketHistogram.size();

        // prune all buckets that can be flushed
        long lastBucketSparsityCheck = Math.min(bucketNumber, movingBucketHistogram.lastKey());

        for (long pruneBucket = movingBucketHistogram.firstKey(); pruneBucket < lastBucketSparsityCheck; ++pruneBucket) {

            Counter bucketSizeHolder = movingBucketHistogram.remove(pruneBucket);
            long bucketSize = bucketSizeHolder != null ? bucketSizeHolder.get() : 0L;

            LOGGER.debug("Checking bucket {} compare sizes, this bucket: {} average: {}", pruneBucket, bucketSize, averageBucketSize);
            ++bucketCount;
            latestReportedBucket = pruneBucket;

            // substract bucketSize from the counter
            movingBucketCount -= bucketSize;

            // check if bucket is empty
            if (bucketSize == 0L) {
                latestEmptyBucketTime = pruneBucket * bucketSpan * MS_IN_SECOND;
                ++emptyBucketCount;

                // do not do sparse analysis on an empty bucket
                continue;
            }

            // simplistic way to calculate data sparsity, just take the log and
            // check the difference
            double logAverageBucketSize = Math.log(averageBucketSize);
            double logBucketSize = Math.log(bucketSize);
            double sparsityScore = logAverageBucketSize - logBucketSize;

            if (sparsityScore > DATA_SPARSITY_THRESHOLD) {
                LOGGER.debug("Sparse bucket {}, this bucket: {} average: {}, sparsity score: {}", pruneBucket, bucketSize,
                        averageBucketSize, sparsityScore);
                ++sparseBucketCount;
                latestSparseBucketTime = pruneBucket * bucketSpan * MS_IN_SECOND;
            }
        }

        // prune the rest if necessary
        for (long pruneBucket = lastBucketSparsityCheck; pruneBucket < bucketNumber; ++pruneBucket) {
            Counter bucketSizeHolder = movingBucketHistogram.remove(pruneBucket);
            long bucketSize = bucketSizeHolder != null ? bucketSizeHolder.get() : 0L;

            bucketCount++;
            latestReportedBucket = pruneBucket;

            // substract bucketSize from the counter
            movingBucketCount -= bucketSize;

            // check if bucket is empty
            if (bucketSize == 0L) {
                latestEmptyBucketTime = pruneBucket * bucketSpan * MS_IN_SECOND;
                ++emptyBucketCount;
            }
        }
    }

    public long getBucketCount() {
        return bucketCount;
    }

    public long getEmptyBucketCount() {
        return emptyBucketCount;
    }

    public Date getLatestEmptyBucketTime() {
        return latestEmptyBucketTime > 0 ? new Date(latestEmptyBucketTime) : null;
    }

    public long getSparseBucketCount() {
        return sparseBucketCount;
    }

    public Date getLatestSparseBucketTime() {
        return latestSparseBucketTime > 0 ? new Date(latestSparseBucketTime) : null;
    }
    
    /**
     * Resets counts,
     * 
     * Note: This does not reset the inner state for e.g. sparse bucket
     * detection.
     *
     */
    public void resetCounts() {
        bucketCount = 0;
        emptyBucketCount = 0;
        sparseBucketCount = 0;
    }
}
