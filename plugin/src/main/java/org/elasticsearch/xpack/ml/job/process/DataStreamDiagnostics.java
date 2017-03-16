/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.apache.lucene.util.Counter;

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

    private final DataCountsReporter dataCountsReporter;
    private final Logger logger;

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
     */
    private final SortedMap<Long, Counter> movingBucketHistogram = new TreeMap<>();

    private final long bucketSpan;
    private final long latency;
    private long movingBucketCount = 0;
    private long lastReportedBucket = -1;

    public DataStreamDiagnostics(DataCountsReporter dataCountsReporter, AnalysisConfig analysisConfig, Logger logger) {
        this.dataCountsReporter = dataCountsReporter;
        this.logger = logger;
        bucketSpan = analysisConfig.getBucketSpan().seconds();
        latency = analysisConfig.getLatency().seconds();
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
        flush(movingBucketHistogram.lastKey() + 1);
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
        if (lastReportedBucket == -1) {
            lastReportedBucket = bucket - 1;
        }

        // flush all bucket out of the window
        flush(bucketHistogramStartBucket);
    }

    /**
     * Flush Bucket reporting till the given bucket.
     * 
     * @param bucketTimeStamp
     *            The timestamp of the last bucket that can be flushed.
     */
    private void flush(long bucketTimeStamp) {

        // check for a longer period of empty buckets
        long emptyBuckets = movingBucketHistogram.firstKey() - lastReportedBucket - 1;
        if (emptyBuckets > 0) {
            dataCountsReporter.reportBuckets(emptyBuckets);
            dataCountsReporter.reportEmptyBuckets(emptyBuckets, (movingBucketHistogram.firstKey() - 1) * bucketSpan * MS_IN_SECOND);
            lastReportedBucket = movingBucketHistogram.firstKey() - 1;
        }

        // calculate the average number of data points in a bucket based on the
        // current history
        double averageBucketSize = (float) movingBucketCount / movingBucketHistogram.size();

        // prune all buckets that can be flushed
        long lastBucketSparsityCheck = Math.min(bucketTimeStamp, movingBucketHistogram.lastKey());

        for (long pruneBucket = movingBucketHistogram.firstKey(); pruneBucket < lastBucketSparsityCheck; ++pruneBucket) {

            Counter bucketSizeHolder = movingBucketHistogram.remove(pruneBucket);
            long bucketSize = bucketSizeHolder != null ? bucketSizeHolder.get() : 0L;

            logger.debug("Checking bucket {} compare sizes, this bucket: {} average: {}", pruneBucket, bucketSize, averageBucketSize);
            dataCountsReporter.reportBucket();
            lastReportedBucket = pruneBucket;

            // substract bucketSize from the counter
            movingBucketCount -= bucketSize;

            // check if bucket is empty
            if (bucketSize == 0L) {
                dataCountsReporter.reportEmptyBucket(pruneBucket * bucketSpan * MS_IN_SECOND);

                // do not do sparse analysis on an empty bucket
                continue;
            }

            // simplistic way to calculate data sparsity, just take the log and
            // check the difference
            double logAverageBucketSize = Math.log(averageBucketSize);
            double logBucketSize = Math.log(bucketSize);
            double sparsityScore = logAverageBucketSize - logBucketSize;

            if (sparsityScore > DATA_SPARSITY_THRESHOLD) {
                logger.debug("Sparse bucket {}, this bucket: {} average: {}, sparsity score: {}", pruneBucket, bucketSize,
                        averageBucketSize, sparsityScore);
                dataCountsReporter.reportSparseBucket(pruneBucket * bucketSpan * MS_IN_SECOND);
            }
        }

        // prune the rest if necessary
        for (long pruneBucket = lastBucketSparsityCheck; pruneBucket < bucketTimeStamp; ++pruneBucket) {
            Counter bucketSizeHolder = movingBucketHistogram.remove(pruneBucket);
            long bucketSize = bucketSizeHolder != null ? bucketSizeHolder.get() : 0L;

            dataCountsReporter.reportBucket();
            lastReportedBucket = pruneBucket;

            // substract bucketSize from the counter
            movingBucketCount -= bucketSize;

            // check if bucket is empty
            if (bucketSize == 0L) {
                dataCountsReporter.reportEmptyBucket(pruneBucket * bucketSpan * MS_IN_SECOND);
            }
        }
    }

}
