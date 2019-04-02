/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.diagnostics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;

import java.util.Date;

public class DataStreamDiagnostics {

    /**
     * Threshold to report potential sparsity problems.
     *
     * Sparsity score is calculated: log(average) - log(current)
     *
     * If score is above the threshold, bucket is reported as sparse bucket.
     */
    private static final int DATA_SPARSITY_THRESHOLD = 2;

    private static final Logger LOGGER = LogManager.getLogger(DataStreamDiagnostics.class);

    private final BucketDiagnostics bucketDiagnostics;

    private long bucketCount = 0;
    private long emptyBucketCount = 0;
    private long latestEmptyBucketTime = -1;
    private long sparseBucketCount = 0;
    private long latestSparseBucketTime = -1;

    public DataStreamDiagnostics(Job job, DataCounts dataCounts) {
        bucketDiagnostics = new BucketDiagnostics(job, dataCounts, createBucketFlushListener());
    }

    private BucketDiagnostics.BucketFlushListener createBucketFlushListener() {
        return (flushedBucketStartMs, flushedBucketCount) -> {
            ++bucketCount;
            if (flushedBucketCount == 0) {
                ++emptyBucketCount;
                latestEmptyBucketTime = flushedBucketStartMs;
            } else {
                // simplistic way to calculate data sparsity, just take the log and
                // check the difference
                double averageBucketSize = bucketDiagnostics.averageBucketCount();
                double logAverageBucketSize = Math.log(averageBucketSize);
                double logBucketSize = Math.log(flushedBucketCount);
                double sparsityScore = logAverageBucketSize - logBucketSize;

                if (sparsityScore > DATA_SPARSITY_THRESHOLD) {
                    LOGGER.debug("Sparse bucket {}, this bucket: {} average: {}, sparsity score: {}", flushedBucketStartMs,
                            flushedBucketCount, averageBucketSize, sparsityScore);
                    ++sparseBucketCount;
                    latestSparseBucketTime = flushedBucketStartMs;
                }
            }
        };
    }

    /**
     * Check record
     *
     * @param recordTimestampInMs
     *            The record timestamp in milliseconds since epoch
     */
    public void checkRecord(long recordTimestampInMs) {
        bucketDiagnostics.addRecord(recordTimestampInMs);
    }

    /**
     * Flush all counters, should be called at the end of the data stream
     */
    public void flush() {
        // flush all we know
        bucketDiagnostics.flush();
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
