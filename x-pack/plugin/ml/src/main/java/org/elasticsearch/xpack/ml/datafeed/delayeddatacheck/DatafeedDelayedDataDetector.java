/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Compares the job's finalized bucket event counts over a given window with the counts that a {@link BucketEventCounter}
 * reports for the datafeed source now, to determine if any data is missing.
 */
public class DatafeedDelayedDataDetector implements DelayedDataDetector {

    private final long bucketSpan;
    private final long window;
    private final String jobId;
    private final Client client;
    private final BucketEventCounter bucketEventCounter;

    DatafeedDelayedDataDetector(long bucketSpan, long window, String jobId, Client client, BucketEventCounter bucketEventCounter) {
        this.bucketSpan = bucketSpan;
        this.window = window;
        this.jobId = jobId;
        this.client = client;
        this.bucketEventCounter = Objects.requireNonNull(bucketEventCounter);
    }

    // visible for testing
    BucketEventCounter getBucketEventCounter() {
        return bucketEventCounter;
    }

    /**
     * Returns finalized buckets in {@code [latestFinalizedBucketMs - window, latestFinalizedBucketMs)} whose current event count
     * exceeds their finalized event count. Blocks; run it on an appropriate thread pool.
     */
    @Override
    public List<BucketWithMissingData> detectMissingData(long latestFinalizedBucketMs) {
        final long end = Intervals.alignToFloor(latestFinalizedBucketMs, bucketSpan);
        final long start = Intervals.alignToFloor(latestFinalizedBucketMs - window, bucketSpan);

        if (end <= start) {
            return Collections.emptyList();
        }

        List<Bucket> finalizedBuckets = checkBucketEvents(start, end);
        Map<Long, Long> currentCounts = bucketEventCounter.currentBucketEventCounts(start, end);
        return finalizedBuckets.stream()
            // We only care about the situation when data is added to the indices
            // Older data could have been removed from the indices, and should not be considered "missing data"
            .filter(bucket -> calculateMissing(currentCounts, bucket) > 0)
            .map(bucket -> BucketWithMissingData.fromMissingAndBucket(calculateMissing(currentCounts, bucket), bucket))
            .collect(Collectors.toList());
    }

    @Override
    public long getWindow() {
        return window;
    }

    private List<Bucket> checkBucketEvents(long start, long end) {
        GetBucketsAction.Request request = new GetBucketsAction.Request(jobId);
        request.setStart(Long.toString(start));
        request.setEnd(Long.toString(end));
        request.setSort("timestamp");
        request.setDescending(false);
        request.setExcludeInterim(true);
        request.setPageParams(new PageParams(0, (int) ((end - start) / bucketSpan)));

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            GetBucketsAction.Response response = client.execute(GetBucketsAction.INSTANCE, request).actionGet();
            return response.getBuckets().results();
        }
    }

    private static long calculateMissing(Map<Long, Long> currentCounts, Bucket bucket) {
        return currentCounts.getOrDefault(bucket.getEpoch() * 1000, 0L) - bucket.getEventCount();
    }
}
