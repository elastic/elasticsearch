/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.stashWithOrigin;


/**
 * This class will search the buckets and indices over a given window to determine if any data is missing
 */
public class DelayedDataDetector {

    private static final String DATE_BUCKETS = "date_buckets";
    private final long bucketSpan;
    private final long window;
    private final DatafeedConfig datafeedConfig;
    private final Client client;
    private final Job job;

    public DelayedDataDetector(Job job, DatafeedConfig datafeedConfig, TimeValue window, Client client) {
        this.job = job;
        this.bucketSpan = job.getAnalysisConfig().getBucketSpan().millis();
        this.datafeedConfig = datafeedConfig;
        long windowMillis = window.millis();
        if (windowMillis < bucketSpan) {
            throw new IllegalArgumentException("[window] must be greater or equal to the [bucket_span]");
        }
        this.window = windowMillis;
        this.client = client;
    }

    public List<BucketWithMissingData> missingData(long latestFinalizedBucket) {
        final long end = Intervals.alignToFloor(latestFinalizedBucket, bucketSpan);
        final long start = Intervals.alignToFloor(latestFinalizedBucket - window, bucketSpan);
        List<Bucket> finalizedBuckets = checkBucketEvents(start, end);
        Map<Long, Long> indexedData = checkTrueData(start, end);
        return finalizedBuckets.stream()
            .filter(bucket -> calculateMissing(indexedData, bucket) > 0)
            .map(bucket -> BucketWithMissingData.fromMissingAndBucket(calculateMissing(indexedData, bucket), bucket))
            .collect(Collectors.toList());
    }

    private List<Bucket> checkBucketEvents(long start, long end) {
        GetBucketsAction.Request request = new GetBucketsAction.Request(job.getId());
        request.setStart(Long.toString(start));
        request.setEnd(Long.toString(end));
        request.setExcludeInterim(true);
        request.setPageParams(new PageParams(0, (int)((end - start)/bucketSpan)));

        try (ThreadContext.StoredContext ignore = stashWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN)) {
            GetBucketsAction.Response response = client.execute(GetBucketsAction.INSTANCE, request).actionGet();
            return response.getBuckets().results();
        }
    }

    private Map<Long, Long> checkTrueData(long start, long end) {
        String timeField = job.getDataDescription().getTimeField();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .size(0)
            .aggregation(new DateHistogramAggregationBuilder(DATE_BUCKETS).interval(bucketSpan).field(timeField))
            .query(ExtractorUtils.wrapInTimeRangeQuery(datafeedConfig.getQuery(), timeField, start, end));

        SearchRequest searchRequest = new SearchRequest(datafeedConfig.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        try (ThreadContext.StoredContext ignore = stashWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN)) {
            SearchResponse response = client.execute(SearchAction.INSTANCE, searchRequest).actionGet();
            List<? extends Histogram.Bucket> buckets = ((Histogram)response.getAggregations().get(DATE_BUCKETS)).getBuckets();
            Map<Long, Long> hashMap = new HashMap<>(buckets.size());
            for (Histogram.Bucket bucket : buckets) {
                long bucketTime = toHistogramKeyToEpoch(bucket.getKey());
                if (bucketTime < 0) {
                        throw new IllegalStateException("Histogram key [" + bucket.getKey() + "] cannot be converted to a timestamp");
                }
                hashMap.put(bucketTime, bucket.getDocCount());
            }
            return hashMap;
        }
    }

    private static long toHistogramKeyToEpoch(Object key) {
        if (key instanceof DateTime) {
            return ((DateTime)key).getMillis();
        } else if (key instanceof Double) {
            return ((Double)key).longValue();
        } else if (key instanceof Long){
            return (Long)key;
        } else {
            return -1L;
        }
    }

    private static long calculateMissing(Map<Long, Long> indexedData, Bucket bucket) {
        return indexedData.getOrDefault(bucket.getEpoch() * 1000, 0L) - bucket.getEventCount();
    }

    public static class BucketWithMissingData {

        private final long missingData;
        private final Bucket bucket;

        static BucketWithMissingData fromMissingAndBucket(long missingData, Bucket bucket) {
            return new BucketWithMissingData(missingData, bucket);
        }

        private BucketWithMissingData(long missingData, Bucket bucket) {
           this.missingData = missingData;
           this.bucket = bucket;
        }

        public Bucket getBucket() {
            return bucket;
        }

        public long getMissingData() {
            return missingData;
        }
    }
}
