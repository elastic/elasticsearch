/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorUtils;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * This class will search the buckets and indices over a given window to determine if any data is missing
 */
public class DatafeedDelayedDataDetector implements DelayedDataDetector {

    private static final Logger logger = LogManager.getLogger(DatafeedDelayedDataDetector.class);

    private static final String DATE_BUCKETS = "date_buckets";

    private final long bucketSpan;
    private final long window;
    private final Client client;
    private final String timeField;
    private final String jobId;
    private final QueryBuilder datafeedQuery;
    private final String[] datafeedIndices;
    private final IndicesOptions indicesOptions;
    private final Map<String, Object> runtimeMappings;

    DatafeedDelayedDataDetector(
        long bucketSpan,
        long window,
        String jobId,
        String timeField,
        QueryBuilder datafeedQuery,
        String[] datafeedIndices,
        IndicesOptions indicesOptions,
        Map<String, Object> runtimeMappings,
        Client client
    ) {
        this.bucketSpan = bucketSpan;
        this.window = window;
        this.jobId = jobId;
        this.timeField = timeField;
        this.datafeedQuery = datafeedQuery;
        this.datafeedIndices = datafeedIndices;
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
        this.runtimeMappings = Objects.requireNonNull(runtimeMappings);
        this.client = client;
    }

    /**
     * This method looks at the {@link DatafeedDelayedDataDetector#datafeedIndices}
     * from {@code latestFinalizedBucket - window} to {@code latestFinalizedBucket} and compares the document counts with the
     * {@link DatafeedDelayedDataDetector#jobId}'s finalized buckets' event counts.
     *
     * It is done synchronously, and can block for a considerable amount of time, it should only be executed within the appropriate
     * thread pool.
     *
     * @param latestFinalizedBucketMs The latest finalized bucket timestamp in milliseconds, signifies the end of the time window check
     * @return A List of {@link BucketWithMissingData} objects that contain each bucket with the current number of missing docs
     */
    @Override
    public List<BucketWithMissingData> detectMissingData(long latestFinalizedBucketMs) {
        final long end = Intervals.alignToFloor(latestFinalizedBucketMs, bucketSpan);
        final long start = Intervals.alignToFloor(latestFinalizedBucketMs - window, bucketSpan);

        if (end <= start) {
            return Collections.emptyList();
        }

        List<Bucket> finalizedBuckets = checkBucketEvents(start, end);
        Map<Long, Long> indexedData = checkCurrentBucketEventCount(start, end);
        return finalizedBuckets.stream()
            // We only care about the situation when data is added to the indices
            // Older data could have been removed from the indices, and should not be considered "missing data"
            .filter(bucket -> calculateMissing(indexedData, bucket) > 0)
            .map(bucket -> BucketWithMissingData.fromMissingAndBucket(calculateMissing(indexedData, bucket), bucket))
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

    private Map<Long, Long> checkCurrentBucketEventCount(long start, long end) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0)
            .aggregation(
                new DateHistogramAggregationBuilder(DATE_BUCKETS).fixedInterval(new DateHistogramInterval(bucketSpan + "ms"))
                    .field(timeField)
            )
            .query(DataExtractorUtils.wrapInTimeRangeQuery(datafeedQuery, timeField, start, end))
            .runtimeMappings(runtimeMappings);

        SearchRequest searchRequest = new SearchRequest(datafeedIndices).source(searchSourceBuilder).indicesOptions(indicesOptions);
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            SearchResponse searchResponse = client.execute(TransportSearchAction.TYPE, searchRequest).actionGet();
            try {
                Histogram histogram = searchResponse.getAggregations().get(DATE_BUCKETS);
                if (histogram == null) {
                    logger.warn("[{}] Delayed data check failed with missing aggregation in search response", jobId);
                    return Collections.emptyMap();
                }
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                Map<Long, Long> hashMap = Maps.newMapWithExpectedSize(buckets.size());
                for (Histogram.Bucket bucket : buckets) {
                    long bucketTime = toHistogramKeyToEpoch(bucket.getKey());
                    if (bucketTime < 0) {
                        throw new IllegalStateException("Histogram key [" + bucket.getKey() + "] cannot be converted to a timestamp");
                    }
                    hashMap.put(bucketTime, bucket.getDocCount());
                }
                return hashMap;
            } finally {
                searchResponse.decRef();
            }
        }
    }

    private static long toHistogramKeyToEpoch(Object key) {
        if (key instanceof ZonedDateTime zdt) {
            return zdt.toInstant().toEpochMilli();
        } else if (key instanceof Double doubleValue) {
            return doubleValue.longValue();
        } else if (key instanceof Long longValue) {
            return longValue;
        } else {
            return -1L;
        }
    }

    private static long calculateMissing(Map<Long, Long> indexedData, Bucket bucket) {
        return indexedData.getOrDefault(bucket.getEpoch() * 1000, 0L) - bucket.getEventCount();
    }
}
