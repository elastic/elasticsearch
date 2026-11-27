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
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorUtils;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/** Counts the documents per bucket in the datafeed indices with a date histogram search over the datafeed query. */
final class SearchBucketEventCounter implements BucketEventCounter {

    private static final Logger logger = LogManager.getLogger(SearchBucketEventCounter.class);

    private static final String DATE_BUCKETS = "date_buckets";

    private final String jobId;
    private final long bucketSpan;
    private final String timeField;
    private final QueryBuilder datafeedQuery;
    private final String[] datafeedIndices;
    private final IndicesOptions indicesOptions;
    private final Map<String, Object> runtimeMappings;
    private final Client client;

    SearchBucketEventCounter(
        String jobId,
        long bucketSpan,
        String timeField,
        QueryBuilder datafeedQuery,
        String[] datafeedIndices,
        IndicesOptions indicesOptions,
        Map<String, Object> runtimeMappings,
        Client client
    ) {
        this.jobId = jobId;
        this.bucketSpan = bucketSpan;
        this.timeField = timeField;
        this.datafeedQuery = datafeedQuery;
        this.datafeedIndices = datafeedIndices;
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
        this.runtimeMappings = Objects.requireNonNull(runtimeMappings);
        this.client = client;
    }

    // visible for testing
    IndicesOptions getIndicesOptions() {
        return indicesOptions;
    }

    @Override
    public Map<Long, Long> currentBucketEventCounts(long start, long end) {
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
                // The aggregations are null when the search had no shard results to reduce
                InternalAggregations aggregations = searchResponse.getAggregations();
                Histogram histogram = aggregations == null ? null : aggregations.get(DATE_BUCKETS);
                if (histogram == null) {
                    // We log search response here to get information about shards and hits which may be helpful while debugging.
                    // The size of the search response is small as we only log if the "date_buckets" aggregation is missing.
                    logger.warn("[{}] Delayed data check failed with missing aggregation in search response [{}]", jobId, searchResponse);
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
}
