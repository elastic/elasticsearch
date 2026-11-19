/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class SearchBucketEventCounterTests extends ESTestCase {

    private static final long BUCKET_SPAN_MS = 60_000L;
    private static final long START_MS = 120_000L;
    private static final long END_MS = 600_000L;
    private static final String TIME_FIELD = "time";
    private static final String HISTOGRAM_NAME = "date_buckets";

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testSearchResponseWithoutAggregationsShouldYieldNoCounts() {
        // A search that has no shard results to reduce returns a response with null aggregations, even though aggregations were requested
        SearchResponse response = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
        assertNull(response.getAggregations());

        assertThat(countWith(response, new AtomicReference<>()), anEmptyMap());
    }

    public void testSearchResponseWithoutDateBucketsShouldYieldNoCounts() {
        SearchResponse response = SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS)
            .aggregations(InternalAggregations.EMPTY)
            .build();

        assertThat(countWith(response, new AtomicReference<>()), anEmptyMap());
    }

    public void testBucketKeysShouldBeConvertedToEpochMillis() {
        long zonedMs = 180_000L;
        long longMs = 240_000L;
        long doubleMs = 300_000L;
        SearchResponse response = histogramResponse(
            histogramBucket(Instant.ofEpochMilli(zonedMs).atZone(ZoneOffset.UTC), 3L),
            histogramBucket(longMs, 5L),
            histogramBucket((double) doubleMs, 7L)
        );

        Map<Long, Long> counts = countWith(response, new AtomicReference<>());

        assertThat(counts, equalTo(Map.of(zonedMs, 3L, longMs, 5L, doubleMs, 7L)));
    }

    public void testBucketKeyThatIsNotATimestampShouldFail() {
        SearchResponse response = histogramResponse(histogramBucket("not-a-timestamp", 1L));

        expectThrows(IllegalStateException.class, () -> countWith(response, new AtomicReference<>()));
    }

    public void testNegativeBucketKeyShouldFail() {
        SearchResponse response = histogramResponse(histogramBucket(-1L, 1L));

        expectThrows(IllegalStateException.class, () -> countWith(response, new AtomicReference<>()));
    }

    public void testSearchShouldHistogramTheDatafeedQueryOverTheRequestedRange() {
        AtomicReference<SearchRequest> captured = new AtomicReference<>();
        SearchResponse response = histogramResponse();
        QueryBuilder datafeedQuery = QueryBuilders.termQuery("host", "a");
        Map<String, Object> runtimeMappings = Map.of("rt", Map.of("type", "keyword"));
        SearchBucketEventCounter counter = new SearchBucketEventCounter(
            "job",
            BUCKET_SPAN_MS,
            TIME_FIELD,
            datafeedQuery,
            new String[] { "index-a", "index-b" },
            IndicesOptions.strictExpandOpen(),
            runtimeMappings,
            respondingWith(response, captured)
        );

        try {
            counter.currentBucketEventCounts(START_MS, END_MS);
        } finally {
            response.decRef();
        }

        SearchRequest request = captured.get();
        assertThat(request.indices(), equalTo(new String[] { "index-a", "index-b" }));
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.strictExpandOpen()));
        assertThat(request.source().size(), equalTo(0));
        assertThat(request.source().runtimeMappings(), equalTo(runtimeMappings));

        BoolQueryBuilder query = (BoolQueryBuilder) request.source().query();
        assertThat(query.filter().get(0), equalTo(datafeedQuery));
        assertThat(query.filter().get(1), instanceOf(RangeQueryBuilder.class));
        RangeQueryBuilder range = (RangeQueryBuilder) query.filter().get(1);
        assertThat(range.fieldName(), equalTo(TIME_FIELD));
        assertThat(range.from(), equalTo(START_MS));
        assertThat(range.to(), equalTo(END_MS));

        DateHistogramAggregationBuilder histogram = (DateHistogramAggregationBuilder) request.source()
            .aggregations()
            .getAggregatorFactories()
            .iterator()
            .next();
        assertThat(histogram.getName(), equalTo(HISTOGRAM_NAME));
        assertThat(histogram.field(), equalTo(TIME_FIELD));
        assertThat(histogram.getFixedInterval().toString(), equalTo(BUCKET_SPAN_MS + "ms"));
    }

    private Map<Long, Long> countWith(SearchResponse response, AtomicReference<SearchRequest> capturedRequest) {
        try {
            return counter(respondingWith(response, capturedRequest)).currentBucketEventCounts(START_MS, END_MS);
        } finally {
            response.decRef();
        }
    }

    private SearchBucketEventCounter counter(Client client) {
        return new SearchBucketEventCounter(
            "job",
            BUCKET_SPAN_MS,
            TIME_FIELD,
            QueryBuilders.matchAllQuery(),
            new String[] { "index" },
            IndicesOptions.lenientExpandOpen(),
            Map.of(),
            client
        );
    }

    private Client respondingWith(SearchResponse searchResponse, AtomicReference<SearchRequest> capturedRequest) {
        return new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action == TransportSearchAction.TYPE) {
                    capturedRequest.set((SearchRequest) request);
                    listener.onResponse((Response) searchResponse);
                } else {
                    throw new AssertionError("unexpected action [" + action.name() + "]");
                }
            }
        };
    }

    private static SearchResponse histogramResponse(Histogram.Bucket... buckets) {
        InternalHistogram histogram = mock(InternalHistogram.class);
        doReturn(HISTOGRAM_NAME).when(histogram).getName();
        doReturn(List.of(buckets)).when(histogram).getBuckets();
        return SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS)
            .aggregations(InternalAggregations.from(List.of(histogram)))
            .build();
    }

    private static Histogram.Bucket histogramBucket(Object key, long docCount) {
        Histogram.Bucket bucket = mock(Histogram.Bucket.class);
        doReturn(key).when(bucket).getKey();
        doReturn(docCount).when(bucket).getDocCount();
        return bucket;
    }
}
