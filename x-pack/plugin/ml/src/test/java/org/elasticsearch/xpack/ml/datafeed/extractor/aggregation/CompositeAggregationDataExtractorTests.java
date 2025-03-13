/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createAvg;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createCompositeBucket;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createMax;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeAggregationDataExtractorTests extends ESTestCase {

    private Client client;
    private String jobId;
    private String timeField;
    private Set<String> fields;
    private List<String> indices;
    private QueryBuilder query;
    private DatafeedTimingStatsReporter timingStatsReporter;
    private CompositeAggregationBuilder compositeAggregationBuilder;
    private AggregatedSearchRequestBuilder aggregatedSearchRequestBuilder;
    private Map<String, Object> runtimeMappings;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(mock(ThreadPool.class));
        when(client.threadPool().getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        jobId = "test-job";
        timeField = "time";
        fields = new HashSet<>();
        fields.addAll(Arrays.asList("time", "airline", "responsetime"));
        indices = Arrays.asList("index-1", "index-2");
        query = QueryBuilders.matchAllQuery();
        compositeAggregationBuilder = AggregationBuilders.composite(
            "buckets",
            Arrays.asList(
                new DateHistogramValuesSourceBuilder("time_bucket").field("time").fixedInterval(new DateHistogramInterval("1000ms")),
                new TermsValuesSourceBuilder("airline").field("airline")
            )
        )
            .size(10)
            .subAggregation(AggregationBuilders.max("time").field("time"))
            .subAggregation(AggregationBuilders.avg("responsetime").field("responsetime"));
        runtimeMappings = Collections.emptyMap();
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
        aggregatedSearchRequestBuilder = (searchSourceBuilder) -> new SearchRequestBuilder(client).setSource(searchSourceBuilder)
            .setAllowPartialSearchResults(false)
            .setIndices(indices.toArray(String[]::new));
    }

    public void testExtraction() throws IOException {
        List<InternalComposite.InternalBucket> compositeBucket = Arrays.asList(
            createCompositeBucket(
                1000L,
                "time_bucket",
                1,
                Arrays.asList(createMax("time", 1999), createAvg("responsetime", 11.0)),
                Collections.singletonList(Tuple.tuple("airline", "a"))
            ),
            createCompositeBucket(
                1000L,
                "time_bucket",
                2,
                Arrays.asList(createMax("time", 1999), createAvg("responsetime", 12.0)),
                Collections.singletonList(Tuple.tuple("airline", "b"))
            ),
            createCompositeBucket(2000L, "time_bucket", 0, Collections.emptyList(), Collections.emptyList()),
            createCompositeBucket(
                3000L,
                "time_bucket",
                4,
                Arrays.asList(createMax("time", 3999), createAvg("responsetime", 31.0)),
                Collections.singletonList(Tuple.tuple("airline", "c"))
            ),
            createCompositeBucket(
                3000L,
                "time_bucket",
                3,
                Arrays.asList(createMax("time", 3999), createAvg("responsetime", 32.0)),
                Collections.singletonList(Tuple.tuple("airline", "b"))
            )
        );

        CompositeAggregationDataExtractor extractor = new CompositeAggregationDataExtractor(
            compositeAggregationBuilder,
            client,
            createContext(1000L, 4000L),
            timingStatsReporter,
            aggregatedSearchRequestBuilder
        );

        ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        ActionFuture<SearchResponse> searchResponse = toActionFuture(
            createSearchResponse("buckets", compositeBucket, Map.of("time_bucket", 4000L, "airline", "d"))
        );
        when(client.execute(eq(TransportSearchAction.TYPE), searchRequestCaptor.capture())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(1000L, 4000L)));
        Optional<InputStream> stream = result.data();
        assertThat(stream.isPresent(), is(true));
        String expectedStream = """
            {"airline":"a","time":1999,"responsetime":11.0,"doc_count":1} \
            {"airline":"b","time":1999,"responsetime":12.0,"doc_count":2} \
            {"airline":"c","time":3999,"responsetime":31.0,"doc_count":4} \
            {"airline":"b","time":3999,"responsetime":32.0,"doc_count":3}""";
        assertThat(asString(stream.get()), equalTo(expectedStream));

        String searchRequest = searchRequestCaptor.getValue().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"size\":0"));
        assertThat(
            searchRequest,
            containsString(
                "\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}},"
                    + "{\"range\":{\"time\":{\"gte\":1000,\"lt\":4000,"
                    + "\"format\":\"epoch_millis\",\"boost\":1.0}}}]"
            )
        );
        assertThat(
            searchRequest,
            stringContainsInOrder(Arrays.asList("aggregations", "composite", "time", "terms", "airline", "avg", "responsetime"))
        );
    }

    public void testExtractionGivenResponseHasNullAggs() throws IOException {
        CompositeAggregationDataExtractor extractor = new CompositeAggregationDataExtractor(
            compositeAggregationBuilder,
            client,
            createContext(1000L, 2000L),
            timingStatsReporter,
            aggregatedSearchRequestBuilder
        );

        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse(null));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(client).execute(eq(TransportSearchAction.TYPE), any());
    }

    public void testExtractionGivenResponseHasEmptyAggs() throws IOException {
        CompositeAggregationDataExtractor extractor = new CompositeAggregationDataExtractor(
            compositeAggregationBuilder,
            client,
            createContext(1000L, 2000L),
            timingStatsReporter,
            aggregatedSearchRequestBuilder
        );

        InternalAggregations emptyAggs = AggregationTestUtils.createAggs(Collections.emptyList());
        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse(emptyAggs));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(client).execute(eq(TransportSearchAction.TYPE), any());
    }

    public void testExtractionGivenCancelBeforeNext() {
        CompositeAggregationDataExtractor extractor = new CompositeAggregationDataExtractor(
            compositeAggregationBuilder,
            client,
            createContext(1000L, 4000L),
            timingStatsReporter,
            aggregatedSearchRequestBuilder
        );

        ActionFuture<SearchResponse> searchResponse = toActionFuture(
            createSearchResponse("time", Collections.emptyList(), Collections.emptyMap())
        );
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        extractor.cancel();
        // Composite aggs should be true because we need to make sure the first search has occurred or not
        assertThat(extractor.hasNext(), is(true));
    }

    public void testExtractionCancelOnFirstPage() throws IOException {
        int numBuckets = 10;
        List<InternalComposite.InternalBucket> buckets = new ArrayList<>(numBuckets);
        long timestamp = 1000;
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(
                createCompositeBucket(
                    timestamp,
                    "time_bucket",
                    3,
                    Arrays.asList(createMax("time", randomLongBetween(timestamp, timestamp + 1000)), createAvg("responsetime", 32.0)),
                    Collections.singletonList(Tuple.tuple("airline", "c"))
                )
            );
        }

        CompositeAggregationDataExtractor extractor = new CompositeAggregationDataExtractor(
            compositeAggregationBuilder,
            client,
            createContext(1000L, timestamp + 1000 + 1),
            timingStatsReporter,
            aggregatedSearchRequestBuilder
        );

        ActionFuture<SearchResponse> searchResponse = toActionFuture(
            createSearchResponse("buckets", buckets, Map.of("time_bucket", 1000L, "airline", "d"))
        );
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        extractor.cancel();
        // We should have next right now as we have not yet determined if we have handled a page or not
        assertThat(extractor.hasNext(), is(true));
        // Should be empty
        assertThat(countMatches('{', asString(extractor.next().data().get())), equalTo(0L));
        // Determined that we were on the first page and ended
        assertThat(extractor.hasNext(), is(false));
    }

    public void testExtractionGivenCancelHalfWay() throws IOException {
        int numBuckets = 10;
        List<InternalComposite.InternalBucket> buckets = new ArrayList<>(numBuckets);
        long timestamp = 1000;
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(
                createCompositeBucket(
                    timestamp,
                    "time_bucket",
                    3,
                    Arrays.asList(createMax("time", randomLongBetween(timestamp, timestamp + 999)), createAvg("responsetime", 32.0)),
                    Collections.singletonList(Tuple.tuple("airline", "c"))
                )
            );
        }

        CompositeAggregationDataExtractor extractor = new CompositeAggregationDataExtractor(
            compositeAggregationBuilder,
            client,
            createContext(1000L, timestamp + 1000 + 1),
            timingStatsReporter,
            aggregatedSearchRequestBuilder
        );

        ActionFuture<SearchResponse> searchResponse = toActionFuture(
            createSearchResponse("buckets", buckets, Map.of("time_bucket", 1000L, "airline", "d"))
        );
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        assertThat(countMatches('{', asString(extractor.next().data().get())), equalTo(10L));
        buckets = new ArrayList<>(numBuckets);
        for (int i = 0; i < 6; i++) {
            buckets.add(
                createCompositeBucket(
                    timestamp,
                    "time_bucket",
                    3,
                    Arrays.asList(createMax("time", randomLongBetween(timestamp, timestamp + 999)), createAvg("responsetime", 32.0)),
                    Collections.singletonList(Tuple.tuple("airline", "c"))
                )
            );
        }
        timestamp += 1000;
        for (int i = 0; i < 4; i++) {
            buckets.add(
                createCompositeBucket(
                    timestamp,
                    "time_bucket",
                    3,
                    Arrays.asList(createMax("time", randomLongBetween(timestamp, timestamp + 999)), createAvg("responsetime", 32.0)),
                    Collections.singletonList(Tuple.tuple("airline", "c"))
                )
            );
        }

        searchResponse = toActionFuture(createSearchResponse("buckets", buckets, Map.of("time_bucket", 3000L, "airline", "a")));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        extractor.cancel();
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.isCancelled(), is(true));
        // Only the docs in the previous bucket before cancelling
        assertThat(countMatches('{', asString(extractor.next().data().get())), equalTo(6L));

        // Once we have handled the 6 remaining in that time bucket, we shouldn't finish the page and the extractor should end
        assertThat(extractor.hasNext(), is(false));

        verify(client, times(2)).execute(eq(TransportSearchAction.TYPE), any());
    }

    public void testExtractionGivenSearchResponseHasError() {
        CompositeAggregationDataExtractor extractor = new CompositeAggregationDataExtractor(
            compositeAggregationBuilder,
            client,
            createContext(1000L, 2000L),
            timingStatsReporter,
            aggregatedSearchRequestBuilder
        );

        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenThrow(
            new SearchPhaseExecutionException("phase 1", "boom", ShardSearchFailure.EMPTY_ARRAY)
        );

        assertThat(extractor.hasNext(), is(true));
        expectThrows(SearchPhaseExecutionException.class, extractor::next);
    }

    private CompositeAggregationDataExtractorContext createContext(long start, long end) {
        return new CompositeAggregationDataExtractorContext(
            jobId,
            timeField,
            fields,
            indices,
            query,
            compositeAggregationBuilder,
            "time_bucket",
            start,
            end,
            true,
            Collections.emptyMap(),
            SearchRequest.DEFAULT_INDICES_OPTIONS,
            runtimeMappings
        );
    }

    private <T> ActionFuture<T> toActionFuture(T t) {
        @SuppressWarnings("unchecked")
        ActionFuture<T> future = (ActionFuture<T>) mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(t);
        return future;
    }

    private SearchResponse createSearchResponse(
        String aggName,
        List<InternalComposite.InternalBucket> buckets,
        Map<String, Object> afterKey
    ) {
        InternalComposite compositeAggregation = mock(InternalComposite.class);
        when(compositeAggregation.getName()).thenReturn(aggName);
        when(compositeAggregation.afterKey()).thenReturn(afterKey);
        when(compositeAggregation.getBuckets()).thenReturn(buckets);

        InternalAggregations searchAggs = AggregationTestUtils.createAggs(Collections.singletonList(compositeAggregation));
        return createSearchResponse(searchAggs);
    }

    private SearchResponse createSearchResponse(InternalAggregations aggregations) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getScrollId()).thenReturn(randomAlphaOfLength(1000));
        when(searchResponse.getAggregations()).thenReturn(aggregations);
        when(searchResponse.getTook()).thenReturn(TimeValue.timeValueMillis(randomNonNegativeLong()));
        return searchResponse;
    }

    private static String asString(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    private static long countMatches(char c, String text) {
        return text.chars().filter(current -> current == c).count();
    }
}
