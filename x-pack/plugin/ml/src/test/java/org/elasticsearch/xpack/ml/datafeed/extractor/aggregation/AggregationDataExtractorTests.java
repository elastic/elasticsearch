/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.Term;
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

import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createHistogramBucket;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createMax;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createTerms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AggregationDataExtractorTests extends ESTestCase {

    private Client client;
    private String jobId;
    private String timeField;
    private Set<String> fields;
    private List<String> indices;
    private QueryBuilder query;
    private AggregatorFactories.Builder aggs;
    private DatafeedTimingStatsReporter timingStatsReporter;
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
        aggs = new AggregatorFactories.Builder().addAggregator(
            AggregationBuilders.histogram("time")
                .field("time")
                .interval(1000)
                .subAggregation(
                    AggregationBuilders.terms("airline")
                        .field("airline")
                        .subAggregation(AggregationBuilders.avg("responsetime").field("responsetime"))
                )
        );
        runtimeMappings = Collections.emptyMap();
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
    }

    public void testExtraction() throws IOException {
        List<InternalHistogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(
                1000L,
                3,
                Arrays.asList(
                    createMax("time", 1999),
                    createTerms("airline", new Term("a", 1, "responsetime", 11.0), new Term("b", 2, "responsetime", 12.0))
                )
            ),
            createHistogramBucket(2000L, 0, Collections.emptyList()),
            createHistogramBucket(
                3000L,
                7,
                Arrays.asList(
                    createMax("time", 3999),
                    createTerms("airline", new Term("c", 4, "responsetime", 31.0), new Term("b", 3, "responsetime", 32.0))
                )
            )
        );

        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, 4000L), timingStatsReporter);

        ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse("time", histogramBuckets));
        when(client.execute(eq(TransportSearchAction.TYPE), searchRequestCaptor.capture())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(1000L, 4000L)));
        Optional<InputStream> stream = result.data();
        assertThat(stream.isPresent(), is(true));
        String expectedStream = """
            {"time":1999,"airline":"a","responsetime":11.0,"doc_count":1} \
            {"time":1999,"airline":"b","responsetime":12.0,"doc_count":2} \
            {"time":3999,"airline":"c","responsetime":31.0,"doc_count":4} \
            {"time":3999,"airline":"b","responsetime":32.0,"doc_count":3}""";
        assertThat(asString(stream.get()), equalTo(expectedStream));
        assertThat(extractor.hasNext(), is(false));

        String searchRequest = searchRequestCaptor.getValue().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"size\":0"));
        assertThat(
            searchRequest,
            containsString(
                "\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}},"
                    + "{\"range\":{\"time\":{\"gte\":0,\"lt\":4000,"
                    + "\"format\":\"epoch_millis\",\"boost\":1.0}}}]"
            )
        );
        assertThat(
            searchRequest,
            stringContainsInOrder(Arrays.asList("aggregations", "histogram", "time", "terms", "airline", "avg", "responsetime"))
        );
    }

    public void testExtractionGivenResponseHasNullAggs() throws IOException {
        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, 2000L), timingStatsReporter);

        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse(null));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(client).execute(eq(TransportSearchAction.TYPE), any());
    }

    public void testExtractionGivenResponseHasEmptyAggs() throws IOException {
        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, 2000L), timingStatsReporter);

        InternalAggregations emptyAggs = AggregationTestUtils.createAggs(Collections.emptyList());
        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse(emptyAggs));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(client).execute(eq(TransportSearchAction.TYPE), any());
    }

    public void testExtractionGivenResponseHasEmptyHistogramAgg() throws IOException {
        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, 2000L), timingStatsReporter);

        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse("time", Collections.emptyList()));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(client).execute(eq(TransportSearchAction.TYPE), any());
    }

    public void testExtractionGivenResponseHasMultipleTopLevelAggs() {
        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, 2000L), timingStatsReporter);

        InternalHistogram histogram1 = mock(InternalHistogram.class);
        when(histogram1.getName()).thenReturn("hist_1");
        InternalHistogram histogram2 = mock(InternalHistogram.class);
        when(histogram2.getName()).thenReturn("hist_2");

        InternalAggregations aggs = AggregationTestUtils.createAggs(Arrays.asList(histogram1, histogram2));
        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse(aggs));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, extractor::next);
        assertThat(e.getMessage(), containsString("Multiple top level aggregations not supported; found: [hist_1, hist_2]"));
    }

    public void testExtractionGivenCancelBeforeNext() {
        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, 4000L), timingStatsReporter);

        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse("time", Collections.emptyList()));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        extractor.cancel();
        assertThat(extractor.hasNext(), is(false));
    }

    public void testExtractionGivenCancelHalfWay() throws IOException {
        int buckets = 1200;
        List<InternalHistogram.Bucket> histogramBuckets = new ArrayList<>(buckets);
        long timestamp = 1000;
        for (int i = 0; i < buckets; i++) {
            histogramBuckets.add(
                createHistogramBucket(
                    timestamp,
                    3,
                    Arrays.asList(
                        createMax("time", timestamp),
                        createTerms("airline", new Term("c", 4, "responsetime", 31.0), new Term("b", 3, "responsetime", 32.0))
                    )
                )
            );
            timestamp += 1000L;
        }

        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, timestamp + 1), timingStatsReporter);

        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSearchResponse("time", histogramBuckets));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);

        assertThat(extractor.hasNext(), is(true));
        assertThat(countMatches('{', asString(extractor.next().data().get())), equalTo(2400L));
        histogramBuckets = new ArrayList<>(buckets);
        for (int i = 0; i < buckets; i++) {
            histogramBuckets.add(
                createHistogramBucket(
                    timestamp,
                    3,
                    Arrays.asList(
                        createMax("time", timestamp),
                        createTerms("airline", new Term("c", 4, "responsetime", 31.0), new Term("b", 3, "responsetime", 32.0))
                    )
                )
            );
            timestamp += 1000L;
        }
        searchResponse = toActionFuture(createSearchResponse("time", histogramBuckets));
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenReturn(searchResponse);
        extractor.cancel();
        assertThat(extractor.hasNext(), is(false));
        assertThat(extractor.isCancelled(), is(true));

        verify(client).execute(eq(TransportSearchAction.TYPE), any());
    }

    public void testExtractionGivenSearchResponseHasError() {
        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, 2000L), timingStatsReporter);
        when(client.execute(eq(TransportSearchAction.TYPE), any())).thenThrow(
            new SearchPhaseExecutionException("phase 1", "boom", ShardSearchFailure.EMPTY_ARRAY)
        );

        assertThat(extractor.hasNext(), is(true));
        expectThrows(SearchPhaseExecutionException.class, extractor::next);
    }

    public void testGetSummary() {
        AggregationDataExtractor extractor = new AggregationDataExtractor(client, createContext(1000L, 2300L), timingStatsReporter);

        ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        ActionFuture<SearchResponse> searchResponse = toActionFuture(createSummaryResponse(1001L, 2299L, 10L));
        when(client.execute(eq(TransportSearchAction.TYPE), searchRequestCaptor.capture())).thenReturn(searchResponse);

        DataExtractor.DataSummary summary = extractor.getSummary();
        assertThat(summary.earliestTime(), equalTo(1001L));
        assertThat(summary.latestTime(), equalTo(2299L));
        assertThat(summary.totalHits(), equalTo(10L));

        String searchRequest = searchRequestCaptor.getValue().toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"size\":0"));
        assertThat(
            searchRequest,
            containsString(
                "\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}},"
                    + "{\"range\":{\"time\":{\"gte\":1000,\"lt\":2300,"
                    + "\"format\":\"epoch_millis\",\"boost\":1.0}}}]"
            )
        );
        assertThat(
            searchRequest,
            containsString(
                "\"aggregations\":{\"earliest_time\":{\"min\":{\"field\":\"time\"}}," + "\"latest_time\":{\"max\":{\"field\":\"time\"}}}}"
            )
        );
        assertThat(searchRequest, not(containsString("\"track_total_hits\":false")));
        assertThat(searchRequest, not(containsString("\"sort\"")));
    }

    private AggregationDataExtractorContext createContext(long start, long end) {
        return new AggregationDataExtractorContext(
            jobId,
            timeField,
            fields,
            indices,
            query,
            aggs,
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

    private SearchResponse createSearchResponse(String histogramName, List<InternalHistogram.Bucket> histogramBuckets) {
        InternalHistogram histogram = mock(InternalHistogram.class);
        when(histogram.getName()).thenReturn(histogramName);
        when(histogram.getBuckets()).thenReturn(histogramBuckets);

        InternalAggregations searchAggs = AggregationTestUtils.createAggs(Collections.singletonList(histogram));
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

    private SearchResponse createSummaryResponse(long start, long end, long totalHits) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(
            new SearchHits(SearchHits.EMPTY, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1)
        );
        when(searchResponse.getAggregations()).thenReturn(
            InternalAggregations.from(List.of(new Min("earliest_time", start, null, null), new Max("latest_time", end, null, null)))
        );
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
