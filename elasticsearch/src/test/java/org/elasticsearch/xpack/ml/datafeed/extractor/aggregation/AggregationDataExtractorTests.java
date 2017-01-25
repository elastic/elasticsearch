/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.Term;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createHistogramBucket;
import static org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationTestUtils.createTerms;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationDataExtractorTests extends ESTestCase {

    private Client client;
    private List<SearchRequestBuilder> capturedSearchRequests;
    private String jobId;
    private String timeField;
    private List<String> types;
    private List<String> indexes;
    private QueryBuilder query;
    private AggregatorFactories.Builder aggs;

    private class TestDataExtractor extends AggregationDataExtractor {

        private SearchResponse nextResponse;

        public TestDataExtractor(long start, long end) {
            super(client, createContext(start, end));
        }

        @Override
        protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
            capturedSearchRequests.add(searchRequestBuilder);
            return nextResponse;
        }

        void setNextResponse(SearchResponse searchResponse) {
            nextResponse = searchResponse;
        }
    }

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        capturedSearchRequests = new ArrayList<>();
        jobId = "test-job";
        timeField = "time";
        indexes = Arrays.asList("index-1", "index-2");
        types = Arrays.asList("type-1", "type-2");
        query = QueryBuilders.matchAllQuery();
        aggs = new AggregatorFactories.Builder()
                .addAggregator(AggregationBuilders.histogram("time").field("time").subAggregation(
                        AggregationBuilders.terms("airline").field("airline").subAggregation(
                                AggregationBuilders.avg("responsetime").field("responsetime"))));
    }

    public void testExtraction() throws IOException {
        List<Histogram.Bucket> histogramBuckets = Arrays.asList(
            createHistogramBucket(1000L, 3, Arrays.asList(
                    createTerms("airline", new Term("a", 1, "responsetime", 11.0), new Term("b", 2, "responsetime", 12.0)))),
            createHistogramBucket(2000L, 0, Arrays.asList()),
            createHistogramBucket(3000L, 7, Arrays.asList(
                    createTerms("airline", new Term("c", 4, "responsetime", 31.0), new Term("b", 3, "responsetime", 32.0))))
        );

        TestDataExtractor extractor = new TestDataExtractor(1000L, 4000L);

        SearchResponse response = createSearchResponse("time", histogramBuckets);
        extractor.setNextResponse(response);

        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        String expectedStream = "{\"time\":1000,\"airline\":\"a\",\"responsetime\":11.0,\"doc_count\":1} "
                + "{\"time\":1000,\"airline\":\"b\",\"responsetime\":12.0,\"doc_count\":2} "
                + "{\"time\":3000,\"airline\":\"c\",\"responsetime\":31.0,\"doc_count\":4} "
                + "{\"time\":3000,\"airline\":\"b\",\"responsetime\":32.0,\"doc_count\":3}";
        assertThat(asString(stream.get()), equalTo(expectedStream));
        assertThat(extractor.hasNext(), is(false));
        assertThat(capturedSearchRequests.size(), equalTo(1));

        String searchRequest = capturedSearchRequests.get(0).toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"size\":0"));
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}}," +
                "{\"range\":{\"time\":{\"from\":1000,\"to\":4000,\"include_lower\":true,\"include_upper\":false," +
                "\"format\":\"epoch_millis\",\"boost\":1.0}}}]"));
        assertThat(searchRequest, containsString("\"sort\":[{\"time\":{\"order\":\"asc\"}}]"));
        assertThat(searchRequest,
                stringContainsInOrder(Arrays.asList("aggregations", "histogram", "time", "terms", "airline", "avg", "responsetime")));
    }

    public void testExtractionGivenCancelBeforeNext() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 4000L);
        SearchResponse response = createSearchResponse("time", Collections.emptyList());
        extractor.setNextResponse(response);

        extractor.cancel();
        assertThat(extractor.hasNext(), is(false));
    }

    public void testExtractionGivenSearchResponseHasError() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);
        extractor.setNextResponse(createErrorResponse());

        assertThat(extractor.hasNext(), is(true));
        expectThrows(IOException.class, () -> extractor.next());
    }

    private AggregationDataExtractorContext createContext(long start, long end) {
        return new AggregationDataExtractorContext(jobId, timeField, indexes, types, query, aggs, start, end);
    }

    private SearchResponse createSearchResponse(String histogramName, List<Histogram.Bucket> histogramBuckets) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getScrollId()).thenReturn(randomAsciiOfLength(1000));

        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn(histogramName);
        when(histogram.getBuckets()).thenReturn(histogramBuckets);

        Aggregations searchAggs = mock(Aggregations.class);
        when(searchAggs.asList()).thenReturn(Arrays.asList(histogram));
        when(searchResponse.getAggregations()).thenReturn(searchAggs);
        return searchResponse;
    }

    private SearchResponse createErrorResponse() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR);
        return searchResponse;
    }

    private static String asString(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }
}
