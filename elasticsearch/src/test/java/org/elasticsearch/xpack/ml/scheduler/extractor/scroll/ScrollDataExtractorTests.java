/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler.extractor.scroll;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScrollDataExtractorTests extends ESTestCase {

    private Client client;
    private List<SearchRequestBuilder> capturedSearchRequests;
    private List<String> capturedContinueScrollIds;
    private List<String> capturedClearScrollIds;
    private String jobId;
    private List<String> jobFields;
    private String timeField;
    private List<String> types;
    private List<String> indexes;
    private QueryBuilder query;
    private AggregatorFactories.Builder aggregations;
    private List<SearchSourceBuilder.ScriptField> scriptFields;
    private int scrollSize;

    private class TestDataExtractor extends ScrollDataExtractor {

        private SearchResponse nextResponse;

        public TestDataExtractor(long start, long end) {
            super(client, createContext(start, end));
        }

        @Override
        protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
            capturedSearchRequests.add(searchRequestBuilder);
            return nextResponse;
        }

        @Override
        protected SearchResponse executeSearchScrollRequest(String scrollId) {
            capturedContinueScrollIds.add(scrollId);
            return nextResponse;
        }

        @Override
        void clearScroll(String scrollId) {
            capturedClearScrollIds.add(scrollId);
        }

        void setNextResponse(SearchResponse searchResponse) {
            nextResponse = searchResponse;
        }
    }

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        capturedSearchRequests = new ArrayList<>();
        capturedContinueScrollIds = new ArrayList<>();
        capturedClearScrollIds = new ArrayList<>();
        timeField = "time";
        jobId = "test-job";
        jobFields = Arrays.asList(timeField, "field_1");
        indexes = Arrays.asList("index-1", "index-2");
        types = Arrays.asList("type-1", "type-2");
        query = QueryBuilders.matchAllQuery();
        aggregations = null;
        scriptFields = Collections.emptyList();
        scrollSize = 1000;
    }

    public void testSinglePageExtraction() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);

        SearchResponse response = createSearchResponse(
                Arrays.asList(1100L, 1200L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );
        extractor.setNextResponse(response);

        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        String expectedStream = "{\"time\":1100,\"field_1\":\"a1\"} {\"time\":1200,\"field_1\":\"a2\"}";
        assertThat(asString(stream.get()), equalTo(expectedStream));

        extractor.setNextResponse(createEmptySearchResponse());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));
        assertThat(capturedSearchRequests.size(), equalTo(1));

        String searchRequest = capturedSearchRequests.get(0).toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"size\":1000"));
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}}," +
                "{\"range\":{\"time\":{\"from\":1000,\"to\":2000,\"include_lower\":true,\"include_upper\":false," +
                "\"format\":\"epoch_millis\",\"boost\":1.0}}}]"));
        assertThat(searchRequest, containsString("\"sort\":[{\"time\":{\"order\":\"asc\"}}]"));

        assertThat(capturedContinueScrollIds.size(), equalTo(1));
        assertThat(capturedContinueScrollIds.get(0), equalTo(response.getScrollId()));

        assertThat(capturedClearScrollIds.isEmpty(), is(true));
    }

    public void testMultiplePageExtraction() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 10000L);

        SearchResponse response1 = createSearchResponse(
                Arrays.asList(1000L, 2000L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );
        extractor.setNextResponse(response1);

        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        String expectedStream = "{\"time\":1000,\"field_1\":\"a1\"} {\"time\":2000,\"field_1\":\"a2\"}";
        assertThat(asString(stream.get()), equalTo(expectedStream));

        SearchResponse response2 = createSearchResponse(
                Arrays.asList(3000L, 4000L),
                Arrays.asList("a3", "a4"),
                Arrays.asList("b3", "b4")
        );
        extractor.setNextResponse(response2);

        assertThat(extractor.hasNext(), is(true));
        stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        expectedStream = "{\"time\":3000,\"field_1\":\"a3\"} {\"time\":4000,\"field_1\":\"a4\"}";
        assertThat(asString(stream.get()), equalTo(expectedStream));

        extractor.setNextResponse(createEmptySearchResponse());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));
        assertThat(capturedSearchRequests.size(), equalTo(1));

        String searchRequest1 = capturedSearchRequests.get(0).toString().replaceAll("\\s", "");
        assertThat(searchRequest1, containsString("\"size\":1000"));
        assertThat(searchRequest1, containsString("\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}}," +
                "{\"range\":{\"time\":{\"from\":1000,\"to\":10000,\"include_lower\":true,\"include_upper\":false," +
                "\"format\":\"epoch_millis\",\"boost\":1.0}}}]"));
        assertThat(searchRequest1, containsString("\"sort\":[{\"time\":{\"order\":\"asc\"}}]"));

        assertThat(capturedContinueScrollIds.size(), equalTo(2));
        assertThat(capturedContinueScrollIds.get(0), equalTo(response1.getScrollId()));
        assertThat(capturedContinueScrollIds.get(1), equalTo(response2.getScrollId()));

        assertThat(capturedClearScrollIds.isEmpty(), is(true));
    }

    public void testMultiplePageExtractionGivenCancel() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 10000L);

        SearchResponse response1 = createSearchResponse(
                Arrays.asList(1000L, 2000L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );
        extractor.setNextResponse(response1);

        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        String expectedStream = "{\"time\":1000,\"field_1\":\"a1\"} {\"time\":2000,\"field_1\":\"a2\"}";
        assertThat(asString(stream.get()), equalTo(expectedStream));

        extractor.cancel();

        SearchResponse response2 = createSearchResponse(
                Arrays.asList(2000L, 3000L),
                Arrays.asList("a3", "a4"),
                Arrays.asList("b3", "b4")
        );
        extractor.setNextResponse(response2);

        assertThat(extractor.isCancelled(), is(true));
        assertThat(extractor.hasNext(), is(true));
        stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        expectedStream = "{\"time\":2000,\"field_1\":\"a3\"}";
        assertThat(asString(stream.get()), equalTo(expectedStream));
        assertThat(extractor.hasNext(), is(false));

        assertThat(capturedClearScrollIds.size(), equalTo(1));
        assertThat(capturedClearScrollIds.get(0), equalTo(response2.getScrollId()));
    }

    public void testExtractionGivenInitSearchResponseHasError() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);
        extractor.setNextResponse(createErrorResponse());

        assertThat(extractor.hasNext(), is(true));
        expectThrows(IOException.class, () -> extractor.next());
    }

    public void testExtractionGivenContinueScrollResponseHasError() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 10000L);

        SearchResponse response1 = createSearchResponse(
                Arrays.asList(1000L, 2000L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );
        extractor.setNextResponse(response1);

        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> stream = extractor.next();
        assertThat(stream.isPresent(), is(true));

        extractor.setNextResponse(createErrorResponse());
        assertThat(extractor.hasNext(), is(true));
        expectThrows(IOException.class, () -> extractor.next());
    }

    private ScrollDataExtractorContext createContext(long start, long end) {
        return new ScrollDataExtractorContext(jobId, jobFields, timeField, indexes, types, query, aggregations, scriptFields, scrollSize,
                start, end);
    }

    private SearchResponse createEmptySearchResponse() {
        return createSearchResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    private SearchResponse createSearchResponse(List<Long> timestamps, List<String> field1Values, List<String> field2Values) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getScrollId()).thenReturn(randomAsciiOfLength(1000));
        SearchHits searchHits = mock(SearchHits.class);
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < timestamps.size(); i++) {
            InternalSearchHit hit = new InternalSearchHit(randomInt());
            Map<String, SearchHitField> fields = new HashMap<>();
            fields.put(timeField, new InternalSearchHitField("time", Arrays.asList(timestamps.get(i))));
            fields.put("field_1", new InternalSearchHitField("field_1", Arrays.asList(field1Values.get(i))));
            fields.put("field_2", new InternalSearchHitField("field_2", Arrays.asList(field2Values.get(i))));
            hit.fields(fields);
            hits.add(hit);
        }
        when(searchHits.getHits()).thenReturn(hits.toArray(new SearchHit[hits.size()]));
        when(searchHits.hits()).thenReturn(hits.toArray(new SearchHit[hits.size()]));
        when(searchResponse.getHits()).thenReturn(searchHits);
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
