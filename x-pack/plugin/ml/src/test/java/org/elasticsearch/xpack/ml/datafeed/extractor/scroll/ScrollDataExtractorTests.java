/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter.DatafeedTimingStatsPersister;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedField;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.TimeBasedExtractedFields;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScrollDataExtractorTests extends ESTestCase {

    private Client client;
    private List<SearchRequestBuilder> capturedSearchRequests;
    private List<String> capturedContinueScrollIds;
    private ArgumentCaptor<ClearScrollRequest> capturedClearScrollRequests;
    private String jobId;
    private TimeBasedExtractedFields extractedFields;
    private List<String> indices;
    private QueryBuilder query;
    private List<SearchSourceBuilder.ScriptField> scriptFields;
    private int scrollSize;
    private long initScrollStartTime;
    private ActionFuture<ClearScrollResponse> clearScrollFuture;
    private DatafeedTimingStatsReporter timingStatsReporter;

    private class TestDataExtractor extends ScrollDataExtractor {

        private Queue<SearchResponse> responses = new LinkedList<>();

        TestDataExtractor(long start, long end) {
            this(createContext(start, end));
        }

        TestDataExtractor(ScrollDataExtractorContext context) {
            super(client, context, timingStatsReporter);
        }

        @Override
        protected InputStream initScroll(long startTimestamp) throws IOException {
            initScrollStartTime = startTimestamp;
            return super.initScroll(startTimestamp);
        }

        @Override
        protected SearchResponse executeSearchRequest(SearchRequestBuilder searchRequestBuilder) {
            capturedSearchRequests.add(searchRequestBuilder);
            return responses.remove();
        }

        @Override
        protected SearchResponse executeSearchScrollRequest(String scrollId) {
            capturedContinueScrollIds.add(scrollId);
            SearchResponse searchResponse = responses.remove();
            if (searchResponse == null) {
                throw new SearchPhaseExecutionException("foo", "bar", new ShardSearchFailure[] {});
            } else {
                return searchResponse;
            }
        }

        void setNextResponse(SearchResponse searchResponse) {
            responses.add(searchResponse);
        }

        public long getInitScrollStartTime() {
            return initScrollStartTime;
        }

        public Long getLastTimestamp() {
            return lastTimestamp;
        }
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        capturedSearchRequests = new ArrayList<>();
        capturedContinueScrollIds = new ArrayList<>();
        jobId = "test-job";
        ExtractedField timeField = ExtractedField.newField("time", Collections.singleton("date"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        extractedFields = new TimeBasedExtractedFields(timeField,
                Arrays.asList(timeField, ExtractedField.newField("field_1", Collections.singleton("keyword"),
                    ExtractedField.ExtractionMethod.DOC_VALUE)));
        indices = Arrays.asList("index-1", "index-2");
        query = QueryBuilders.matchAllQuery();
        scriptFields = Collections.emptyList();
        scrollSize = 1000;

        clearScrollFuture = mock(ActionFuture.class);
        capturedClearScrollRequests = ArgumentCaptor.forClass(ClearScrollRequest.class);
        when(client.execute(same(ClearScrollAction.INSTANCE), capturedClearScrollRequests.capture())).thenReturn(clearScrollFuture);
        timingStatsReporter = new DatafeedTimingStatsReporter(new DatafeedTimingStats(jobId), mock(DatafeedTimingStatsPersister.class));
    }

    public void testSinglePageExtraction() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);

        SearchResponse response1 = createSearchResponse(
                Arrays.asList(1100L, 1200L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );
        extractor.setNextResponse(response1);

        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        String expectedStream = "{\"time\":1100,\"field_1\":\"a1\"} {\"time\":1200,\"field_1\":\"a2\"}";
        assertThat(asString(stream.get()), equalTo(expectedStream));

        SearchResponse response2 = createEmptySearchResponse();
        extractor.setNextResponse(response2);
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
        assertThat(searchRequest, containsString("\"stored_fields\":\"_none_\""));

        assertThat(capturedContinueScrollIds.size(), equalTo(1));
        assertThat(capturedContinueScrollIds.get(0), equalTo(response1.getScrollId()));

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        assertThat(capturedClearScrollIds.size(), equalTo(1));
        assertThat(capturedClearScrollIds.get(0), equalTo(response2.getScrollId()));
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

        SearchResponse response3 = createEmptySearchResponse();
        extractor.setNextResponse(response3);
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

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        assertThat(capturedClearScrollIds.size(), equalTo(1));
        assertThat(capturedClearScrollIds.get(0), equalTo(response3.getScrollId()));
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
                Arrays.asList(2000L, 2000L, 3000L),
                Arrays.asList("a3", "a4", "a5"),
                Arrays.asList("b3", "b4", "b5")
        );
        extractor.setNextResponse(response2);

        assertThat(extractor.isCancelled(), is(true));
        assertThat(extractor.hasNext(), is(true));
        stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        expectedStream = "{\"time\":2000,\"field_1\":\"a3\"} {\"time\":2000,\"field_1\":\"a4\"}";
        assertThat(asString(stream.get()), equalTo(expectedStream));
        assertThat(extractor.hasNext(), is(false));

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        assertThat(capturedClearScrollIds.size(), equalTo(1));
        assertThat(capturedClearScrollIds.get(0), equalTo(response2.getScrollId()));
    }

    public void testExtractionGivenInitSearchResponseHasError() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);
        extractor.setNextResponse(createErrorResponse());

        assertThat(extractor.hasNext(), is(true));
        expectThrows(IOException.class, extractor::next);
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
        expectThrows(IOException.class, extractor::next);

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        assertThat(capturedClearScrollIds.size(), equalTo(1));
    }

    public void testExtractionGivenInitSearchResponseHasShardFailures() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);
        extractor.setNextResponse(createResponseWithShardFailures());
        extractor.setNextResponse(createResponseWithShardFailures());

        assertThat(extractor.hasNext(), is(true));
        expectThrows(IOException.class, extractor::next);

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        // We should clear the scroll context twice: once for the first search when we retry
        // and once after the retry where we'll have an exception
        assertThat(capturedClearScrollIds.size(), equalTo(2));
    }

    public void testExtractionGivenInitSearchResponseEncounteredUnavailableShards() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);
        extractor.setNextResponse(createResponseWithUnavailableShards(1));
        extractor.setNextResponse(createResponseWithUnavailableShards(1));

        assertThat(extractor.hasNext(), is(true));
        IOException e = expectThrows(IOException.class, extractor::next);
        assertThat(e.getMessage(), equalTo("[" + jobId + "] Search request encountered [1] unavailable shards"));
    }

    public void testResetScrollAfterShardFailure() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);

        SearchResponse goodResponse = createSearchResponse(
                Arrays.asList(1100L, 1200L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );
        extractor.setNextResponse(goodResponse);
        extractor.setNextResponse(createResponseWithShardFailures());
        extractor.setNextResponse(goodResponse);
        extractor.setNextResponse(createResponseWithShardFailures());

        // first response is good
        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> output = extractor.next();
        assertThat(output.isPresent(), is(true));
        // this should recover from the first shard failure and try again
        assertThat(extractor.hasNext(), is(true));
        output = extractor.next();
        assertThat(output.isPresent(), is(true));
        // A second failure is not tolerated
        assertThat(extractor.hasNext(), is(true));
        expectThrows(IOException.class, extractor::next);

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        assertThat(capturedClearScrollIds.size(), equalTo(2));
    }

    public void testResetScollUsesLastResultTimestamp() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);

        SearchResponse goodResponse = createSearchResponse(
                Arrays.asList(1100L, 1200L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );

        extractor.setNextResponse(goodResponse);
        extractor.setNextResponse(createResponseWithShardFailures());
        extractor.setNextResponse(createResponseWithShardFailures());

        Optional<InputStream> output = extractor.next();
        assertThat(output.isPresent(), is(true));
        assertEquals(1000L, extractor.getInitScrollStartTime());

        expectThrows(IOException.class, () -> extractor.next());
        // the new start time after error is the last record timestamp +1
        assertEquals(1201L, extractor.getInitScrollStartTime());
    }

    public void testResetScrollAfterSearchPhaseExecutionException() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);
        SearchResponse firstResponse = createSearchResponse(
                Arrays.asList(1100L, 1200L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );

        SearchResponse secondResponse = createSearchResponse(
                Arrays.asList(1300L, 1400L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );

        extractor.setNextResponse(firstResponse);
        extractor.setNextResponse(null); // this will throw a SearchPhaseExecutionException
        extractor.setNextResponse(secondResponse);
        extractor.setNextResponse(null); // this will throw a SearchPhaseExecutionException


        // first response is good
        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> output = extractor.next();
        assertThat(output.isPresent(), is(true));
        // this should recover from the SearchPhaseExecutionException and try again
        assertThat(extractor.hasNext(), is(true));
        output = extractor.next();
        assertThat(output.isPresent(), is(true));
        assertEquals(Long.valueOf(1400L), extractor.getLastTimestamp());
        // A second failure is not tolerated
        assertThat(extractor.hasNext(), is(true));
        expectThrows(SearchPhaseExecutionException.class, extractor::next);

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        assertThat(capturedClearScrollIds.size(), equalTo(2));
    }

    public void testSearchPhaseExecutionExceptionOnInitScroll() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2000L);

        extractor.setNextResponse(createResponseWithShardFailures());
        extractor.setNextResponse(createResponseWithShardFailures());

        expectThrows(IOException.class, extractor::next);

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        // We should clear the scroll context twice: once for the first search when we retry
        // and once after the retry where we'll have an exception
        assertThat(capturedClearScrollIds.size(), equalTo(2));
    }

    public void testDomainSplitScriptField() throws IOException {

        SearchSourceBuilder.ScriptField withoutSplit = new SearchSourceBuilder.ScriptField(
                "script1", mockScript("return 1+1;"), false);
        SearchSourceBuilder.ScriptField withSplit = new SearchSourceBuilder.ScriptField(
                "script2", new Script(ScriptType.INLINE, "painless", "return domainSplit('foo.com', params);", emptyMap()), false);

        List<SearchSourceBuilder.ScriptField> sFields = Arrays.asList(withoutSplit, withSplit);
        ScrollDataExtractorContext context = new ScrollDataExtractorContext(jobId, extractedFields, indices,
                query, sFields, scrollSize, 1000, 2000, Collections.emptyMap());

        TestDataExtractor extractor = new TestDataExtractor(context);

        SearchResponse response1 = createSearchResponse(
                Arrays.asList(1100L, 1200L),
                Arrays.asList("a1", "a2"),
                Arrays.asList("b1", "b2")
        );
        extractor.setNextResponse(response1);

        assertThat(extractor.hasNext(), is(true));
        Optional<InputStream> stream = extractor.next();
        assertThat(stream.isPresent(), is(true));
        String expectedStream = "{\"time\":1100,\"field_1\":\"a1\"} {\"time\":1200,\"field_1\":\"a2\"}";
        assertThat(asString(stream.get()), equalTo(expectedStream));

        SearchResponse response2 = createEmptySearchResponse();
        extractor.setNextResponse(response2);
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
        assertThat(searchRequest, containsString("\"stored_fields\":\"_none_\""));

        // Check for the scripts
        assertThat(searchRequest, containsString("{\"script\":{\"source\":\"return 1 + 1;\",\"lang\":\"mockscript\"}"
                .replaceAll("\\s", "")));

        assertThat(capturedContinueScrollIds.size(), equalTo(1));
        assertThat(capturedContinueScrollIds.get(0), equalTo(response1.getScrollId()));

        List<String> capturedClearScrollIds = getCapturedClearScrollIds();
        assertThat(capturedClearScrollIds.size(), equalTo(1));
        assertThat(capturedClearScrollIds.get(0), equalTo(response2.getScrollId()));
    }

    private ScrollDataExtractorContext createContext(long start, long end) {
        return new ScrollDataExtractorContext(jobId, extractedFields, indices, query, scriptFields, scrollSize, start, end,
                Collections.emptyMap());
    }

    private SearchResponse createEmptySearchResponse() {
        return createSearchResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    private SearchResponse createSearchResponse(List<Long> timestamps, List<String> field1Values, List<String> field2Values) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getScrollId()).thenReturn(randomAlphaOfLength(1000));
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < timestamps.size(); i++) {
            SearchHit hit = new SearchHit(randomInt());
            Map<String, DocumentField> fields = new HashMap<>();
            fields.put(extractedFields.timeField(), new DocumentField("time", Collections.singletonList(timestamps.get(i))));
            fields.put("field_1", new DocumentField("field_1", Collections.singletonList(field1Values.get(i))));
            fields.put("field_2", new DocumentField("field_2", Collections.singletonList(field2Values.get(i))));
            hit.fields(fields);
            hits.add(hit);
        }
        SearchHits searchHits = new SearchHits(hits.toArray(new SearchHit[0]),
            new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO), 1);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchResponse.getTook()).thenReturn(TimeValue.timeValueMillis(randomNonNegativeLong()));
        return searchResponse;
    }

    private SearchResponse createErrorResponse() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR);
        when(searchResponse.getScrollId()).thenReturn(randomAlphaOfLength(1000));
        return searchResponse;
    }

    private SearchResponse createResponseWithShardFailures() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getShardFailures()).thenReturn(
                new ShardSearchFailure[] { new ShardSearchFailure(new RuntimeException("shard failed"))});
        when(searchResponse.getFailedShards()).thenReturn(1);
        when(searchResponse.getScrollId()).thenReturn(randomAlphaOfLength(1000));
        return searchResponse;
    }

    private SearchResponse createResponseWithUnavailableShards(int unavailableShards) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getSuccessfulShards()).thenReturn(2);
        when(searchResponse.getTotalShards()).thenReturn(2 + unavailableShards);
        when(searchResponse.getFailedShards()).thenReturn(unavailableShards);
        return searchResponse;
    }

    private List<String> getCapturedClearScrollIds() {
        return capturedClearScrollRequests.getAllValues().stream().map(r -> r.getScrollIds().get(0)).collect(Collectors.toList());
    }

    private static String asString(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }
}
