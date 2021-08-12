/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter.DatafeedTimingStatsPersister;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChunkedDataExtractorTests extends ESTestCase {

    private Client client;
    private List<SearchRequest> capturedSearchRequests;
    private String jobId;
    private String timeField;
    private List<String> indices;
    private QueryBuilder query;
    private int scrollSize;
    private TimeValue chunkSpan;
    private DataExtractorFactory dataExtractorFactory;
    private DatafeedTimingStatsReporter timingStatsReporter;

    private class TestDataExtractor extends ChunkedDataExtractor {

        private SearchResponse nextResponse;
        private SearchPhaseExecutionException ex;

        TestDataExtractor(long start, long end) {
            super(client, dataExtractorFactory, createContext(start, end), timingStatsReporter);
        }

        TestDataExtractor(long start, long end, boolean hasAggregations, Long histogramInterval) {
            super(client, dataExtractorFactory, createContext(start, end, hasAggregations, histogramInterval), timingStatsReporter);
        }

        @Override
        protected SearchResponse executeSearchRequest(ActionRequestBuilder<SearchRequest, SearchResponse> searchRequestBuilder) {
            capturedSearchRequests.add(searchRequestBuilder.request());
            if (ex != null) {
                throw ex;
            }
            return nextResponse;
        }

        void setNextResponse(SearchResponse searchResponse) {
            nextResponse = searchResponse;
        }

        void setNextResponseToError(SearchPhaseExecutionException ex) {
            this.ex = ex;
        }
    }

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        capturedSearchRequests = new ArrayList<>();
        jobId = "test-job";
        timeField = "time";
        indices = Arrays.asList("index-1", "index-2");
        query = QueryBuilders.matchAllQuery();
        scrollSize = 1000;
        chunkSpan = null;
        dataExtractorFactory = mock(DataExtractorFactory.class);
        timingStatsReporter = new DatafeedTimingStatsReporter(new DatafeedTimingStats(jobId), mock(DatafeedTimingStatsPersister.class));
    }

    public void testExtractionGivenNoData() throws IOException {
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2300L);
        extractor.setNextResponse(createSearchResponse(0L, 0L, 0L));

        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenSpecifiedChunk() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2300L);
        extractor.setNextResponse(createSearchResponse(10L, 1000L, 2200L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);
        InputStream inputStream3 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1, inputStream2);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtactor1);

        DataExtractor subExtactor2 = new StubSubExtractor(inputStream3);
        when(dataExtractorFactory.newExtractor(2000L, 2300L)).thenReturn(subExtactor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream3, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2000L);
        verify(dataExtractorFactory).newExtractor(2000L, 2300L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(1));
        String searchRequest = capturedSearchRequests.get(0).toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"size\":0"));
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}}," +
                "{\"range\":{\"time\":{\"from\":1000,\"to\":2300,\"include_lower\":true,\"include_upper\":false," +
                "\"format\":\"epoch_millis\",\"boost\":1.0}}}]"));
        assertThat(searchRequest, containsString("\"aggregations\":{\"earliest_time\":{\"min\":{\"field\":\"time\"}}," +
                "\"latest_time\":{\"max\":{\"field\":\"time\"}}}}"));
        assertThat(searchRequest, not(containsString("\"track_total_hits\":false")));
        assertThat(searchRequest, not(containsString("\"sort\"")));
    }

    public void testExtractionGivenSpecifiedChunkAndAggs() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2300L, true, 1000L);
        extractor.setNextResponse(createSearchResponse(randomLongBetween(2, 10000), 1000L, 2200L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);
        InputStream inputStream3 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1, inputStream2);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtactor1);

        DataExtractor subExtactor2 = new StubSubExtractor(inputStream3);
        when(dataExtractorFactory.newExtractor(2000L, 2300L)).thenReturn(subExtactor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream3, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2000L);
        verify(dataExtractorFactory).newExtractor(2000L, 2300L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(1));
        String searchRequest = capturedSearchRequests.get(0).toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"size\":0"));
        assertThat(searchRequest, containsString("\"query\":{\"bool\":{\"filter\":[{\"match_all\":{\"boost\":1.0}}," +
            "{\"range\":{\"time\":{\"from\":1000,\"to\":2300,\"include_lower\":true,\"include_upper\":false," +
            "\"format\":\"epoch_millis\",\"boost\":1.0}}}]"));
        assertThat(searchRequest, containsString("\"aggregations\":{\"earliest_time\":{\"min\":{\"field\":\"time\"}}," +
            "\"latest_time\":{\"max\":{\"field\":\"time\"}}}}"));
        assertThat(searchRequest, not(containsString("\"track_total_hits\":false")));
        assertThat(searchRequest, not(containsString("\"sort\"")));
    }

    public void testExtractionGivenAutoChunkAndAggs() throws IOException {
        chunkSpan = null;
        TestDataExtractor extractor = new TestDataExtractor(100_000L, 450_000L, true, 200L);

        extractor.setNextResponse(createSearchResponse(randomLongBetween(2, 10000), 100_000L, 400_000L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        // 200 * 1_000 == 200_000
        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(100_000L, 300_000L)).thenReturn(subExtactor1);

        DataExtractor subExtactor2 = new StubSubExtractor(inputStream2);
        when(dataExtractorFactory.newExtractor(300_000L, 450_000L)).thenReturn(subExtactor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().get());
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(100_000L, 300_000L);
        verify(dataExtractorFactory).newExtractor(300_000L, 450_000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(1));
    }

    public void testExtractionGivenAutoChunkAndAggsAndNoData() throws IOException {
        chunkSpan = null;
        TestDataExtractor extractor = new TestDataExtractor(100L, 500L, true, 200L);

        extractor.setNextResponse(createNullSearchResponse());

        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(1));
    }

    public void testExtractionGivenAutoChunkAndScrollSize1000() throws IOException {
        chunkSpan = null;
        scrollSize = 1000;
        TestDataExtractor extractor = new TestDataExtractor(100000L, 450000L);

        // 300K millis * 1000 * 10 / 15K docs = 200000
        extractor.setNextResponse(createSearchResponse(15000L, 100000L, 400000L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(100000L, 300000L)).thenReturn(subExtactor1);

        DataExtractor subExtactor2 = new StubSubExtractor(inputStream2);
        when(dataExtractorFactory.newExtractor(300000L, 450000L)).thenReturn(subExtactor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().get());
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(100000L, 300000L);
        verify(dataExtractorFactory).newExtractor(300000L, 450000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(1));
    }

    public void testExtractionGivenAutoChunkAndScrollSize500() throws IOException {
        chunkSpan = null;
        scrollSize = 500;
        TestDataExtractor extractor = new TestDataExtractor(100000L, 450000L);

        // 300K millis * 500 * 10 / 15K docs = 100000
        extractor.setNextResponse(createSearchResponse(15000L, 100000L, 400000L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(100000L, 200000L)).thenReturn(subExtactor1);

        DataExtractor subExtactor2 = new StubSubExtractor(inputStream2);
        when(dataExtractorFactory.newExtractor(200000L, 300000L)).thenReturn(subExtactor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));

        verify(dataExtractorFactory).newExtractor(100000L, 200000L);
        verify(dataExtractorFactory).newExtractor(200000L, 300000L);

        assertThat(capturedSearchRequests.size(), equalTo(1));
    }

    public void testExtractionGivenAutoChunkIsLessThanMinChunk() throws IOException {
        chunkSpan = null;
        scrollSize = 1000;
        TestDataExtractor extractor = new TestDataExtractor(100000L, 450000L);

        // 30K millis * 1000 * 10 / 150K docs = 2000 < min of 60K
        extractor.setNextResponse(createSearchResponse(150000L, 100000L, 400000L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(100000L, 160000L)).thenReturn(subExtactor1);

        DataExtractor subExtactor2 = new StubSubExtractor(inputStream2);
        when(dataExtractorFactory.newExtractor(160000L, 220000L)).thenReturn(subExtactor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));

        verify(dataExtractorFactory).newExtractor(100000L, 160000L);
        verify(dataExtractorFactory).newExtractor(160000L, 220000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(1));
    }

    public void testExtractionGivenAutoChunkAndDataTimeSpreadIsZero() throws IOException {
        chunkSpan = null;
        scrollSize = 1000;
        TestDataExtractor extractor = new TestDataExtractor(100L, 500L);

        extractor.setNextResponse(createSearchResponse(150000L, 300L, 300L));

        InputStream inputStream1 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(300L, 500L)).thenReturn(subExtactor1);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(300L, 500L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(1));
    }

    public void testExtractionGivenAutoChunkAndTotalTimeRangeSmallerThanChunk() throws IOException {
        chunkSpan = null;
        scrollSize = 1000;
        TestDataExtractor extractor = new TestDataExtractor(1L, 101L);

        // 100 millis * 1000 * 10 / 10 docs = 100000
        extractor.setNextResponse(createSearchResponse(10L, 1L, 101L));

        InputStream inputStream1 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(1L, 101L)).thenReturn(subExtactor1);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(1L, 101L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(1));
    }

    public void testExtractionGivenAutoChunkAndIntermediateEmptySearchShouldReconfigure() throws IOException {
        chunkSpan = null;
        scrollSize = 500;
        TestDataExtractor extractor = new TestDataExtractor(100000L, 400000L);

        // 300K millis * 500 * 10 / 15K docs = 100000
        extractor.setNextResponse(createSearchResponse(15000L, 100000L, 400000L));

        InputStream inputStream1 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(100000L, 200000L)).thenReturn(subExtactor1);

        // This one is empty
        DataExtractor subExtactor2 = new StubSubExtractor();
        when(dataExtractorFactory.newExtractor(200000, 300000L)).thenReturn(subExtactor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));

        // Now we have: 200K millis * 500 * 10 / 5K docs = 200000
        extractor.setNextResponse(createSearchResponse(5000, 200000L, 400000L));

        // This is the last one
        InputStream inputStream2 = mock(InputStream.class);
        DataExtractor subExtactor3 = new StubSubExtractor(inputStream2);
        when(dataExtractorFactory.newExtractor(200000, 400000)).thenReturn(subExtactor3);

        assertEquals(inputStream2, extractor.next().get());
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(100000L, 200000L);
        verify(dataExtractorFactory).newExtractor(200000L, 300000L);
        verify(dataExtractorFactory).newExtractor(200000L, 400000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);

        assertThat(capturedSearchRequests.size(), equalTo(2));

        String searchRequest = capturedSearchRequests.get(0).toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"from\":100000,\"to\":400000"));
        searchRequest = capturedSearchRequests.get(1).toString().replaceAll("\\s", "");
        assertThat(searchRequest, containsString("\"from\":200000,\"to\":400000"));
    }

    public void testCancelGivenNextWasNeverCalled() {
        chunkSpan = TimeValue.timeValueSeconds(1);
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2300L);
        extractor.setNextResponse(createSearchResponse(10L, 1000L, 2200L));

        InputStream inputStream1 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtactor1);

        assertThat(extractor.hasNext(), is(true));

        extractor.cancel();

        assertThat(extractor.isCancelled(), is(true));
        assertThat(extractor.hasNext(), is(false));
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testCancelGivenCurrentSubExtractorHasMore() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2300L);
        extractor.setNextResponse(createSearchResponse(10L, 1000L, 2200L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1, inputStream2);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtactor1);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());

        extractor.cancel();

        assertThat(extractor.isCancelled(), is(true));
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().get());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testCancelGivenCurrentSubExtractorIsDone() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2300L);
        extractor.setNextResponse(createSearchResponse(10L, 1000L, 2200L));

        InputStream inputStream1 = mock(InputStream.class);

        DataExtractor subExtactor1 = new StubSubExtractor(inputStream1);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtactor1);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().get());

        extractor.cancel();

        assertThat(extractor.isCancelled(), is(true));
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testDataSummaryRequestIsFailed() {
        chunkSpan = TimeValue.timeValueSeconds(2);
        TestDataExtractor extractor = new TestDataExtractor(1000L, 2300L);
        extractor.setNextResponseToError(new SearchPhaseExecutionException("search phase 1", "boom", ShardSearchFailure.EMPTY_ARRAY));

        assertThat(extractor.hasNext(), is(true));
        expectThrows(SearchPhaseExecutionException.class, extractor::next);
    }

    public void testNoDataSummaryHasNoData() {
        ChunkedDataExtractor.DataSummary summary = ChunkedDataExtractor.AggregatedDataSummary.noDataSummary(randomNonNegativeLong());
        assertFalse(summary.hasData());
    }

    private SearchResponse createSearchResponse(long totalHits, long earliestTime, long latestTime) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        SearchHit[] hits = new SearchHit[(int)totalHits];
        SearchHits searchHits = new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1);
        when(searchResponse.getHits()).thenReturn(searchHits);

        List<Aggregation> aggs = new ArrayList<>();
        Min min = mock(Min.class);
        when(min.getValue()).thenReturn((double) earliestTime);
        when(min.getName()).thenReturn("earliest_time");
        aggs.add(min);
        Max max = mock(Max.class);
        when(max.getValue()).thenReturn((double) latestTime);
        when(max.getName()).thenReturn("latest_time");
        aggs.add(max);
        Aggregations aggregations = new Aggregations(aggs) {};
        when(searchResponse.getAggregations()).thenReturn(aggregations);
        return searchResponse;
    }

    private SearchResponse createNullSearchResponse() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        SearchHit[] hits = new SearchHit[0];
        SearchHits searchHits = new SearchHits(hits, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1);
        when(searchResponse.getHits()).thenReturn(searchHits);

        List<Aggregation> aggs = new ArrayList<>();
        Min min = mock(Min.class);
        when(min.getValue()).thenReturn(Double.POSITIVE_INFINITY);
        when(min.getName()).thenReturn("earliest_time");
        aggs.add(min);
        Max max = mock(Max.class);
        when(max.getValue()).thenReturn(Double.POSITIVE_INFINITY);
        when(max.getName()).thenReturn("latest_time");
        aggs.add(max);
        Aggregations aggregations = new Aggregations(aggs) {};
        when(searchResponse.getAggregations()).thenReturn(aggregations);
        return searchResponse;
    }

    private ChunkedDataExtractorContext createContext(long start, long end) {
        return createContext(start, end, false, null);
    }

    private ChunkedDataExtractorContext createContext(long start, long end, boolean hasAggregations, Long histogramInterval) {
        return new ChunkedDataExtractorContext(jobId, timeField, indices, query, scrollSize, start, end, chunkSpan,
            ChunkedDataExtractorFactory.newIdentityTimeAligner(), Collections.emptyMap(), hasAggregations, histogramInterval,
            SearchRequest.DEFAULT_INDICES_OPTIONS, Collections.emptyMap());
    }

    private static class StubSubExtractor implements DataExtractor {
        List<InputStream> streams = new ArrayList<>();
        boolean hasNext = true;

        StubSubExtractor() {}

        StubSubExtractor(InputStream... streams) {
            Collections.addAll(this.streams, streams);
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Optional<InputStream> next() {
            if (streams.isEmpty()) {
                hasNext = false;
                return Optional.empty();
            }
            return Optional.of(streams.remove(0));
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void cancel() {
            // do nothing
        }

        @Override
        public long getEndTime() {
            return 0;
        }
    }
}
