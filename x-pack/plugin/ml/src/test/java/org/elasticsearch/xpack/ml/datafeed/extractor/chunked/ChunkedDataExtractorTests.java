/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor.DataSummary;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChunkedDataExtractorTests extends ESTestCase {

    private String jobId;
    private int scrollSize;
    private TimeValue chunkSpan;
    private DataExtractorFactory dataExtractorFactory;

    @Before
    public void setUpTests() {
        jobId = "test-job";
        scrollSize = 1000;
        chunkSpan = null;
        dataExtractorFactory = mock(DataExtractorFactory.class);
    }

    public void testExtractionGivenNoData() throws IOException {
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L));

        DataExtractor summaryExtractor = new StubSubExtractor(new SearchInterval(1000L, 2300L), new DataSummary(null, null, 0L));
        when(dataExtractorFactory.newExtractor(1000L, 2300L)).thenReturn(summaryExtractor);

        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2300L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenSpecifiedChunk() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L));

        DataExtractor summaryExtractor = new StubSubExtractor(new SearchInterval(1000L, 2300L), new DataSummary(1000L, 2300L, 10L));
        when(dataExtractorFactory.newExtractor(1000L, 2300L)).thenReturn(summaryExtractor);

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);
        InputStream inputStream3 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(1000L, 2000L), inputStream1, inputStream2);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtractor1);

        DataExtractor subExtractor2 = new StubSubExtractor(new SearchInterval(2000L, 2300L), inputStream3);
        when(dataExtractorFactory.newExtractor(2000L, 2300L)).thenReturn(subExtractor2);

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(1000L, 2000L)));
        assertEquals(inputStream1, result.data().get());
        assertThat(extractor.hasNext(), is(true));
        result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(1000L, 2000L)));
        assertEquals(inputStream2, result.data().get());
        assertThat(extractor.hasNext(), is(true));
        result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(2000L, 2300L)));
        assertEquals(inputStream3, result.data().get());
        assertThat(extractor.hasNext(), is(true));
        result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(2000L, 2300L)));
        assertThat(result.data().isPresent(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2300L);
        verify(dataExtractorFactory).newExtractor(1000L, 2000L);
        verify(dataExtractorFactory).newExtractor(2000L, 2300L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenSpecifiedChunkAndAggs() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        DataExtractor summaryExtractor = new StubSubExtractor(
            new SearchInterval(1000L, 2300L),
            new DataSummary(1000L, 2200L, randomFrom(0L, 2L, 10000L))
        );
        when(dataExtractorFactory.newExtractor(1000L, 2300L)).thenReturn(summaryExtractor);

        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L, true, 200L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);
        InputStream inputStream3 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(1000L, 2000L), inputStream1, inputStream2);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtractor1);

        DataExtractor subExtractor2 = new StubSubExtractor(new SearchInterval(2000L, 2300L), inputStream3);
        when(dataExtractorFactory.newExtractor(2000L, 2300L)).thenReturn(subExtractor2);

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(1000L, 2000L)));
        assertEquals(inputStream1, result.data().get());
        assertThat(extractor.hasNext(), is(true));
        result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(1000L, 2000L)));
        assertEquals(inputStream2, result.data().get());
        assertThat(extractor.hasNext(), is(true));
        result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(2000L, 2300L)));
        assertEquals(inputStream3, result.data().get());
        assertThat(extractor.hasNext(), is(true));
        result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(2000L, 2300L)));
        assertThat(result.data().isPresent(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2300L);
        verify(dataExtractorFactory).newExtractor(1000L, 2000L);
        verify(dataExtractorFactory).newExtractor(2000L, 2300L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenAutoChunkAndAggs() throws IOException {
        chunkSpan = null;
        DataExtractor summaryExtractor = new StubSubExtractor(
            new SearchInterval(100_000L, 450_000L),
            new DataSummary(100_000L, 400_000L, randomFrom(0L, 2L, 10000L))
        );
        when(dataExtractorFactory.newExtractor(100_000L, 450_000L)).thenReturn(summaryExtractor);

        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(100_000L, 450_000L, true, 200L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        // 200 * 1_000 == 200_000
        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(100_000L, 300_000L), inputStream1);
        when(dataExtractorFactory.newExtractor(100_000L, 300_000L)).thenReturn(subExtractor1);

        DataExtractor subExtractor2 = new StubSubExtractor(new SearchInterval(300_000L, 450_000L), inputStream2);
        when(dataExtractorFactory.newExtractor(300_000L, 450_000L)).thenReturn(subExtractor2);

        assertThat(extractor.hasNext(), is(true));
        DataExtractor.Result result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(100_000L, 300_000L)));
        assertEquals(inputStream1, result.data().get());
        assertThat(extractor.hasNext(), is(true));
        result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(300_000L, 450_000L)));
        assertEquals(inputStream2, result.data().get());
        result = extractor.next();
        assertThat(result.searchInterval(), equalTo(new SearchInterval(300_000L, 450_000L)));
        assertThat(result.data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(100_000L, 450_000L);
        verify(dataExtractorFactory).newExtractor(100_000L, 300_000L);
        verify(dataExtractorFactory).newExtractor(300_000L, 450_000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenAutoChunkAndAggsAndNoData() throws IOException {
        chunkSpan = null;
        DataExtractor summaryExtractor = new StubSubExtractor(new SearchInterval(100L, 500L), new DataSummary(null, null, 0L));
        when(dataExtractorFactory.newExtractor(100L, 500L)).thenReturn(summaryExtractor);

        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(100L, 500L, true, 200L));

        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(100L, 500L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenAutoChunkAndScrollSize1000() throws IOException {
        chunkSpan = null;
        scrollSize = 1000;

        // 300K millis * 1000 * 10 / 15K docs = 200000
        DataExtractor summaryExtractor = new StubSubExtractor(
            new SearchInterval(100000L, 450000L),
            new DataSummary(100000L, 400000L, 15000L)
        );
        when(dataExtractorFactory.newExtractor(100000L, 450000L)).thenReturn(summaryExtractor);

        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(100000L, 450000L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(100_000L, 300_000L), inputStream1);
        when(dataExtractorFactory.newExtractor(100_000L, 300_000L)).thenReturn(subExtractor1);

        DataExtractor subExtractor2 = new StubSubExtractor(new SearchInterval(300_000L, 450_000L), inputStream2);
        when(dataExtractorFactory.newExtractor(300_000L, 450_000L)).thenReturn(subExtractor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().data().get());
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(100000L, 450000L);
        verify(dataExtractorFactory).newExtractor(100000L, 300000L);
        verify(dataExtractorFactory).newExtractor(300000L, 450000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenAutoChunkAndScrollSize500() throws IOException {
        chunkSpan = null;
        scrollSize = 500;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(100000L, 450000L));

        DataExtractor summaryExtractor = new StubSubExtractor(
            new SearchInterval(100000L, 450000L),
            new DataSummary(100000L, 400000L, 15000L)
        );
        when(dataExtractorFactory.newExtractor(100000L, 450000L)).thenReturn(summaryExtractor);

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(100_000L, 200_000L), inputStream1);
        when(dataExtractorFactory.newExtractor(100000L, 200000L)).thenReturn(subExtractor1);

        DataExtractor subExtractor2 = new StubSubExtractor(new SearchInterval(200_000L, 300_000L), inputStream2);
        when(dataExtractorFactory.newExtractor(200000L, 300000L)).thenReturn(subExtractor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));

        verify(dataExtractorFactory).newExtractor(100000L, 450000L);
        verify(dataExtractorFactory).newExtractor(100000L, 200000L);
        verify(dataExtractorFactory).newExtractor(200000L, 300000L);
    }

    public void testExtractionGivenAutoChunkIsLessThanMinChunk() throws IOException {
        chunkSpan = null;
        scrollSize = 1000;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(100000L, 450000L));

        // 30K millis * 1000 * 10 / 150K docs = 2000 < min of 60K
        DataExtractor summaryExtractor = new StubSubExtractor(
            new SearchInterval(100000L, 450000L),
            new DataSummary(100000L, 400000L, 150000L)
        );
        when(dataExtractorFactory.newExtractor(100000L, 450000L)).thenReturn(summaryExtractor);

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(100_000L, 160_000L), inputStream1);
        when(dataExtractorFactory.newExtractor(100000L, 160000L)).thenReturn(subExtractor1);

        DataExtractor subExtractor2 = new StubSubExtractor(new SearchInterval(160_000L, 220_000L), inputStream2);
        when(dataExtractorFactory.newExtractor(160000L, 220000L)).thenReturn(subExtractor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));

        verify(dataExtractorFactory).newExtractor(100000L, 450000L);
        verify(dataExtractorFactory).newExtractor(100000L, 160000L);
        verify(dataExtractorFactory).newExtractor(160000L, 220000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenAutoChunkAndDataTimeSpreadIsZero() throws IOException {
        chunkSpan = null;
        scrollSize = 1000;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(100L, 500L));

        DataExtractor summaryExtractor = new StubSubExtractor(new SearchInterval(100L, 500L), new DataSummary(300L, 300L, 150000L));
        when(dataExtractorFactory.newExtractor(100L, 500L)).thenReturn(summaryExtractor);

        InputStream inputStream1 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(300L, 500L), inputStream1);
        when(dataExtractorFactory.newExtractor(300L, 500L)).thenReturn(subExtractor1);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(100L, 500L);
        verify(dataExtractorFactory).newExtractor(300L, 500L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenAutoChunkAndTotalTimeRangeSmallerThanChunk() throws IOException {
        chunkSpan = null;
        scrollSize = 1000;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1L, 101L));

        // 100 millis * 1000 * 10 / 10 docs = 100000
        InputStream inputStream1 = mock(InputStream.class);
        DataExtractor stubExtractor = new StubSubExtractor(new SearchInterval(1L, 101L), new DataSummary(1L, 101L, 10L), inputStream1);
        when(dataExtractorFactory.newExtractor(1L, 101L)).thenReturn(stubExtractor);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory, times(2)).newExtractor(1L, 101L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenAutoChunkAndIntermediateEmptySearchShouldReconfigure() throws IOException {
        chunkSpan = null;
        scrollSize = 500;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(100000L, 400000L));

        // 300K millis * 500 * 10 / 15K docs = 100000
        DataExtractor summaryExtractor = new StubSubExtractor(
            new SearchInterval(100000L, 400000L),
            new DataSummary(100000L, 400000L, 15000L)
        );
        when(dataExtractorFactory.newExtractor(100000L, 400000L)).thenReturn(summaryExtractor);

        InputStream inputStream1 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(100_000L, 200_000L), inputStream1);
        when(dataExtractorFactory.newExtractor(100000L, 200000L)).thenReturn(subExtractor1);

        // This one is empty
        DataExtractor subExtractor2 = new StubSubExtractor(new SearchInterval(200_000L, 300_000L));
        when(dataExtractorFactory.newExtractor(200000, 300000L)).thenReturn(subExtractor2);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));

        // Now we have: 200K millis * 500 * 10 / 5K docs = 200000
        InputStream inputStream2 = mock(InputStream.class);
        DataExtractor newExtractor = new StubSubExtractor(
            new SearchInterval(300000L, 400000L),
            new DataSummary(300000L, 400000L, 5000L),
            inputStream2
        );
        when(dataExtractorFactory.newExtractor(300000L, 400000L)).thenReturn(newExtractor);

        assertEquals(inputStream2, extractor.next().data().get());
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(100000L, 400000L);  // Initial summary
        verify(dataExtractorFactory).newExtractor(100000L, 200000L);  // Chunk 1
        verify(dataExtractorFactory).newExtractor(200000L, 300000L);  // Chunk 2 with no data
        verify(dataExtractorFactory, times(2)).newExtractor(300000L, 400000L);  // Reconfigure and new chunk
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testCancelGivenNextWasNeverCalled() {
        chunkSpan = TimeValue.timeValueSeconds(1);
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L));
        assertThat(extractor.hasNext(), is(true));
        extractor.cancel();
        assertThat(extractor.isCancelled(), is(true));
        assertThat(extractor.hasNext(), is(false));
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testCancelGivenCurrentSubExtractorHasMore() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L));

        DataExtractor summaryExtractor = new StubSubExtractor(new SearchInterval(1000L, 2300L), new DataSummary(1000L, 2200L, 10L));
        when(dataExtractorFactory.newExtractor(1000L, 2300L)).thenReturn(summaryExtractor);

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(1000L, 2000L), inputStream1, inputStream2);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtractor1);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().data().get());

        extractor.cancel();

        assertThat(extractor.isCancelled(), is(true));
        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream2, extractor.next().data().get());
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2300L);
        verify(dataExtractorFactory).newExtractor(1000L, 2000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testCancelGivenCurrentSubExtractorIsDone() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);

        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L));

        DataExtractor summaryExtractor = new StubSubExtractor(new SearchInterval(1000L, 2300L), new DataSummary(1000L, 2200L, 10L));
        when(dataExtractorFactory.newExtractor(1000L, 2300L)).thenReturn(summaryExtractor);

        InputStream inputStream1 = mock(InputStream.class);

        DataExtractor subExtractor1 = new StubSubExtractor(new SearchInterval(1000L, 3000L), inputStream1);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtractor1);

        assertThat(extractor.hasNext(), is(true));
        assertEquals(inputStream1, extractor.next().data().get());

        extractor.cancel();

        assertThat(extractor.isCancelled(), is(true));
        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(1000L, 2300L);
        verify(dataExtractorFactory).newExtractor(1000L, 2000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testDataSummaryRequestIsFailed() {
        chunkSpan = TimeValue.timeValueSeconds(2);
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L));
        when(dataExtractorFactory.newExtractor(1000L, 2300L)).thenThrow(
            new SearchPhaseExecutionException("search phase 1", "boom", ShardSearchFailure.EMPTY_ARRAY)
        );

        assertThat(extractor.hasNext(), is(true));
        expectThrows(SearchPhaseExecutionException.class, extractor::next);
    }

    public void testNoDataSummaryHasNoData() {
        DataSummary summary = new DataSummary(null, null, 0L);
        assertFalse(summary.hasData());
    }

    private ChunkedDataExtractorContext createContext(long start, long end) {
        return createContext(start, end, false, null);
    }

    private ChunkedDataExtractorContext createContext(long start, long end, boolean hasAggregations, Long histogramInterval) {
        return new ChunkedDataExtractorContext(
            jobId,
            scrollSize,
            start,
            end,
            chunkSpan,
            ChunkedDataExtractorFactory.newIdentityTimeAligner(),
            hasAggregations,
            histogramInterval
        );
    }

    private static class StubSubExtractor implements DataExtractor {

        private final DataSummary summary;
        private final SearchInterval searchInterval;
        private final List<InputStream> streams = new ArrayList<>();
        private boolean hasNext = true;

        StubSubExtractor(SearchInterval searchInterval, InputStream... streams) {
            this(searchInterval, null, streams);
        }

        StubSubExtractor(SearchInterval searchInterval, DataSummary summary, InputStream... streams) {
            this.searchInterval = searchInterval;
            this.summary = summary;
            Collections.addAll(this.streams, streams);
        }

        @Override
        public DataSummary getSummary() {
            return summary;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Result next() {
            if (streams.isEmpty()) {
                hasNext = false;
                return new Result(searchInterval, Optional.empty());
            }
            return new Result(searchInterval, Optional.of(streams.remove(0)));
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
        public void destroy() {
            // do nothing
        }

        @Override
        public long getEndTime() {
            return 0;
        }
    }
}
