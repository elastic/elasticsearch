/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.LinkedClusterState;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor.DataSummary;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractor;
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
import static org.mockito.AdditionalMatchers.lt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChunkedDataExtractorTests extends ESTestCase {

    private static final long ESQL_GROUPING_INTERVAL_MILLIS = 1_000L;

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

    // Replaces testEsqlChunkAtCapAfterShrinkShouldMarkWindowIncomplete, which expected a single shrink attempt: a truncated
    // chunk is now shrunk repeatedly, so it only turns incomplete once the shrinks run out or the floor is reached.
    public void testEsqlTruncatedChunkShouldStopShrinkingAtGroupingIntervalFloor() throws IOException {
        chunkSpan = TimeValue.timeValueMinutes(1);
        ChunkedDataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(0L, 120_000L));
        stubSummary(0L, 120_000L, new DataSummary(0L, 119_999L, 2_000L));
        InputStream firstStream = mock(InputStream.class);
        InputStream secondStream = mock(InputStream.class);
        InputStream floorStream = mock(InputStream.class);
        // Every result is at the real injected LIMIT (EsqlDataExtractor.INJECTED_ROW_LIMIT), so it is genuinely truncated.
        // 60s shrinks to 3s, then to the 1s grouping interval, below which it cannot go.
        long truncated = EsqlDataExtractor.INJECTED_ROW_LIMIT;
        when(dataExtractorFactory.newExtractor(0L, 60_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 60_000L), truncated, firstStream)
        );
        when(dataExtractorFactory.newExtractor(0L, 3_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 3_000L), truncated, secondStream)
        );
        when(dataExtractorFactory.newExtractor(0L, 1_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 1_000L), truncated, floorStream)
        );

        DataExtractor.Result result = extractor.next();

        assertThat(result.rowCount(), equalTo(truncated));
        assertThat(result.data().orElseThrow(), equalTo(floorStream));
        assertThat(extractor.getIncompleteSearchInterval(), equalTo(Optional.of(new SearchInterval(0L, 1_000L))));
        verify(firstStream).close();
        verify(secondStream).close();
    }

    public void testEsqlTruncatedChunkShouldShrinkRepeatedlyUntilComplete() throws IOException {
        chunkSpan = TimeValue.timeValueMillis(400_000L);
        ChunkedDataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(0L, 800_000L));
        stubSummary(0L, 800_000L, new DataSummary(0L, 799_999L, 50_000L));
        InputStream firstStream = mock(InputStream.class);
        InputStream secondStream = mock(InputStream.class);
        InputStream completeStream = mock(InputStream.class);
        long truncated = EsqlDataExtractor.INJECTED_ROW_LIMIT;
        // 400s is truncated, the first shrink (20s) is still truncated, the second shrink (1s) is complete
        when(dataExtractorFactory.newExtractor(0L, 400_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 400_000L), truncated, firstStream)
        );
        when(dataExtractorFactory.newExtractor(0L, 20_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 20_000L), truncated, secondStream)
        );
        when(dataExtractorFactory.newExtractor(0L, 1_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 1_000L), 500L, completeStream)
        );

        DataExtractor.Result result = extractor.next();

        assertThat(result.rowCount(), equalTo(500L));
        assertThat(result.data().orElseThrow(), equalTo(completeStream));
        assertThat(extractor.getIncompleteSearchInterval(), equalTo(Optional.empty()));
        verify(firstStream).close();
        verify(secondStream).close();
    }

    public void testEsqlTruncatedChunkShouldMarkWindowIncompleteAfterMaxShrinkAttempts() throws IOException {
        // The window is wide enough that the interval is still above the grouping interval after every allowed shrink, so
        // only the attempt cap stops the shrinking.
        long[] lengths = shrinkLengths(ChunkedDataExtractor.MAX_ESQL_SHRINK_ATTEMPTS, 2 * ESQL_GROUPING_INTERVAL_MILLIS);
        long windowLength = 2 * lengths[0];
        chunkSpan = TimeValue.timeValueMillis(lengths[0]);
        ChunkedDataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(0L, windowLength));
        stubSummary(0L, windowLength, new DataSummary(0L, windowLength - 1, 1_000_000L));
        List<InputStream> streams = new ArrayList<>();
        for (long length : lengths) {
            InputStream stream = mock(InputStream.class);
            streams.add(stream);
            when(dataExtractorFactory.newExtractor(0L, length)).thenReturn(
                new StubSubExtractor(new SearchInterval(0L, length), EsqlDataExtractor.INJECTED_ROW_LIMIT, stream)
            );
        }

        DataExtractor.Result result = extractor.next();

        long lastLength = lengths[lengths.length - 1];
        assertThat(lastLength > ESQL_GROUPING_INTERVAL_MILLIS, is(true));
        assertThat(result.data().orElseThrow(), equalTo(streams.get(streams.size() - 1)));
        assertThat(extractor.getIncompleteSearchInterval(), equalTo(Optional.of(new SearchInterval(0L, lastLength))));
        // one initial query plus MAX_ESQL_SHRINK_ATTEMPTS shrunk ones, and no further attempt
        verify(dataExtractorFactory, times(lengths.length)).newExtractor(eq(0L), lt(windowLength));
        for (int i = 0; i < streams.size() - 1; i++) {
            verify(streams.get(i)).close();
        }
    }

    public void testEsqlShrinkAttemptsShouldResetForTheNextChunk() throws IOException {
        // The last allowed shrink of the first chunk succeeds, which uses up all attempts; the next chunk must be able to
        // shrink again.
        long[] lengths = shrinkLengths(ChunkedDataExtractor.MAX_ESQL_SHRINK_ATTEMPTS, ESQL_GROUPING_INTERVAL_MILLIS);
        long chunkLength = lengths[0];
        long firstChunkEnd = lengths[lengths.length - 1];
        chunkSpan = TimeValue.timeValueMillis(chunkLength);
        long windowEnd = firstChunkEnd + chunkLength;
        ChunkedDataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(0L, windowEnd));
        stubSummary(0L, windowEnd, new DataSummary(0L, windowEnd - 1, 1_000_000L));
        for (int i = 0; i < lengths.length - 1; i++) {
            when(dataExtractorFactory.newExtractor(0L, lengths[i])).thenReturn(
                new StubSubExtractor(new SearchInterval(0L, lengths[i]), EsqlDataExtractor.INJECTED_ROW_LIMIT, mock(InputStream.class))
            );
        }
        InputStream firstChunkStream = mock(InputStream.class);
        when(dataExtractorFactory.newExtractor(0L, firstChunkEnd)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, firstChunkEnd), 500L, firstChunkStream)
        );
        // second chunk starts where the shrunk first one ended and is truncated at full length
        long secondShrunkEnd = firstChunkEnd + lengths[1];
        when(dataExtractorFactory.newExtractor(firstChunkEnd, firstChunkEnd + chunkLength)).thenReturn(
            new StubSubExtractor(
                new SearchInterval(firstChunkEnd, firstChunkEnd + chunkLength),
                EsqlDataExtractor.INJECTED_ROW_LIMIT,
                mock(InputStream.class)
            )
        );
        InputStream secondChunkStream = mock(InputStream.class);
        when(dataExtractorFactory.newExtractor(firstChunkEnd, secondShrunkEnd)).thenReturn(
            new StubSubExtractor(new SearchInterval(firstChunkEnd, secondShrunkEnd), 500L, secondChunkStream)
        );

        assertThat(extractor.next().data().orElseThrow(), equalTo(firstChunkStream));
        assertThat(extractor.next().data().orElseThrow(), equalTo(secondChunkStream));
        assertThat(extractor.getIncompleteSearchInterval(), equalTo(Optional.empty()));
    }

    public void testEsqlCompleteResultBelowInjectedLimitShouldBeAcceptedWithoutRetry() throws IOException {
        // Replaces testEsqlCompleteHighRowChunkShouldRetrySmallerWithoutMarkingWindowIncomplete: results of 1,000 to 9,999 rows
        // are complete (only the injected LIMIT of 10,000 truncates), so they are no longer discarded and re-queried smaller.
        long rowCount = randomLongBetween(1_000L, EsqlDataExtractor.INJECTED_ROW_LIMIT - 1);
        chunkSpan = TimeValue.timeValueMinutes(1);
        ChunkedDataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(0L, 120_000L));
        stubSummary(0L, 120_000L, new DataSummary(0L, 119_999L, 3_000L));
        InputStream stream = mock(InputStream.class);
        when(dataExtractorFactory.newExtractor(0L, 60_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 60_000L), rowCount, stream)
        );

        DataExtractor.Result result = extractor.next();

        assertThat(result.rowCount(), equalTo(rowCount));
        assertThat(result.data().orElseThrow(), equalTo(stream));
        assertThat(extractor.getIncompleteSearchInterval(), equalTo(Optional.empty()));
        verify(stream, never()).close();
        verify(dataExtractorFactory, never()).newExtractor(eq(0L), lt(60_000L));
    }

    public void testEsqlUserLimitBelowCapShouldNotMarkWindowIncomplete() throws IOException {
        chunkSpan = TimeValue.timeValueMinutes(1);
        ChunkedDataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(0L, 120_000L));
        stubSummary(0L, 120_000L, new DataSummary(0L, 119_999L, 2_000L));
        when(dataExtractorFactory.newExtractor(0L, 60_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 60_000L), 50L, mock(InputStream.class))
        );

        DataExtractor.Result result = extractor.next();

        assertThat(result.rowCount(), equalTo(50L));
        assertThat(extractor.getIncompleteSearchInterval(), equalTo(Optional.empty()));
    }

    public void testEsqlChunkAtCapInUnshrinkableWindowShouldMarkWindowIncomplete() throws IOException {
        // Window equals exactly one grouping interval, so the real aligner cannot shrink it any further.
        ChunkedDataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(0L, 1_000L));
        when(dataExtractorFactory.newExtractor(0L, 1_000L)).thenReturn(
            new StubSubExtractor(new SearchInterval(0L, 1_000L), new DataSummary(0L, 0L, 10_000L)),
            new StubSubExtractor(new SearchInterval(0L, 1_000L), 10_000L, mock(InputStream.class))
        );

        extractor.next();

        assertThat(extractor.getIncompleteSearchInterval(), equalTo(Optional.of(new SearchInterval(0L, 1_000L))));
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

    public void testLinkedClusterStatesPropagateThroughChunkedExtractor() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L));

        DataExtractor summaryExtractor = new StubSubExtractor(new SearchInterval(1000L, 2300L), new DataSummary(1000L, 2300L, 10L));
        when(dataExtractorFactory.newExtractor(1000L, 2300L)).thenReturn(summaryExtractor);

        List<LinkedClusterState> clusterStates = List.of(
            new LinkedClusterState("remote_1", LinkedClusterState.Status.AVAILABLE, null, 50L)
        );

        InputStream inputStream1 = mock(InputStream.class);
        DataExtractor subExtractor1 = new StubSubExtractorWithClusterStates(new SearchInterval(1000L, 2000L), clusterStates, inputStream1);
        when(dataExtractorFactory.newExtractor(1000L, 2000L)).thenReturn(subExtractor1);

        DataExtractor subExtractor2 = new StubSubExtractor(new SearchInterval(2000L, 2300L));
        when(dataExtractorFactory.newExtractor(2000L, 2300L)).thenReturn(subExtractor2);

        DataExtractor.Result result = extractor.next();
        assertThat(result.data().isPresent(), is(true));
        assertThat(result.linkedClusterStates(), equalTo(clusterStates));

        // Advance past the empty sub-extractor results until we get the final empty result
        while (extractor.hasNext()) {
            result = extractor.next();
        }
        // The final empty result should still carry the last seen linked project states
        assertThat(result.linkedClusterStates(), equalTo(clusterStates));
    }

    public void testLinkedClusterStatesPropagateWhenSetUpChunkedSearchThrows() {
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createContext(1000L, 2300L));

        List<LinkedClusterState> skippedStates = List.of(new LinkedClusterState("remote_1", LinkedClusterState.Status.SKIPPED, null, 50L));
        DataExtractor summaryExtractor = new StubSummaryExtractorThrowingWithClusterStates(skippedStates);
        when(dataExtractorFactory.newExtractor(1000L, 2300L)).thenReturn(summaryExtractor);

        assertThat(extractor.hasNext(), is(true));
        expectThrows(ResourceNotFoundException.class, extractor::next);

        // Despite the failure, the skipped cluster states observed by the summary extractor
        // must be surfaced via getLinkedClusterStates() so that callers can update CCS stats.
        assertThat(extractor.getLinkedClusterStates(), equalTo(skippedStates));
    }

    public void testNoDataSummaryHasNoData() {
        DataSummary summary = new DataSummary(null, null, 0L);
        assertFalse(summary.hasData());
    }

    private ChunkedDataExtractorContext createContext(long start, long end) {
        return createContext(start, end, false, null, false);
    }

    private ChunkedDataExtractorContext createContext(long start, long end, boolean hasAggregations, Long histogramInterval) {
        return createContext(start, end, hasAggregations, histogramInterval, false);
    }

    private ChunkedDataExtractorContext createEsqlContext(long start, long end) {
        return createEsqlContext(start, end, ESQL_GROUPING_INTERVAL_MILLIS);
    }

    private ChunkedDataExtractorContext createEsqlContext(long start, long end, long groupingIntervalMillis) {
        ChunkedDataExtractorContext.TimeAligner timeAligner = ChunkedDataExtractorFactory.newIntervalTimeAligner(groupingIntervalMillis);
        return new ChunkedDataExtractorContext(
            jobId,
            scrollSize,
            timeAligner.alignToCeil(start),
            timeAligner.alignToFloor(end),
            chunkSpan,
            timeAligner,
            false,
            null,
            true
        );
    }

    private ChunkedDataExtractorContext createContext(
        long start,
        long end,
        boolean hasAggregations,
        Long histogramInterval,
        boolean hasEsqlQuery
    ) {
        return new ChunkedDataExtractorContext(
            jobId,
            scrollSize,
            start,
            end,
            chunkSpan,
            ChunkedDataExtractorFactory.newIdentityTimeAligner(),
            hasAggregations,
            histogramInterval,
            hasEsqlQuery
        );
    }

    private void stubSummary(long start, long end, DataSummary summary) {
        when(dataExtractorFactory.newExtractor(start, end)).thenReturn(new StubSubExtractor(new SearchInterval(start, end), summary));
    }

    private void stubChunk(long start, long end, InputStream... streams) {
        when(dataExtractorFactory.newExtractor(start, end)).thenReturn(new StubSubExtractor(new SearchInterval(start, end), streams));
    }

    private void stubChunkWithSummary(long start, long end, DataSummary summary, InputStream... streams) {
        when(dataExtractorFactory.newExtractor(start, end)).thenReturn(
            new StubSubExtractor(new SearchInterval(start, end), summary, streams)
        );
    }

    private void assertNextStream(DataExtractor extractor, InputStream expected) throws IOException {
        assertThat(extractor.hasNext(), is(true));
        assertEquals(expected, extractor.next().data().get());
    }

    private void assertNoMoreData(DataExtractor extractor) throws IOException {
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));
    }

    public void testExtractionGivenEsqlQueryAndAutoChunk() throws IOException {
        chunkSpan = null;
        stubSummary(100_000L, 450_000L, new DataSummary(100_000L, 400_000L, 750L));

        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(100_000L, 450_000L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);
        stubChunk(100_000L, 300_000L, inputStream1);
        stubChunk(300_000L, 450_000L, inputStream2);

        assertNextStream(extractor, inputStream1);
        assertNextStream(extractor, inputStream2);
        assertNoMoreData(extractor);

        verify(dataExtractorFactory).newExtractor(100_000L, 450_000L);
        verify(dataExtractorFactory).newExtractor(100_000L, 300_000L);
        verify(dataExtractorFactory).newExtractor(300_000L, 450_000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenEsqlQueryAndAutoChunkIsLessThanMinChunk() throws IOException {
        chunkSpan = null;
        stubSummary(100_000L, 450_000L, new DataSummary(100_000L, 400_000L, 150_000L));

        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(100_000L, 450_000L));

        InputStream inputStream1 = mock(InputStream.class);
        InputStream inputStream2 = mock(InputStream.class);
        stubChunk(100_000L, 160_000L, inputStream1);
        stubChunk(160_000L, 220_000L, inputStream2);

        assertNextStream(extractor, inputStream1);
        assertNextStream(extractor, inputStream2);
        assertThat(extractor.hasNext(), is(true));

        verify(dataExtractorFactory).newExtractor(100_000L, 450_000L);
        verify(dataExtractorFactory).newExtractor(100_000L, 160_000L);
        verify(dataExtractorFactory).newExtractor(160_000L, 220_000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenEsqlQueryAndAutoChunkAndDataTimeSpreadIsZero() throws IOException {
        chunkSpan = null;
        final long start = 300_000L;
        final long end = 500_000L;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end));

        InputStream inputStream1 = mock(InputStream.class);
        stubChunkWithSummary(start, end, new DataSummary(start, start, 150_000L), inputStream1);

        assertNextStream(extractor, inputStream1);
        assertNoMoreData(extractor);

        verify(dataExtractorFactory, times(2)).newExtractor(start, end);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenEsqlQueryAndSpecifiedChunk() throws IOException {
        chunkSpan = TimeValue.timeValueSeconds(1);
        final long start = 1_000L;
        final long end = 2_000L;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end));

        InputStream inputStream1 = mock(InputStream.class);
        stubChunkWithSummary(start, end, new DataSummary(start, end, 10L), inputStream1);

        assertNextStream(extractor, inputStream1);
        assertNoMoreData(extractor);

        verify(dataExtractorFactory, times(2)).newExtractor(start, end);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenEsqlQueryAndNoData() throws IOException {
        chunkSpan = null;
        final long start = 1_000L;
        final long end = 5_000L;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end));

        stubSummary(start, end, new DataSummary(null, null, 0L));

        assertThat(extractor.hasNext(), is(true));
        assertThat(extractor.next().data().isPresent(), is(false));
        assertThat(extractor.hasNext(), is(false));

        verify(dataExtractorFactory).newExtractor(start, end);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenEsqlQueryAndSummaryEarliestTimeBeforeStart() throws IOException {
        chunkSpan = null;
        final long start = 3_600_000L;
        final long end = 7_200_000L;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end));

        InputStream inputStream1 = mock(InputStream.class);
        stubChunkWithSummary(start, end, new DataSummary(0L, 3_700_000L, 1L), inputStream1);

        assertNextStream(extractor, inputStream1);
        assertNoMoreData(extractor);

        verify(dataExtractorFactory, times(2)).newExtractor(start, end);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testEsqlWindowShouldAlignToDeclaredGroupingInterval() throws IOException {
        final long groupingInterval = 60_000L;
        final long alignedStart = 180_000L;
        final long alignedEnd = 300_000L;
        ChunkedDataExtractorContext context = createEsqlContext(125_000L, 305_000L, groupingInterval);
        assertThat(context.start(), equalTo(alignedStart));
        assertThat(context.end(), equalTo(alignedEnd));

        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, context);

        stubChunkWithSummary(alignedStart, alignedEnd, new DataSummary(alignedStart, alignedEnd, 10L), mock(InputStream.class));

        assertThat(extractor.hasNext(), is(true));
        extractor.next();

        verify(dataExtractorFactory, times(2)).newExtractor(alignedStart, alignedEnd);
    }

    public void testEsqlChunkSpanShouldBeAMultipleOfGroupingInterval() throws IOException {
        final long groupingInterval = 60_000L;
        final long start = 0L;
        final long end = 600_000L;
        chunkSpan = null;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end, groupingInterval));

        // timeSpread=500_000, totalHits=750 -> raw span=333_333 -> ceil to 360_000 (6 * 60_000)
        stubSummary(start, end, new DataSummary(start, 500_000L, 750L));
        stubChunk(start, 360_000L, mock(InputStream.class));

        assertThat(extractor.hasNext(), is(true));
        extractor.next();

        verify(dataExtractorFactory).newExtractor(start, 360_000L);
    }

    // Prior to elastic-workspace-g2sz.1's fix, currentStart was deliberately never advanced to the
    // summary's earliest time for ES|QL datafeeds (see git history on ChunkedDataExtractor around this
    // guard), out of caution that the summary's MIN(??timeField) reflected the user's own pipeline (which
    // could transform the time field, e.g. DATE_TRUNC) and might therefore run ahead of the true source
    // data. EsqlDataExtractor#getSummary() no longer runs the user's pipeline to compute earliest/latest --
    // it always queries the source time field directly (see EsqlDataExtractor#fetchSourceRangeSummary) --
    // so that concern no longer applies, and skipping the forward jump was actively harmful: without it, an
    // unbounded preview/first real _start walked one chunk at a time across the *entire* configured range
    // (e.g. epoch to "now") instead of straight to where the data actually starts, which measured as a
    // ~750x slowdown vs. the DSL equivalent on a real repro (see the bead for the measurement).
    public void testSummaryEarliestAheadOfWindowStartShouldAdvanceCurrentStart() throws IOException {
        final long groupingInterval = 60_000L;
        final long start = 0L;
        final long end = 960_000L;
        chunkSpan = null;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end, groupingInterval));

        // earliest (480_000, already grouping-interval-aligned) is far ahead of the window start (0);
        // chunkSpan works out larger than the remaining range, so the chunk end is capped at
        // context.end() (960_000), not derived from earliest + chunkSpan.
        stubSummary(start, end, new DataSummary(480_000L, 540_000L, 1L));
        stubChunk(480_000L, end, mock(InputStream.class));

        assertThat(extractor.hasNext(), is(true));
        extractor.next();

        verify(dataExtractorFactory).newExtractor(480_000L, end);
        // newExtractor(start, end) is called exactly once -- for the summary probe that discovers
        // earliest=480_000. Pre-fix, currentStart never advanced, so the actual data fetch re-issued the
        // *same* (start, end) call a second time instead of jumping straight to (480_000, end); this
        // assertion catches that regression.
        verify(dataExtractorFactory, times(1)).newExtractor(start, end);
    }

    public void testSummaryEarliestBehindWindowStartShouldNotRewindCurrentStart() throws IOException {
        // Regression guard for the DATE_TRUNC-style scenario the pre-fix guard was meant to protect
        // against: even though getSummary() no longer runs the user's pipeline for earliest/latest, a
        // resumed/continuing datafeed can legitimately re-query a window whose start is already ahead of
        // the full index's raw earliest doc (e.g. mid-history, after prior checkpoints) -- currentStart
        // must never rewind behind the window it was asked to search.
        final long groupingInterval = 60_000L;
        final long start = 180_000L;
        final long end = 600_000L;
        chunkSpan = null;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end, groupingInterval));

        // earliest (60_000) is behind the window start (180_000); timeSpread=latest-earliest=120_000 ->
        // chunkSpan = max(MIN_CHUNK_SPAN, 500 * 120_000 / 500) = 120_000, already a grouping-interval
        // multiple.
        stubSummary(start, end, new DataSummary(60_000L, 180_000L, 500L));
        stubChunk(start, 300_000L, mock(InputStream.class));

        assertThat(extractor.hasNext(), is(true));
        extractor.next();

        verify(dataExtractorFactory).newExtractor(start, 300_000L);
        verify(dataExtractorFactory, times(0)).newExtractor(60_000L, 300_000L);
    }

    public void testGroupedRowsShouldNotBeSplitAcrossChunks() throws IOException {
        chunkSpan = TimeValue.timeValueMillis(90_000L);
        final long groupingInterval = 60_000L;
        final long start = 0L;
        final long end = 300_000L;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end, groupingInterval));

        stubSummary(start, end, new DataSummary(start, end, 10L));
        stubChunk(start, 120_000L, mock(InputStream.class));
        stubChunk(120_000L, 240_000L, mock(InputStream.class));
        stubChunk(240_000L, end, mock(InputStream.class));

        assertThat(extractor.hasNext(), is(true));
        extractor.next();
        extractor.next();
        extractor.next();

        verify(dataExtractorFactory).newExtractor(start, 120_000L);
        verify(dataExtractorFactory).newExtractor(120_000L, 240_000L);
        verify(dataExtractorFactory).newExtractor(240_000L, end);
    }

    public void testExtractionGivenEsqlQueryAndAutoChunkAndTotalTimeRangeSmallerThanChunk() throws IOException {
        chunkSpan = null;
        final long start = 1_000L;
        final long end = 11_000L;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(start, end));

        InputStream inputStream1 = mock(InputStream.class);
        stubChunkWithSummary(start, end, new DataSummary(start, end, 10L), inputStream1);

        assertNextStream(extractor, inputStream1);
        assertNoMoreData(extractor);

        verify(dataExtractorFactory, times(2)).newExtractor(start, end);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testExtractionGivenEsqlQueryAndIntermediateEmptySearchShouldReconfigure() throws IOException {
        chunkSpan = null;
        DataExtractor extractor = new ChunkedDataExtractor(dataExtractorFactory, createEsqlContext(100_000L, 400_000L));

        stubSummary(100_000L, 400_000L, new DataSummary(100_000L, 400_000L, 1_500L));
        InputStream inputStream1 = mock(InputStream.class);
        stubChunk(100_000L, 200_000L, inputStream1);
        stubChunk(200_000L, 300_000L);

        assertNextStream(extractor, inputStream1);
        assertThat(extractor.hasNext(), is(true));

        InputStream inputStream2 = mock(InputStream.class);
        stubChunkWithSummary(300_000L, 400_000L, new DataSummary(300_000L, 400_000L, 500L), inputStream2);

        assertNextStream(extractor, inputStream2);
        assertNoMoreData(extractor);

        verify(dataExtractorFactory).newExtractor(100_000L, 400_000L);
        verify(dataExtractorFactory).newExtractor(100_000L, 200_000L);
        verify(dataExtractorFactory).newExtractor(200_000L, 300_000L);
        verify(dataExtractorFactory, times(2)).newExtractor(300_000L, 400_000L);
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    /**
     * Interval lengths of an ES|QL chunk that is truncated and shrunk {@code shrinks} times: the first entry is the initial
     * chunk and every further one the result of a shrink (the interval is cut by the ratio of the 500 row target to the
     * 10,000 row limit, i.e. divided by 20). The last entry is {@code lastLength}.
     */
    private static long[] shrinkLengths(int shrinks, long lastLength) {
        long[] lengths = new long[shrinks + 1];
        long length = lastLength;
        for (int i = shrinks; i >= 0; i--) {
            lengths[i] = length;
            length = Math.multiplyExact(length, 20L);
        }
        return lengths;
    }

    private static class StubSubExtractor implements DataExtractor {

        private final DataSummary summary;
        private final SearchInterval searchInterval;
        private final List<InputStream> streams = new ArrayList<>();
        private final long rowCount;
        private boolean hasNext = true;

        StubSubExtractor(SearchInterval searchInterval, InputStream... streams) {
            this(searchInterval, null, -1L, streams);
        }

        StubSubExtractor(SearchInterval searchInterval, DataSummary summary, InputStream... streams) {
            this(searchInterval, summary, -1L, streams);
        }

        StubSubExtractor(SearchInterval searchInterval, long rowCount, InputStream... streams) {
            this(searchInterval, null, rowCount, streams);
        }

        StubSubExtractor(SearchInterval searchInterval, DataSummary summary, long rowCount, InputStream... streams) {
            this.searchInterval = searchInterval;
            this.summary = summary;
            this.rowCount = rowCount;
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
                return new Result(searchInterval, Optional.empty(), List.of());
            }
            return new Result(searchInterval, Optional.of(streams.remove(0)), List.of(), rowCount);
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

    private static class StubSubExtractorWithClusterStates extends StubSubExtractor {
        private final List<LinkedClusterState> linkedClusterStates;

        StubSubExtractorWithClusterStates(
            SearchInterval searchInterval,
            List<LinkedClusterState> linkedClusterStates,
            InputStream... streams
        ) {
            super(searchInterval, streams);
            this.linkedClusterStates = linkedClusterStates;
        }

        @Override
        public Result next() {
            Result base = super.next();
            return new Result(base.searchInterval(), base.data(), linkedClusterStates);
        }
    }

    /**
     * A summary extractor that throws {@link ResourceNotFoundException} from {@link #getSummary()} to simulate
     * a CCS search where a remote cluster is skipped. The skipped cluster states are available via
     * {@link #getLinkedClusterStates()} so that callers can capture them before the exception propagates.
     */
    private static class StubSummaryExtractorThrowingWithClusterStates implements DataExtractor {
        private final List<LinkedClusterState> linkedClusterStates;

        StubSummaryExtractorThrowingWithClusterStates(List<LinkedClusterState> linkedClusterStates) {
            this.linkedClusterStates = linkedClusterStates;
        }

        @Override
        public DataSummary getSummary() {
            throw new ResourceNotFoundException("remote cluster skipped");
        }

        @Override
        public List<LinkedClusterState> getLinkedClusterStates() {
            return linkedClusterStates;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Result next() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void cancel() {}

        @Override
        public void destroy() {}

        @Override
        public long getEndTime() {
            return 0;
        }
    }
}
