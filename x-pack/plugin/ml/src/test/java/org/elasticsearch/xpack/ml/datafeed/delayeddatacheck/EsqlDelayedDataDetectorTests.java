/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EsqlDelayedDataDetectorTests extends ESTestCase {

    private static final long BUCKET_SPAN_MS = TimeValue.timeValueMinutes(1).millis();
    private static final long WINDOW_MS = TimeValue.timeValueMinutes(10).millis();
    private static final long BUCKET_TWO_MIN_MS = TimeValue.timeValueMinutes(2).millis();
    private static final long BUCKET_THREE_MIN_MS = TimeValue.timeValueMinutes(3).millis();
    private static final long BUCKET_FOUR_MIN_MS = TimeValue.timeValueMinutes(4).millis();
    private static final String JOB_ID = "test-job";
    private static final String TIME_FIELD = "ts";
    private static final String COUNT_FIELD = "event_count";
    private static final long LATEST_MS = BUCKET_SPAN_MS + WINDOW_MS;
    private static final long START_MS = LATEST_MS - WINDOW_MS;

    private Client client;
    private DataExtractorFactory dataExtractorFactory;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        dataExtractorFactory = mock(DataExtractorFactory.class);
    }

    public void testBucketsWithUnchangedOrRemovedDocsShouldNotBeReported() {
        Bucket unchanged = newBucket(BUCKET_TWO_MIN_MS, 5L);
        Bucket withMissing = newBucket(BUCKET_THREE_MIN_MS, 3L);
        Bucket dataRemoved = newBucket(BUCKET_FOUR_MIN_MS, 5L);
        stubExtractorAndBuckets(
            START_MS,
            LATEST_MS,
            List.of(unchanged, withMissing, dataRemoved),
            ndjson(row(BUCKET_TWO_MIN_MS, 5) + "\n" + row(BUCKET_THREE_MIN_MS, 7) + "\n" + row(BUCKET_FOUR_MIN_MS, 3))
        );

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getBucket(), equalTo(withMissing));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(4L));
    }

    public void testMultipleRowsAndBatchesWithBlankLinesShouldBeSummed() {
        stubExtractorAndBuckets(
            START_MS,
            LATEST_MS,
            List.of(newBucket(BUCKET_TWO_MIN_MS, 4L)),
            ndjson(row(BUCKET_TWO_MIN_MS, 1) + "\n\n" + row(BUCKET_TWO_MIN_MS, 1) + "\n"),
            ndjson(row(BUCKET_TWO_MIN_MS, 3))
        );

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(1L));
    }

    public void testWindowSmallerThanAlignmentShouldReturnEmptyWithoutQuerying() {
        List<BucketWithMissingData> missing = newDetectorWithWindow(500L).detectMissingData(1_000L);

        assertThat(missing, is(Collections.emptyList()));
        verify(dataExtractorFactory, never()).newExtractor(anyLong(), anyLong());
    }

    public void testWindowOfSeveralBucketsShouldRequestAllFinalizedBucketsInOnePage() {
        stubExtractorAndBuckets(START_MS, LATEST_MS, List.of(newBucket(BUCKET_TWO_MIN_MS, 3L)), ndjson(row(BUCKET_TWO_MIN_MS, 5)));

        newDetector().detectMissingData(LATEST_MS);

        ArgumentCaptor<GetBucketsAction.Request> captor = ArgumentCaptor.forClass(GetBucketsAction.Request.class);
        verify(client).execute(eq(GetBucketsAction.INSTANCE), captor.capture());
        GetBucketsAction.Request request = captor.getValue();
        assertThat(request.getJobId(), equalTo(JOB_ID));
        assertThat(request.getStart(), equalTo(Long.toString(START_MS)));
        assertThat(request.getEnd(), equalTo(Long.toString(LATEST_MS)));
        assertThat(request.isExcludeInterim(), is(true));
        assertThat(request.getPageParams().getFrom(), equalTo(0));
        assertThat(request.getPageParams().getSize(), equalTo((int) (WINDOW_MS / BUCKET_SPAN_MS)));
    }

    public void testInvalidEmittedTimeShouldThrowEvenWhenCountIsNull() {
        for (String record : List.of(
            "{\"event_count\":null}",
            "{\"ts\":[120000],\"event_count\":null}",
            "{\"ts\":\"not-a-date\",\"event_count\":3}",
            "{\"ts\":59999,\"event_count\":3}",
            "{\"ts\":660000,\"event_count\":3}"
        )) {
            StubDataExtractor extractor = stubExtractorAndBuckets(
                START_MS,
                LATEST_MS,
                List.of(newBucket(BUCKET_TWO_MIN_MS, 1L)),
                ndjson(record)
            );

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newDetector().detectMissingData(LATEST_MS));
            assertThat(record, e.getMessage(), containsString(TIME_FIELD));
            assertThat(record, extractor.destroyed, is(true));
        }
    }

    public void testBatchStreamsShouldAllBeClosedAndExtractorDestroyed() {
        TrackingStream first = new TrackingStream(row(BUCKET_TWO_MIN_MS, 3));
        TrackingStream second = new TrackingStream(row(BUCKET_TWO_MIN_MS, 2));
        StubDataExtractor extractor = new StubDataExtractor(first, second);
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenReturn(extractor);
        stubBuckets(List.of(newBucket(BUCKET_TWO_MIN_MS, 4L)));

        newDetector().detectMissingData(LATEST_MS);

        assertThat(first.closed, is(true));
        assertThat(second.closed, is(true));
        assertThat(extractor.destroyed, is(true));
    }

    public void testIoFailureShouldBeWrappedAndExtractorDestroyed() {
        StubDataExtractor extractor = new StubDataExtractor() {
            @Override
            public boolean hasNext() {
                return destroyed == false && cancelled == false;
            }

            @Override
            public Result next() throws IOException {
                throw new IOException("simulated read error");
            }
        };
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenReturn(extractor);
        stubBuckets(List.of(newBucket(BUCKET_TWO_MIN_MS, 1L)));

        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> newDetector().detectMissingData(LATEST_MS));

        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertThat(extractor.destroyed, is(true));
    }

    public void testParseSummaryCountShouldMatchAutodetectRounding() {
        assertThat(EsqlDelayedDataDetector.parseSummaryCount(1.6d), equalTo(2L));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount(0.4d), equalTo(EsqlDelayedDataDetector.IGNORED_SUMMARY_COUNT));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount(2.5d), equalTo(3L));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount(-3), equalTo(EsqlDelayedDataDetector.IGNORED_SUMMARY_COUNT));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount(null), equalTo(EsqlDelayedDataDetector.IGNORED_SUMMARY_COUNT));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount("3"), equalTo(3L));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount("3.4"), equalTo(3L));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount("many"), equalTo(EsqlDelayedDataDetector.IGNORED_SUMMARY_COUNT));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount("NaN"), equalTo(EsqlDelayedDataDetector.IGNORED_SUMMARY_COUNT));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount("3d"), equalTo(EsqlDelayedDataDetector.IGNORED_SUMMARY_COUNT));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount("1e3"), equalTo(1000L));
        assertThat(EsqlDelayedDataDetector.parseSummaryCount(" 4 "), equalTo(4L));
    }

    public void testRoundedSummaryCountInNdjsonShouldAppearAsMissingDocuments() {
        stubExtractorAndBuckets(START_MS, LATEST_MS, List.of(newBucket(BUCKET_TWO_MIN_MS, 0L)), ndjson(row(BUCKET_TWO_MIN_MS, 1.6)));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
    }

    public void testCancelBeforeExtractorIsPublishedShouldReportNothing() {
        EsqlDelayedDataDetector detector = newDetector();
        StubDataExtractor extractor = new StubDataExtractor(ndjson(row(BUCKET_TWO_MIN_MS, 9)));
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenAnswer(invocation -> {
            detector.cancel();
            return extractor;
        });
        stubBuckets(List.of(newBucket(BUCKET_TWO_MIN_MS, 1L)));

        assertThat(detector.detectMissingData(LATEST_MS), is(Collections.emptyList()));
        assertThat(extractor.cancelled, is(true));
        assertThat(extractor.destroyed, is(true));
    }

    public void testCancelDuringRequeryShouldReportNothingEvenIfExtractorThrows() {
        EsqlDelayedDataDetector detector = newDetector();
        StubDataExtractor extractor = new StubDataExtractor(ndjson(row(BUCKET_TWO_MIN_MS, 9))) {
            @Override
            public Result next() {
                detector.cancel();
                throw new IllegalStateException("extractor cancelled");
            }
        };
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenReturn(extractor);
        stubBuckets(List.of(newBucket(BUCKET_TWO_MIN_MS, 1L)));

        assertThat(detector.detectMissingData(LATEST_MS), is(Collections.emptyList()));
        assertThat(extractor.destroyed, is(true));
    }

    public void testRuntimeFailureWithoutCancelShouldPropagateAndDestroyExtractor() {
        StubDataExtractor extractor = new StubDataExtractor(ndjson("{}")) {
            @Override
            public Result next() {
                throw new IllegalStateException("boom");
            }
        };
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenReturn(extractor);
        stubBuckets(List.of(newBucket(BUCKET_TWO_MIN_MS, 1L)));

        expectThrows(IllegalStateException.class, () -> newDetector().detectMissingData(LATEST_MS));
        assertThat(extractor.destroyed, is(true));
    }

    public void testTruncatedRequeryShouldReportNothing() {
        StubDataExtractor extractor = stubExtractorAndBuckets(
            START_MS,
            LATEST_MS,
            List.of(newBucket(BUCKET_TWO_MIN_MS, 3L)),
            ndjson(row(BUCKET_TWO_MIN_MS, 5))
        );
        extractor.incomplete = new SearchInterval(200_000L, 300_000L);

        assertThat(newDetector().detectMissingData(LATEST_MS), is(Collections.emptyList()));
        assertThat(extractor.destroyed, is(true));
    }

    private EsqlDelayedDataDetector newDetector() {
        return newDetector(WINDOW_MS);
    }

    private EsqlDelayedDataDetector newDetectorWithWindow(long windowMs) {
        return new EsqlDelayedDataDetector(BUCKET_SPAN_MS, windowMs, JOB_ID, TIME_FIELD, COUNT_FIELD, dataExtractorFactory, client);
    }

    private EsqlDelayedDataDetector newDetector(long windowMs) {
        return new EsqlDelayedDataDetector(BUCKET_SPAN_MS, windowMs, JOB_ID, TIME_FIELD, COUNT_FIELD, dataExtractorFactory, client);
    }

    private static String row(long timeMs, Object count) {
        return "{\"ts\":" + timeMs + ",\"event_count\":" + count + "}";
    }

    private static Bucket newBucket(long epochMs, long eventCount) {
        Bucket bucket = new Bucket(JOB_ID, new Date(epochMs), BUCKET_SPAN_MS / 1000);
        bucket.setEventCount(eventCount);
        return bucket;
    }

    private StubDataExtractor stubExtractorAndBuckets(long start, long end, List<Bucket> buckets, InputStream... batches) {
        StubDataExtractor extractor = new StubDataExtractor(batches);
        when(dataExtractorFactory.newExtractor(start, end)).thenReturn(extractor);
        stubBuckets(buckets);
        return extractor;
    }

    @SuppressWarnings("unchecked")
    private void stubBuckets(List<Bucket> buckets) {
        QueryPage<Bucket> page = new QueryPage<>(buckets, buckets.size(), Bucket.RESULTS_FIELD);
        ActionFuture<GetBucketsAction.Response> future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(new GetBucketsAction.Response(page));
        when(client.execute(eq(GetBucketsAction.INSTANCE), any(GetBucketsAction.Request.class))).thenReturn(future);
    }

    private static InputStream ndjson(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private static class TrackingStream extends ByteArrayInputStream {
        boolean closed;

        TrackingStream(String content) {
            super(content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static class StubDataExtractor implements DataExtractor {
        private final List<InputStream> batches = new ArrayList<>();
        private int next;
        boolean destroyed;
        boolean cancelled;
        SearchInterval incomplete;

        StubDataExtractor(InputStream... batches) {
            for (InputStream batch : batches) {
                if (batch != null) {
                    this.batches.add(batch);
                }
            }
        }

        @Override
        public DataSummary getSummary() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            return cancelled == false && next < batches.size();
        }

        @Override
        public Result next() throws IOException {
            return new Result(new SearchInterval(0L, 1L), Optional.ofNullable(batches.get(next++)), List.of());
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public void destroy() {
            destroyed = true;
        }

        @Override
        public long getEndTime() {
            return 0L;
        }

        @Override
        public Optional<SearchInterval> getIncompleteSearchInterval() {
            return Optional.ofNullable(incomplete);
        }
    }
}
