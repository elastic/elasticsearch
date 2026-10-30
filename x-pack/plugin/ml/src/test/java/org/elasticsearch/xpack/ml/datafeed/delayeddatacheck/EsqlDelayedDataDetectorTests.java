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

    private static final long BUCKET_SPAN_MS = 60_000L;
    private static final long WINDOW_MS = 600_000L;
    private static final String JOB_ID = "test-job";
    private static final String TIME_FIELD = "ts";
    private static final String COUNT_FIELD = "event_count";
    private static final long LATEST_MS = 660_000L;
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
        Bucket unchanged = newBucket(120_000L, 5L);
        Bucket withMissing = newBucket(180_000L, 3L);
        Bucket dataRemoved = newBucket(240_000L, 5L);
        stubExtractorAndBuckets(
            START_MS,
            LATEST_MS,
            List.of(unchanged, withMissing, dataRemoved),
            ndjson("{\"ts\":120000,\"event_count\":5}\n{\"ts\":180000,\"event_count\":7}\n{\"ts\":240000,\"event_count\":3}")
        );

        List<BucketWithMissingData> missing = newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getBucket(), equalTo(withMissing));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(4L));
    }

    public void testMultipleRowsAndBatchesWithBlankLinesShouldBeSummed() {
        stubExtractorAndBuckets(
            START_MS,
            LATEST_MS,
            List.of(newBucket(120_000L, 4L)),
            ndjson("{\"ts\":120000,\"event_count\":1}\n\n{\"ts\":120000,\"event_count\":1}\n"),
            null,
            ndjson("{\"ts\":120000,\"event_count\":3}")
        );

        List<BucketWithMissingData> missing = newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(1L));
    }

    public void testGroupingIntervalAboveBucketSpanShouldAlignSourceRangeAndLabelByEmittedTime() {
        long groupingIntervalMs = 120_000L;
        stubExtractorAndBuckets(0L, 600_000L, List.of(newBucket(120_000L, 3L)), ndjson("{\"ts\":150000,\"event_count\":5}"));

        List<BucketWithMissingData> missing = newDetector(groupingIntervalMs).detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getTimeStamp(), equalTo(120L));
        verify(dataExtractorFactory).newExtractor(0L, 600_000L);
    }

    public void testWindowSmallerThanAlignmentShouldReturnEmptyWithoutQuerying() {
        List<BucketWithMissingData> missing = newDetectorWithWindow(500L).detectMissingData(1_000L);

        assertThat(missing, is(Collections.emptyList()));
        verify(dataExtractorFactory, never()).newExtractor(anyLong(), anyLong());
    }

    public void testWindowOfSeveralBucketsShouldRequestAllFinalizedBucketsInOnePage() {
        stubExtractorAndBuckets(START_MS, LATEST_MS, List.of(newBucket(120_000L, 3L)), ndjson("{\"ts\":120000,\"event_count\":5}"));

        newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS);

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
            StubDataExtractor extractor = stubExtractorAndBuckets(START_MS, LATEST_MS, List.of(newBucket(120_000L, 1L)), ndjson(record));

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS)
            );
            assertThat(record, e.getMessage(), containsString(TIME_FIELD));
            assertThat(record, extractor.destroyed, is(true));
        }
    }

    public void testBatchStreamsShouldAllBeClosedAndExtractorDestroyed() {
        TrackingStream first = new TrackingStream("{\"ts\":120000,\"event_count\":3}");
        TrackingStream second = new TrackingStream("{\"ts\":120000,\"event_count\":2}");
        StubDataExtractor extractor = new StubDataExtractor(first, second);
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenReturn(extractor);
        stubBuckets(List.of(newBucket(120_000L, 4L)));

        newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS);

        assertThat(first.closed, is(true));
        assertThat(second.closed, is(true));
        assertThat(extractor.destroyed, is(true));
    }

    public void testIoFailureShouldBeWrappedAndExtractorDestroyed() {
        StubDataExtractor extractor = new StubDataExtractor((InputStream) null) {
            @Override
            public Result next() throws IOException {
                throw new IOException("simulated read error");
            }
        };
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenReturn(extractor);
        stubBuckets(List.of(newBucket(120_000L, 1L)));

        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS));

        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertThat(extractor.destroyed, is(true));
    }

    public void testSummaryCountValuesShouldBeInterpretedLikeAutodetect() {
        // Autodetect (CDataGatherer::extractCountFromField) parses a double, ignores null/unparseable/negative, and adds 0.5 and truncates.
        // JSON literal of the summary count, then the count autodetect would use (0 = record ignored).
        String[][] cases = {
            { "1.6", "2" },
            { "0.4", "0" },
            { "2.5", "3" },
            { "-3", "0" },
            { "null", "0" },
            { "\"3\"", "3" },
            { "\"3.4\"", "3" },
            { "\"many\"", "0" },
            { "\"NaN\"", "0" },
            { "\"Infinity\"", "0" },
            { "\"3d\"", "0" },
            { "\"3f\"", "0" },
            { "\"0x10\"", "0" },
            { "\"1e3\"", "1000" },
            { "\"+3\"", "3" },
            { "\"\"", "0" },
            { "\" 4 \"", "4" },
            { "9007199254740993", "9007199254740992" },
            { "9223372036854775807", String.valueOf(Long.MAX_VALUE) } };
        for (String[] c : cases) {
            stubExtractorAndBuckets(
                START_MS,
                LATEST_MS,
                List.of(newBucket(120_000L, 0L)),
                ndjson("{\"ts\":120000,\"event_count\":" + c[0] + "}")
            );

            List<BucketWithMissingData> missing = newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS);

            assertThat(c[0], missing.size(), equalTo(c[1].equals("0") ? 0 : 1));
            if (missing.isEmpty() == false) {
                assertThat(c[0], missing.get(0).getMissingDocumentCount(), equalTo(Long.parseLong(c[1])));
            }
        }
    }

    public void testCancelBeforeExtractorIsPublishedShouldReportNothing() {
        EsqlDelayedDataDetector detector = newDetector(BUCKET_SPAN_MS);
        StubDataExtractor extractor = new StubDataExtractor(ndjson("{\"ts\":120000,\"event_count\":9}"));
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenAnswer(invocation -> {
            detector.cancel();
            return extractor;
        });
        stubBuckets(List.of(newBucket(120_000L, 1L)));

        assertThat(detector.detectMissingData(LATEST_MS), is(Collections.emptyList()));
        assertThat(extractor.cancelled, is(true));
        assertThat(extractor.destroyed, is(true));
    }

    public void testCancelDuringRequeryShouldReportNothingEvenIfExtractorThrows() {
        EsqlDelayedDataDetector detector = newDetector(BUCKET_SPAN_MS);
        StubDataExtractor extractor = new StubDataExtractor(ndjson("{\"ts\":120000,\"event_count\":9}")) {
            @Override
            public Result next() {
                detector.cancel();
                throw new IllegalStateException("extractor cancelled");
            }
        };
        when(dataExtractorFactory.newExtractor(START_MS, LATEST_MS)).thenReturn(extractor);
        stubBuckets(List.of(newBucket(120_000L, 1L)));

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
        stubBuckets(List.of(newBucket(120_000L, 1L)));

        expectThrows(IllegalStateException.class, () -> newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS));
        assertThat(extractor.destroyed, is(true));
    }

    public void testTruncatedRequeryShouldReportNothing() {
        StubDataExtractor extractor = stubExtractorAndBuckets(
            START_MS,
            LATEST_MS,
            List.of(newBucket(120_000L, 3L)),
            ndjson("{\"ts\":120000,\"event_count\":5}")
        );
        extractor.incomplete = new SearchInterval(200_000L, 300_000L);

        assertThat(newDetector(BUCKET_SPAN_MS).detectMissingData(LATEST_MS), is(Collections.emptyList()));
        assertThat(extractor.destroyed, is(true));
    }

    private EsqlDelayedDataDetector newDetector(long groupingIntervalMs) {
        return newDetector(groupingIntervalMs, WINDOW_MS);
    }

    private EsqlDelayedDataDetector newDetectorWithWindow(long windowMs) {
        return newDetector(BUCKET_SPAN_MS, windowMs);
    }

    private EsqlDelayedDataDetector newDetector(long groupingIntervalMs, long windowMs) {
        return new EsqlDelayedDataDetector(
            BUCKET_SPAN_MS,
            windowMs,
            JOB_ID,
            TIME_FIELD,
            groupingIntervalMs,
            COUNT_FIELD,
            dataExtractorFactory,
            client
        );
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
            Collections.addAll(this.batches, batches);
        }

        @Override
        public DataSummary getSummary() {
            return null;
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
