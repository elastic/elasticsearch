/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EsqlDelayedDataDetectorTests extends ESTestCase {

    private static final long BUCKET_SPAN_MS = 60_000L;
    private static final long WINDOW_MS = 600_000L;
    private static final String JOB_ID = "test-job";
    private static final String TIME_FIELD = "ts";
    private static final String SOURCE_TIME_FIELD = "source_ts";
    private static final String COUNT_FIELD = "event_count";
    private static final long LATEST_MS = 660_000L;
    private static final long END_MS = LATEST_MS;
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

    public void testGetWindowReturnsConfiguredWindow() {
        EsqlDelayedDataDetector detector = newDetector();
        assertThat(detector.getWindow(), equalTo(WINDOW_MS));
    }

    public void testDetectMissingDataGivenWindowAlignment() {
        Bucket bucket = newBucket(120_000L, 3L);
        stubQueryAndBuckets(START_MS, END_MS, ndjson("{\"ts\":120000,\"event_count\":5}"), List.of(bucket));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
        assertThat(missing.get(0).getBucket(), equalTo(bucket));
    }

    public void testDetectMissingDataShouldUseGroupingIntervalForSourceRangeAndEmittedTimeForBucketLabels() {
        long groupingIntervalMs = 120_000L;
        long latestMs = 660_000L;
        long expectedStart = 0L;
        long expectedEnd = 600_000L;
        Bucket bucket = newBucket(120_000L, 3L);
        stubQueryAndBuckets(expectedStart, expectedEnd, ndjson("{\"ts\":120000,\"event_count\":5}"), List.of(bucket));

        List<BucketWithMissingData> missing = newDetector(BUCKET_SPAN_MS, WINDOW_MS, groupingIntervalMs).detectMissingData(latestMs);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getTimeStamp(), equalTo(120L));
        verify(dataExtractorFactory).newExtractor(expectedStart, expectedEnd);
    }

    public void testDetectMissingDataGivenEndEqualsStartReturnsEmptyList() {
        long smallWindowMs = 500L;
        EsqlDelayedDataDetector detector = newDetector(BUCKET_SPAN_MS, smallWindowMs);
        List<BucketWithMissingData> result = detector.detectMissingData(1_000L);
        assertThat(result, is(Collections.emptyList()));
    }

    public void testDetectMissingDataOnlyIncludesBucketsWithPositiveMissing() {
        Bucket bucketUnchanged = newBucket(120_000L, 5L);
        Bucket bucketWithMissing = newBucket(180_000L, 3L);
        Bucket bucketRemovedData = newBucket(240_000L, 5L);

        InputStream ndjson = ndjson(
            "{\"ts\":120000,\"event_count\":5}",
            "{\"ts\":180000,\"event_count\":7}",
            "{\"ts\":240000,\"event_count\":3}"
        );
        stubQueryAndBuckets(START_MS, END_MS, ndjson, List.of(bucketUnchanged, bucketWithMissing, bucketRemovedData));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getBucket(), equalTo(bucketWithMissing));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(4L));
    }

    public void testDetectMissingDataGivenEpochSecondsToMillisAlignment() {
        long bucketSpanMs = 5_000L;
        long windowMs = 50_000L;
        long latestMs = 55_000L;

        Bucket bucket = newBucket(10_000L, 2L, bucketSpanMs);
        stubQueryAndBuckets(5_000L, 55_000L, ndjson("{\"ts\":10000,\"event_count\":4}"), List.of(bucket));

        EsqlDelayedDataDetector detector = newDetector(bucketSpanMs, windowMs);
        List<BucketWithMissingData> missing = detector.detectMissingData(latestMs);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
    }

    public void testAccumulateBucketCountsGivenNullSummaryCountFieldRowIsSkipped() {
        InputStream ndjson = ndjson("{\"ts\":120000,\"event_count\":2}", "{\"ts\":120000}", "{\"ts\":120000,\"event_count\":3}");
        stubQueryAndBuckets(START_MS, END_MS, ndjson, List.of(newBucket(120_000L, 3L)));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
    }

    public void testAccumulateBucketCountsGivenNullSummaryCountShouldStillValidateEveryEmittedTime() {
        for (String record : List.of(
            "{\"event_count\":null}",
            "{\"ts\":null,\"event_count\":null}",
            "{\"ts\":[120000],\"event_count\":null}",
            "{\"ts\":\"not-a-date\",\"event_count\":null}",
            "{\"ts\":59999,\"event_count\":null}",
            "{\"ts\":660000,\"event_count\":null}"
        )) {
            stubQueryAndBuckets(START_MS, END_MS, ndjson(record), List.of(newBucket(120_000L, 1L)));

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newDetector().detectMissingData(LATEST_MS));
            assertThat(e.getMessage(), containsString(TIME_FIELD));
        }
    }

    public void testAccumulateBucketCountsGivenStringCountFieldThrowsIllegalArgument() {
        stubQueryAndBuckets(START_MS, END_MS, ndjson("{\"ts\":120000,\"event_count\":\"not-a-number\"}"), List.of(newBucket(120_000L, 1L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newDetector().detectMissingData(LATEST_MS));
        assertThat(e.getMessage(), containsString(COUNT_FIELD));
        assertThat(e.getMessage(), containsString("numeric"));
    }

    public void testAccumulateBucketCountsGivenUnsupportedTimeFieldThrowsIllegalArgument() {
        stubQueryAndBuckets(START_MS, END_MS, ndjson("{\"ts\":\"not-a-number\",\"event_count\":3}"), List.of(newBucket(120_000L, 1L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newDetector().detectMissingData(LATEST_MS));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString("date or numeric"));
    }

    public void testAccumulateBucketCountsGivenNullTimeFieldThrowsIllegalArgument() {
        StubDataExtractor extractor = stubQueryAndBuckets(
            START_MS,
            END_MS,
            ndjson("{\"event_count\":3}"),
            List.of(newBucket(120_000L, 3L))
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newDetector().detectMissingData(LATEST_MS));
        assertThat(e.getMessage(), containsString("non-null scalar"));
        assertThat(extractor.isDestroyed(), is(true));
    }

    public void testAccumulateBucketCountsGivenMultiValuedTimeFieldThrowsIllegalArgument() {
        stubQueryAndBuckets(START_MS, END_MS, ndjson("{\"ts\":[120000],\"event_count\":3}"), List.of(newBucket(120_000L, 1L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newDetector().detectMissingData(LATEST_MS));
        assertThat(e.getMessage(), containsString("multi-valued"));
    }

    public void testAccumulateBucketCountsGivenTimeBeforeSourceWindowThrowsIllegalArgument() {
        stubQueryAndBuckets(START_MS, END_MS, ndjson("{\"ts\":59999,\"event_count\":3}"), List.of(newBucket(120_000L, 1L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newDetector().detectMissingData(LATEST_MS));
        assertThat(e.getMessage(), containsString("before the source window start"));
    }

    public void testAccumulateBucketCountsGivenTimeAtSourceWindowEndThrowsIllegalArgument() {
        stubQueryAndBuckets(START_MS, END_MS, ndjson("{\"ts\":660000,\"event_count\":3}"), List.of(newBucket(120_000L, 1L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newDetector().detectMissingData(LATEST_MS));
        assertThat(e.getMessage(), containsString("at or after the source window end"));
    }

    public void testAccumulateBucketCountsGivenDateAndDateNanosTimeFieldsShouldConvertToEpochMillis() {
        stubQueryAndBuckets(
            START_MS,
            END_MS,
            ndjson(
                "{\"ts\":\"1970-01-01T00:02:00.000Z\",\"event_count\":2}",
                "{\"ts\":\"1970-01-01T00:02:00.123456789Z\",\"event_count\":3}"
            ),
            List.of(newBucket(120_000L, 3L))
        );

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
    }

    public void testAccumulateBucketCountsGivenBlankLinesAreSkipped() {
        String ndjson = "{\"ts\":120000,\"event_count\":5}\n\n\n";
        stubQueryAndBuckets(START_MS, END_MS, ndjsonStream(ndjson), List.of(newBucket(120_000L, 3L)));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);
        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
    }

    public void testDetectMissingDataAccumulatesAcrossMultipleBatches() {
        StubDataExtractor extractor = new StubDataExtractor(
            ndjson("{\"ts\":120000,\"event_count\":3}"),
            ndjson("{\"ts\":120000,\"event_count\":2}")
        );
        when(dataExtractorFactory.newExtractor(START_MS, END_MS)).thenReturn(extractor);
        stubBucketsResponse(START_MS, END_MS, List.of(newBucket(120_000L, 4L)));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(1L));
    }

    public void testDetectMissingDataGivenEmptyResultIsSkipped() throws IOException {
        StubDataExtractor extractor = new StubDataExtractor((InputStream) null, ndjson("{\"ts\":120000,\"event_count\":5}"));
        when(dataExtractorFactory.newExtractor(START_MS, END_MS)).thenReturn(extractor);
        stubBucketsResponse(START_MS, END_MS, List.of(newBucket(120_000L, 3L)));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
    }

    public void testDetectMissingDataDestroysExtractorAfterUse() throws IOException {
        StubDataExtractor extractor = stubQueryAndBuckets(
            START_MS,
            END_MS,
            ndjson("{\"ts\":120000,\"event_count\":5}"),
            List.of(newBucket(120_000L, 3L))
        );

        newDetector().detectMissingData(LATEST_MS);

        assertThat(extractor.isDestroyed(), is(true));
    }

    public void testDetectMissingDataGivenIoExceptionIsWrapped() {
        StubDataExtractor extractor = new ThrowingDataExtractor();
        when(dataExtractorFactory.newExtractor(START_MS, END_MS)).thenReturn(extractor);
        stubBucketsResponse(START_MS, END_MS, List.of(newBucket(120_000L, 1L)));

        UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> newDetector().detectMissingData(LATEST_MS));
        assertThat(e.getMessage(), containsString(JOB_ID));
        assertThat(e.getCause(), instanceOf(IOException.class));
    }

    public void testDetectMissingDataGivenNoBucketsReturnsEmptyList() throws IOException {
        stubQueryAndBuckets(START_MS, END_MS, ndjson("{\"ts\":120000,\"event_count\":5}"), Collections.emptyList());

        List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);
        assertThat(missing, is(Collections.emptyList()));
    }

    public void testDetectMissingDataGivenTruncatedReQueryShouldSkipReportingAndLogWarning() {
        Bucket bucket = newBucket(120_000L, 3L);
        SearchInterval incompleteInterval = new SearchInterval(200_000L, 300_000L);
        StubDataExtractor extractor = new StubDataExtractor(incompleteInterval, ndjson("{\"ts\":120000,\"event_count\":5}"));
        when(dataExtractorFactory.newExtractor(START_MS, END_MS)).thenReturn(extractor);
        stubBucketsResponse(START_MS, END_MS, List.of(bucket));

        try (var mockLog = MockLog.capture(EsqlDelayedDataDetector.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "warn on truncated delayed-data re-query",
                    EsqlDelayedDataDetector.class.getCanonicalName(),
                    Level.WARN,
                    "*" + JOB_ID + "*truncated*"
                )
            );

            List<BucketWithMissingData> missing = newDetector().detectMissingData(LATEST_MS);

            assertThat(missing, is(Collections.emptyList()));
            mockLog.assertAllExpectationsMatched();
        }
        assertThat(extractor.isDestroyed(), is(true));
    }

    public void testGetBucketEventsBuildsExpectedRequest() throws IOException {
        stubQueryAndBuckets(START_MS, END_MS, ndjson("{\"ts\":120000,\"event_count\":5}"), List.of(newBucket(120_000L, 3L)));

        newDetector().detectMissingData(LATEST_MS);

        ArgumentCaptor<GetBucketsAction.Request> requestCaptor = ArgumentCaptor.forClass(GetBucketsAction.Request.class);
        verify(client).execute(eq(GetBucketsAction.INSTANCE), requestCaptor.capture());
        GetBucketsAction.Request request = requestCaptor.getValue();
        assertThat(request.getJobId(), equalTo(JOB_ID));
        assertThat(request.getStart(), equalTo(Long.toString(START_MS)));
        assertThat(request.getEnd(), equalTo(Long.toString(END_MS)));
        assertThat(request.getSort(), equalTo("timestamp"));
        assertThat(request.isDescending(), is(false));
        assertThat(request.isExcludeInterim(), is(true));
        assertThat(request.getPageParams().getFrom(), equalTo(0));
        assertThat(request.getPageParams().getSize(), equalTo((int) ((END_MS - START_MS) / BUCKET_SPAN_MS)));
    }

    private EsqlDelayedDataDetector newDetector() {
        return newDetector(BUCKET_SPAN_MS, WINDOW_MS);
    }

    private EsqlDelayedDataDetector newDetector(long bucketSpanMs, long windowMs) {
        return newDetector(bucketSpanMs, windowMs, bucketSpanMs);
    }

    private EsqlDelayedDataDetector newDetector(long bucketSpanMs, long windowMs, long groupingIntervalMs) {
        return new EsqlDelayedDataDetector(
            bucketSpanMs,
            windowMs,
            JOB_ID,
            SOURCE_TIME_FIELD,
            TIME_FIELD,
            groupingIntervalMs,
            COUNT_FIELD,
            dataExtractorFactory,
            client
        );
    }

    private Bucket newBucket(long epochMs, long eventCount) {
        return newBucket(epochMs, eventCount, BUCKET_SPAN_MS);
    }

    private Bucket newBucket(long epochMs, long eventCount, long bucketSpanMs) {
        Bucket bucket = new Bucket(JOB_ID, new Date(epochMs), bucketSpanMs / 1000);
        bucket.setEventCount(eventCount);
        return bucket;
    }

    private StubDataExtractor stubQueryAndBuckets(long start, long end, InputStream ndjson, List<Bucket> buckets) {
        StubDataExtractor extractor = new StubDataExtractor(ndjson);
        when(dataExtractorFactory.newExtractor(start, end)).thenReturn(extractor);
        stubBucketsResponse(start, end, buckets);
        return extractor;
    }

    @SuppressWarnings("unchecked")
    private void stubBucketsResponse(long start, long end, List<Bucket> buckets) {
        QueryPage<Bucket> page = new QueryPage<>(buckets, buckets.size(), Bucket.RESULTS_FIELD);
        GetBucketsAction.Response response = new GetBucketsAction.Response(page);
        ActionFuture<GetBucketsAction.Response> future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(response);
        when(
            client.execute(
                eq(GetBucketsAction.INSTANCE),
                argThat(
                    (GetBucketsAction.Request request) -> Long.toString(start).equals(request.getStart())
                        && Long.toString(end).equals(request.getEnd())
                )
            )
        ).thenReturn(future);
    }

    private static InputStream ndjson(String... jsonLines) {
        String joined = String.join("\n", jsonLines);
        return ndjsonStream(joined);
    }

    private static InputStream ndjsonStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private static class StubDataExtractor implements DataExtractor {

        private final List<InputStream> batches;
        private final SearchInterval incompleteSearchInterval;
        private int nextIndex = 0;
        private boolean destroyed = false;

        StubDataExtractor(InputStream... batches) {
            this(null, batches);
        }

        StubDataExtractor(SearchInterval incompleteSearchInterval, InputStream... batches) {
            this.incompleteSearchInterval = incompleteSearchInterval;
            this.batches = Arrays.asList(batches);
        }

        @Override
        public DataSummary getSummary() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return nextIndex < batches.size();
        }

        @Override
        public Result next() throws IOException {
            return new Result(new SearchInterval(0L, 1L), Optional.ofNullable(batches.get(nextIndex++)), List.of());
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void cancel() {}

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
            return Optional.ofNullable(incompleteSearchInterval);
        }

        boolean isDestroyed() {
            return destroyed;
        }
    }

    private static class ThrowingDataExtractor extends StubDataExtractor {

        ThrowingDataExtractor() {
            super((InputStream) null);
        }

        @Override
        public Result next() throws IOException {
            throw new IOException("simulated read error");
        }
    }
}
