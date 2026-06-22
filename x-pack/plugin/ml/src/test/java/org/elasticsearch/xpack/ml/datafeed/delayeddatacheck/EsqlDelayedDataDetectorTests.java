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
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlDelayedDataDetectorTests extends ESTestCase {

    private static final long BUCKET_SPAN_MS = 60_000L;   // 1 minute
    private static final long WINDOW_MS = 600_000L;       // 10 minutes
    private static final String JOB_ID = "test-job";
    private static final String TIME_FIELD = "ts";
    private static final String COUNT_FIELD = "event_count";

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

    public void testDetectMissingDataGivenWindowAlignment() throws IOException {
        // latest = 660_000, bucketSpan = 60_000
        // end = alignToFloor(660_000, 60_000) = 660_000
        // start = alignToFloor(660_000 - 600_000, 60_000) = 60_000
        long latestMs = 660_000L;

        // One bucket at epoch-second 120 (= millis 120_000) with eventCount=3
        Bucket bucket = newBucket(120_000L, 3L);

        // Extractor returns one NDJSON row: ts=120_000, event_count=5 → missing = 5 - 3 = 2
        StubDataExtractor extractor = new StubDataExtractor(ndjson("{\"ts\":120000,\"event_count\":5}"));
        when(dataExtractorFactory.newExtractor(60_000L, 660_000L)).thenReturn(extractor);
        stubBucketsResponse(60_000L, 660_000L, List.of(bucket));

        EsqlDelayedDataDetector detector = newDetector();
        List<BucketWithMissingData> missing = detector.detectMissingData(latestMs);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
        assertThat(missing.get(0).getBucket(), equalTo(bucket));
    }

    public void testDetectMissingDataGivenEndEqualsStartReturnsEmptyList() {
        // The guard `if (end <= start) return emptyList()` fires when bucketSpan is larger than window,
        // so `latest` and `latest - window` both land in the same bucket.
        // bucketSpan = 60_000 (1 min), window = 500 ms, latest = 1_000 ms:
        // end = alignToFloor(1_000, 60_000) = 0
        // start = alignToFloor(1_000 - 500, 60_000) = alignToFloor(500, 60_000) = 0
        // end == start → return empty list without contacting the extractor or buckets API.
        long smallWindowMs = 500L;
        EsqlDelayedDataDetector detector = newDetector(BUCKET_SPAN_MS, smallWindowMs);
        List<BucketWithMissingData> result = detector.detectMissingData(1_000L);
        assertThat(result, is(Collections.emptyList()));
    }

    public void testDetectMissingDataOnlyIncludesBucketsWithPositiveMissing() throws IOException {
        long latestMs = 660_000L;

        // Bucket at 120_000 ms: eventCount=5, indexedCount=5 → missing=0 → excluded
        Bucket bucketUnchanged = newBucket(120_000L, 5L);
        // Bucket at 180_000 ms: eventCount=3, indexedCount=7 → missing=4 → included
        Bucket bucketWithMissing = newBucket(180_000L, 3L);
        // Bucket at 240_000 ms: eventCount=5, indexedCount=3 → missing=-2 (removed data) → excluded
        Bucket bucketRemovedData = newBucket(240_000L, 5L);

        InputStream ndjson = ndjson(
            "{\"ts\":120000,\"event_count\":5}",
            "{\"ts\":180000,\"event_count\":7}",
            "{\"ts\":240000,\"event_count\":3}"
        );
        StubDataExtractor extractor = new StubDataExtractor(ndjson);
        when(dataExtractorFactory.newExtractor(60_000L, 660_000L)).thenReturn(extractor);
        stubBucketsResponse(60_000L, 660_000L, List.of(bucketUnchanged, bucketWithMissing, bucketRemovedData));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(latestMs);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getBucket(), equalTo(bucketWithMissing));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(4L));
    }

    public void testDetectMissingDataGivenEpochSecondsToMillisAlignment() throws IOException {
        // Verifies that bucket.getEpoch() (seconds) is multiplied by 1000 to align with extractor
        // millis keys. Here bucketSpan=5_000 ms (5 s), bucket at epoch-second=10 (=millis 10_000).
        long bucketSpanMs = 5_000L;
        long windowMs = 50_000L;
        long latestMs = 55_000L;
        // end = alignToFloor(55_000, 5_000) = 55_000
        // start = alignToFloor(55_000 - 50_000 = 5_000, 5_000) = 5_000

        Bucket bucket = newBucket(10_000L, 2L, bucketSpanMs);
        // extractor row ts=10_000 ms → bucketStart=alignToFloor(10_000, 5_000)=10_000
        StubDataExtractor extractor = new StubDataExtractor(ndjson("{\"ts\":10000,\"event_count\":4}"));
        when(dataExtractorFactory.newExtractor(5_000L, 55_000L)).thenReturn(extractor);
        stubBucketsResponse(5_000L, 55_000L, List.of(bucket));

        EsqlDelayedDataDetector detector = newDetector(bucketSpanMs, windowMs);
        List<BucketWithMissingData> missing = detector.detectMissingData(latestMs);

        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L)); // 4 - 2
    }

    public void testAccumulateBucketCountsGivenMissingSummaryCountFieldThrows() throws IOException {
        long latestMs = 660_000L;
        // NDJSON row missing the summary_count_field_name ("event_count") entirely
        StubDataExtractor extractor = new StubDataExtractor(ndjson("{\"ts\":120000}"));
        when(dataExtractorFactory.newExtractor(60_000L, 660_000L)).thenReturn(extractor);
        stubBucketsResponse(60_000L, 660_000L, List.of(newBucket(120_000L, 1L)));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> newDetector().detectMissingData(latestMs));
        assertThat(e.getMessage(), containsString(COUNT_FIELD));
    }

    public void testAccumulateBucketCountsGivenNullTimeFieldRowIsSkipped() throws IOException {
        long latestMs = 660_000L;
        // First row has no time field (skipped); second row is normal
        InputStream ndjson = ndjson("{\"event_count\":3}", "{\"ts\":120000,\"event_count\":5}");
        StubDataExtractor extractor = new StubDataExtractor(ndjson);
        when(dataExtractorFactory.newExtractor(60_000L, 660_000L)).thenReturn(extractor);
        stubBucketsResponse(60_000L, 660_000L, List.of(newBucket(120_000L, 3L)));

        // Should not throw; row without ts is skipped; bucket at 120_000 sees 5-3=2 missing
        List<BucketWithMissingData> missing = newDetector().detectMissingData(latestMs);
        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
    }

    public void testAccumulateBucketCountsGivenBlankLinesAreSkipped() throws IOException {
        long latestMs = 660_000L;
        // Blank lines interspersed should be ignored
        String ndjson = "{\"ts\":120000,\"event_count\":5}\n\n\n";
        StubDataExtractor extractor = new StubDataExtractor(ndjsonStream(ndjson));
        when(dataExtractorFactory.newExtractor(60_000L, 660_000L)).thenReturn(extractor);
        stubBucketsResponse(60_000L, 660_000L, List.of(newBucket(120_000L, 3L)));

        List<BucketWithMissingData> missing = newDetector().detectMissingData(latestMs);
        assertThat(missing, hasSize(1));
        assertThat(missing.get(0).getMissingDocumentCount(), equalTo(2L));
    }

    public void testDetectMissingDataGivenIoExceptionIsWrapped() {
        long latestMs = 660_000L;
        StubDataExtractor extractor = new ThrowingDataExtractor();
        when(dataExtractorFactory.newExtractor(60_000L, 660_000L)).thenReturn(extractor);
        stubBucketsResponse(60_000L, 660_000L, List.of(newBucket(120_000L, 1L)));

        expectThrows(UncheckedIOException.class, () -> newDetector().detectMissingData(latestMs));
    }

    public void testDetectMissingDataGivenNoBucketsReturnsEmptyList() throws IOException {
        long latestMs = 660_000L;
        StubDataExtractor extractor = new StubDataExtractor(ndjson("{\"ts\":120000,\"event_count\":5}"));
        when(dataExtractorFactory.newExtractor(60_000L, 660_000L)).thenReturn(extractor);
        stubBucketsResponse(60_000L, 660_000L, Collections.emptyList());

        List<BucketWithMissingData> missing = newDetector().detectMissingData(latestMs);
        assertThat(missing, is(Collections.emptyList()));
    }

    private EsqlDelayedDataDetector newDetector() {
        return newDetector(BUCKET_SPAN_MS, WINDOW_MS);
    }

    private EsqlDelayedDataDetector newDetector(long bucketSpanMs, long windowMs) {
        return new EsqlDelayedDataDetector(bucketSpanMs, windowMs, JOB_ID, TIME_FIELD, COUNT_FIELD, dataExtractorFactory, client);
    }

    /**
     * Creates a {@link Bucket} at the given epoch-millis timestamp with the default {@code BUCKET_SPAN_MS}.
     */
    private Bucket newBucket(long epochMs, long eventCount) {
        return newBucket(epochMs, eventCount, BUCKET_SPAN_MS);
    }

    private Bucket newBucket(long epochMs, long eventCount, long bucketSpanMs) {
        Bucket bucket = new Bucket(JOB_ID, new Date(epochMs), bucketSpanMs / 1000);
        bucket.setEventCount(eventCount);
        return bucket;
    }

    @SuppressWarnings("unchecked")
    private void stubBucketsResponse(long start, long end, List<Bucket> buckets) {
        QueryPage<Bucket> page = new QueryPage<>(buckets, buckets.size(), Bucket.RESULTS_FIELD);
        GetBucketsAction.Response response = new GetBucketsAction.Response(page);
        ActionFuture<GetBucketsAction.Response> future = mock(ActionFuture.class);
        when(future.actionGet()).thenReturn(response);
        when(client.execute(eq(GetBucketsAction.INSTANCE), any())).thenReturn(future);
    }

    /** Builds a single NDJSON InputStream from the given JSON-object strings (one per line). */
    private static InputStream ndjson(String... jsonLines) {
        String joined = String.join("\n", jsonLines);
        return ndjsonStream(joined);
    }

    private static InputStream ndjsonStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    /** A minimal {@link DataExtractor} that yields a single pre-built {@link InputStream} then signals done. */
    private static class StubDataExtractor implements DataExtractor {

        private final InputStream data;
        private boolean hasNext = true;

        StubDataExtractor(InputStream data) {
            this.data = data;
        }

        @Override
        public DataSummary getSummary() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Result next() {
            hasNext = false;
            return new Result(new org.elasticsearch.xpack.core.ml.datafeed.SearchInterval(0L, 1L), Optional.of(data), List.of());
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
            return 0L;
        }
    }

    /**
     * A {@link DataExtractor} stub whose {@link #next()} throws an {@link IOException} to verify
     * that the detector wraps it in an {@link UncheckedIOException}.
     */
    private static class ThrowingDataExtractor extends StubDataExtractor {

        ThrowingDataExtractor() {
            super(null);
        }

        @Override
        public Result next() {
            throw new UncheckedIOException(new IOException("simulated read error"));
        }
    }
}
