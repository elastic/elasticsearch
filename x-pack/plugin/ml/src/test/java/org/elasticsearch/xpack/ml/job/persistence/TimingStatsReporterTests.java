/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;
import org.junit.Before;
import org.mockito.InOrder;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TimingStatsReporterTests extends ESTestCase {

    private static final String JOB_ID = "my-job-id";
    private static final Instant TIMESTAMP = Instant.ofEpochMilli(1000000000);
    private static final Duration BUCKET_SPAN = Duration.ofMinutes(1);

    private JobResultsPersister.Builder bulkResultsPersister;

    @Before
    public void setUpTests() {
        bulkResultsPersister = mock(JobResultsPersister.Builder.class);
    }

    public void testGetCurrentTimingStats() {
        TimingStats stats = createTimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89);
        TimingStatsReporter reporter = createReporter(stats);
        assertThat(reporter.getCurrentTimingStats(), equalTo(stats));

        verifyZeroInteractions(bulkResultsPersister);
    }

    public void testReporting() {
        TimingStatsReporter reporter = createReporter(new TimingStats(JOB_ID));
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID)));

        reporter.reportBucket(createBucket(10));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createTimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0, 10.0)));

        reporter.reportBucket(createBucket(20));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createTimingStats(JOB_ID, 2, 10.0, 20.0, 15.0, 10.1, 30.0)));

        reporter.reportBucket(createBucket(15));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createTimingStats(JOB_ID, 3, 10.0, 20.0, 15.0, 10.149, 45.0)));

        InOrder inOrder = inOrder(bulkResultsPersister);
        inOrder.verify(bulkResultsPersister).persistTimingStats(createTimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0, 10.0));
        inOrder.verify(bulkResultsPersister).persistTimingStats(createTimingStats(JOB_ID, 2, 10.0, 20.0, 15.0, 10.1, 30.0));
        verifyNoMoreInteractions(bulkResultsPersister);
    }

    public void testFinishReporting() {
        TimingStatsReporter reporter = createReporter(new TimingStats(JOB_ID));
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID)));

        reporter.reportBucket(createBucket(10));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createTimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0, 10.0)));

        reporter.reportBucket(createBucket(10));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createTimingStats(JOB_ID, 2, 10.0, 10.0, 10.0, 10.0, 20.0)));

        reporter.reportBucket(createBucket(10));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createTimingStats(JOB_ID, 3, 10.0, 10.0, 10.0, 10.0, 30.0)));

        reporter.finishReporting();
        assertThat(reporter.getCurrentTimingStats(), equalTo(createTimingStats(JOB_ID, 3, 10.0, 10.0, 10.0, 10.0, 30.0)));

        InOrder inOrder = inOrder(bulkResultsPersister);
        inOrder.verify(bulkResultsPersister).persistTimingStats(createTimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0, 10.0));
        inOrder.verify(bulkResultsPersister).persistTimingStats(createTimingStats(JOB_ID, 3, 10.0, 10.0, 10.0, 10.0, 30.0));
        verifyNoMoreInteractions(bulkResultsPersister);
    }

    public void testFinishReporting_NoChange() {
        TimingStatsReporter reporter = createReporter(new TimingStats(JOB_ID));
        reporter.finishReporting();

        verifyZeroInteractions(bulkResultsPersister);
    }

    public void testFinishReporting_WithChange() {
        TimingStatsReporter reporter = createReporter(new TimingStats(JOB_ID));
        reporter.reportBucket(createBucket(10));
        reporter.finishReporting();

        verify(bulkResultsPersister).persistTimingStats(createTimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0, 10.0));
        verifyNoMoreInteractions(bulkResultsPersister);
    }

    public void testTimingStatsDifferSignificantly() {
        assertThat(
            TimingStatsReporter.differSignificantly(
                createTimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), createTimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0)),
            is(false));
        assertThat(
            TimingStatsReporter.differSignificantly(
                createTimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), createTimingStats(JOB_ID, 10, 10.0, 11.0, 1.0, 10.0)),
            is(false));
        assertThat(
            TimingStatsReporter.differSignificantly(
                createTimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), createTimingStats(JOB_ID, 10, 10.0, 12.0, 1.0, 10.0)),
            is(true));
    }

    public void testValuesDifferSignificantly() {
        assertThat(TimingStatsReporter.differSignificantly((Double) null, (Double) null), is(false));
        assertThat(TimingStatsReporter.differSignificantly(1.0, null), is(true));
        assertThat(TimingStatsReporter.differSignificantly(null, 1.0), is(true));
        assertThat(TimingStatsReporter.differSignificantly(0.9, 1.0), is(false));
        assertThat(TimingStatsReporter.differSignificantly(1.0, 0.9), is(false));
        assertThat(TimingStatsReporter.differSignificantly(0.9, 1.000001), is(true));
        assertThat(TimingStatsReporter.differSignificantly(1.0, 0.899999), is(true));
        assertThat(TimingStatsReporter.differSignificantly(0.0, 1.0), is(true));
        assertThat(TimingStatsReporter.differSignificantly(1.0, 0.0), is(true));
    }

    private TimingStatsReporter createReporter(TimingStats timingStats) {
        return new TimingStatsReporter(timingStats, bulkResultsPersister);
    }

    private static TimingStats createTimingStats(
        String jobId,
        long bucketCount,
        @Nullable Double minBucketProcessingTimeMs,
        @Nullable Double maxBucketProcessingTimeMs,
        @Nullable Double avgBucketProcessingTimeMs,
        @Nullable Double exponentialAvgBucketProcessingTimeMs) {
        return createTimingStats(
            jobId,
            bucketCount,
            minBucketProcessingTimeMs,
            maxBucketProcessingTimeMs,
            avgBucketProcessingTimeMs,
            exponentialAvgBucketProcessingTimeMs,
            0.0);
    }

    private static TimingStats createTimingStats(
            String jobId,
            long bucketCount,
            @Nullable Double minBucketProcessingTimeMs,
            @Nullable Double maxBucketProcessingTimeMs,
            @Nullable Double avgBucketProcessingTimeMs,
            @Nullable Double exponentialAvgBucketProcessingTimeMs,
            double incrementalBucketProcessingTimeMs) {
        ExponentialAverageCalculationContext context =
            new ExponentialAverageCalculationContext(incrementalBucketProcessingTimeMs, TIMESTAMP.plus(BUCKET_SPAN), null);
        return new TimingStats(
            jobId,
            bucketCount,
            minBucketProcessingTimeMs,
            maxBucketProcessingTimeMs,
            avgBucketProcessingTimeMs,
            exponentialAvgBucketProcessingTimeMs,
            context);
    }

    private static Bucket createBucket(long processingTimeMs) {
        Bucket bucket = new Bucket(JOB_ID, Date.from(TIMESTAMP), BUCKET_SPAN.getSeconds());
        bucket.setProcessingTimeMs(processingTimeMs);
        return bucket;
    }
}
