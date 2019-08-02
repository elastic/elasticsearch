/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter.DatafeedTimingStatsPersister;
import org.junit.Before;
import org.mockito.InOrder;

import java.sql.Date;
import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class DatafeedTimingStatsReporterTests extends ESTestCase {

    private static final String JOB_ID = "my-job-id";
    private static final Instant TIMESTAMP = Instant.ofEpochMilli(1000000000);
    private static final TimeValue ONE_SECOND = TimeValue.timeValueSeconds(1);

    private DatafeedTimingStatsPersister timingStatsPersister;

    @Before
    public void setUpTests() {
        timingStatsPersister = mock(DatafeedTimingStatsPersister.class);
    }

    public void testReportSearchDuration_Null() {
        DatafeedTimingStatsReporter reporter = createReporter(createDatafeedTimingStats(JOB_ID, 3, 10, 10000.0));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 3, 10, 10000.0)));

        reporter.reportSearchDuration(null);
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 3, 10, 10000.0)));

        verifyZeroInteractions(timingStatsPersister);
    }

    public void testReportSearchDuration_Zero() {
        DatafeedTimingStatsReporter reporter = createReporter(createDatafeedTimingStats(JOB_ID, 0, 0, 0.0));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 0, 0, 0.0)));

        reporter.reportSearchDuration(TimeValue.ZERO);
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 1, 0, 0.0)));

        verify(timingStatsPersister).persistDatafeedTimingStats(createDatafeedTimingStats(JOB_ID, 1, 0, 0.0), RefreshPolicy.NONE);
        verifyNoMoreInteractions(timingStatsPersister);
    }

    public void testReportSearchDuration() {
        DatafeedTimingStatsReporter reporter = createReporter(createDatafeedTimingStats(JOB_ID, 13, 10, 10000.0, 10000.0));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 13, 10, 10000.0, 10000.0)));

        reporter.reportSearchDuration(ONE_SECOND);
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 14, 10, 11000.0, 11000.0)));

        reporter.reportSearchDuration(ONE_SECOND);
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 15, 10, 12000.0, 12000.0)));

        reporter.reportSearchDuration(ONE_SECOND);
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 16, 10, 13000.0, 13000.0)));

        reporter.reportSearchDuration(ONE_SECOND);
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 17, 10, 14000.0, 14000.0)));

        InOrder inOrder = inOrder(timingStatsPersister);
        inOrder.verify(timingStatsPersister).persistDatafeedTimingStats(
            createDatafeedTimingStats(JOB_ID, 15, 10, 12000.0, 12000.0), RefreshPolicy.NONE);
        inOrder.verify(timingStatsPersister).persistDatafeedTimingStats(
            createDatafeedTimingStats(JOB_ID, 17, 10, 14000.0, 14000.0), RefreshPolicy.NONE);
        verifyNoMoreInteractions(timingStatsPersister);
    }

    public void testReportDataCounts_Null() {
        DatafeedTimingStatsReporter reporter = createReporter(createDatafeedTimingStats(JOB_ID, 3, 10, 10000.0));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 3, 10, 10000.0)));

        reporter.reportDataCounts(null);
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 3, 10, 10000.0)));

        verifyZeroInteractions(timingStatsPersister);
    }

    public void testReportDataCounts() {
        DatafeedTimingStatsReporter reporter = createReporter(createDatafeedTimingStats(JOB_ID, 3, 20, 10000.0));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 3, 20, 10000.0)));

        reporter.reportDataCounts(createDataCounts(1));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 3, 21, 10000.0)));

        reporter.reportDataCounts(createDataCounts(1));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 3, 22, 10000.0)));

        reporter.reportDataCounts(createDataCounts(1));
        assertThat(reporter.getCurrentTimingStats(), equalTo(createDatafeedTimingStats(JOB_ID, 3, 23, 10000.0)));

        InOrder inOrder = inOrder(timingStatsPersister);
        inOrder.verify(timingStatsPersister).persistDatafeedTimingStats(
            createDatafeedTimingStats(JOB_ID, 3, 23, 10000.0), RefreshPolicy.NONE);
        verifyNoMoreInteractions(timingStatsPersister);
    }

    public void testFinishReporting_NoChange() {
        DatafeedTimingStatsReporter reporter = createReporter(createDatafeedTimingStats(JOB_ID, 3, 10, 10000.0));
        reporter.reportDataCounts(createDataCounts(0));
        reporter.finishReporting();

        verifyZeroInteractions(timingStatsPersister);
    }

    public void testFinishReporting_WithChange() {
        DatafeedTimingStatsReporter reporter = createReporter(new DatafeedTimingStats(JOB_ID));
        reporter.reportDataCounts(createDataCounts(0, TIMESTAMP));
        reporter.finishReporting();

        verify(timingStatsPersister).persistDatafeedTimingStats(
            new DatafeedTimingStats(JOB_ID, 0, 0, 0.0, new ExponentialAverageCalculationContext(0.0, TIMESTAMP, null)),
            RefreshPolicy.IMMEDIATE);
        verifyNoMoreInteractions(timingStatsPersister);
    }

    public void testTimingStatsDifferSignificantly() {
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                createDatafeedTimingStats(JOB_ID, 5, 10, 1000.0), createDatafeedTimingStats(JOB_ID, 5, 10, 1000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                createDatafeedTimingStats(JOB_ID, 5, 10, 1000.0), createDatafeedTimingStats(JOB_ID, 5, 10, 1100.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                createDatafeedTimingStats(JOB_ID, 5, 10, 1000.0), createDatafeedTimingStats(JOB_ID, 5, 10, 1120.0)),
            is(true));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                createDatafeedTimingStats(JOB_ID, 5, 10, 10000.0), createDatafeedTimingStats(JOB_ID, 5, 10, 11000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                createDatafeedTimingStats(JOB_ID, 5, 10, 10000.0), createDatafeedTimingStats(JOB_ID, 5, 10, 11200.0)),
            is(true));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                createDatafeedTimingStats(JOB_ID, 5, 10, 100000.0), createDatafeedTimingStats(JOB_ID, 5, 10, 110000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                createDatafeedTimingStats(JOB_ID, 5, 10, 100000.0), createDatafeedTimingStats(JOB_ID, 5, 10, 110001.0)),
            is(true));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                createDatafeedTimingStats(JOB_ID, 5, 10, 100000.0), createDatafeedTimingStats(JOB_ID, 50, 10, 100000.0)),
            is(true));
    }

    private DatafeedTimingStatsReporter createReporter(DatafeedTimingStats timingStats) {
        return new DatafeedTimingStatsReporter(timingStats, timingStatsPersister);
    }

    private static DatafeedTimingStats createDatafeedTimingStats(
            String jobId,
            long searchCount,
            long bucketCount,
            double totalSearchTimeMs) {
        return createDatafeedTimingStats(jobId, searchCount, bucketCount, totalSearchTimeMs, 0.0);
    }

    private static DatafeedTimingStats createDatafeedTimingStats(
            String jobId,
            long searchCount,
            long bucketCount,
            double totalSearchTimeMs,
            double incrementalSearchTimeMs) {
        ExponentialAverageCalculationContext context = new ExponentialAverageCalculationContext(incrementalSearchTimeMs, null, null);
        return new DatafeedTimingStats(jobId, searchCount, bucketCount, totalSearchTimeMs, context);
    }

    private static DataCounts createDataCounts(long bucketCount, Instant latestRecordTimestamp) {
        DataCounts dataCounts = createDataCounts(bucketCount);
        dataCounts.setLatestRecordTimeStamp(Date.from(latestRecordTimestamp));
        return dataCounts;
    }

    private static DataCounts createDataCounts(long bucketCount) {
        DataCounts dataCounts = new DataCounts(JOB_ID);
        dataCounts.incrementBucketCount(bucketCount);
        return dataCounts;
    }
}
