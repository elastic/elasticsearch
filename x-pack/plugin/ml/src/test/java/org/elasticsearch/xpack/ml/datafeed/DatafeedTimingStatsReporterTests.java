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
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.junit.Before;
import org.mockito.InOrder;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class DatafeedTimingStatsReporterTests extends ESTestCase {

    private static final String JOB_ID = "my-job-id";
    private static final TimeValue ONE_SECOND = TimeValue.timeValueSeconds(1);

    private JobResultsPersister jobResultsPersister;

    @Before
    public void setUpTests() {
        jobResultsPersister = mock(JobResultsPersister.class);
    }

    public void testReportSearchDuration_Null() {
        DatafeedTimingStatsReporter timingStatsReporter =
            new DatafeedTimingStatsReporter(new DatafeedTimingStats(JOB_ID, 3, 10, 10000.0), jobResultsPersister);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 10, 10000.0)));

        timingStatsReporter.reportSearchDuration(null);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 10, 10000.0)));

        verifyZeroInteractions(jobResultsPersister);
    }

    public void testReportSearchDuration_Zero() {
        DatafeedTimingStatsReporter timingStatsReporter =
            new DatafeedTimingStatsReporter(new DatafeedTimingStats(JOB_ID), jobResultsPersister);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 0, 0, 0.0)));

        timingStatsReporter.reportSearchDuration(TimeValue.ZERO);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 1, 0, 0.0)));

        verify(jobResultsPersister).persistDatafeedTimingStats(new DatafeedTimingStats(JOB_ID, 1, 0, 0.0), RefreshPolicy.IMMEDIATE);
        verifyNoMoreInteractions(jobResultsPersister);
    }

    public void testReportSearchDuration() {
        DatafeedTimingStatsReporter timingStatsReporter =
            new DatafeedTimingStatsReporter(new DatafeedTimingStats(JOB_ID, 13, 10, 10000.0), jobResultsPersister);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 13, 10, 10000.0)));

        timingStatsReporter.reportSearchDuration(ONE_SECOND);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 14, 10, 11000.0)));

        timingStatsReporter.reportSearchDuration(ONE_SECOND);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 15, 10, 12000.0)));

        timingStatsReporter.reportSearchDuration(ONE_SECOND);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 16, 10, 13000.0)));

        timingStatsReporter.reportSearchDuration(ONE_SECOND);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 17, 10, 14000.0)));

        InOrder inOrder = inOrder(jobResultsPersister);
        inOrder.verify(jobResultsPersister).persistDatafeedTimingStats(
            new DatafeedTimingStats(JOB_ID, 15, 10, 12000.0), RefreshPolicy.IMMEDIATE);
        inOrder.verify(jobResultsPersister).persistDatafeedTimingStats(
            new DatafeedTimingStats(JOB_ID, 17, 10, 14000.0), RefreshPolicy.IMMEDIATE);
        verifyNoMoreInteractions(jobResultsPersister);
    }

    public void testReportDataCounts_Null() {
        DatafeedTimingStatsReporter timingStatsReporter =
            new DatafeedTimingStatsReporter(new DatafeedTimingStats(JOB_ID, 3, 10, 10000.0), jobResultsPersister);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 10, 10000.0)));

        timingStatsReporter.reportDataCounts(null);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 10, 10000.0)));

        verifyZeroInteractions(jobResultsPersister);
    }

    public void testReportDataCounts() {
        DatafeedTimingStatsReporter timingStatsReporter =
            new DatafeedTimingStatsReporter(new DatafeedTimingStats(JOB_ID, 3, 20, 10000.0), jobResultsPersister);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 20, 10000.0)));

        timingStatsReporter.reportDataCounts(createDataCountsWithBucketCount(1));
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 21, 10000.0)));

        timingStatsReporter.reportDataCounts(createDataCountsWithBucketCount(1));
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 22, 10000.0)));

        timingStatsReporter.reportDataCounts(createDataCountsWithBucketCount(1));
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 23, 10000.0)));

        InOrder inOrder = inOrder(jobResultsPersister);
        inOrder.verify(jobResultsPersister).persistDatafeedTimingStats(
            new DatafeedTimingStats(JOB_ID, 3, 23, 10000.0), RefreshPolicy.IMMEDIATE);
        verifyNoMoreInteractions(jobResultsPersister);
    }

    private static DataCounts createDataCountsWithBucketCount(long bucketCount) {
        DataCounts dataCounts = new DataCounts(JOB_ID);
        dataCounts.incrementBucketCount(bucketCount);
        return dataCounts;
    }

    public void testTimingStatsDifferSignificantly() {
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10, 1000.0), new DatafeedTimingStats(JOB_ID, 5, 10, 1000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10, 1000.0), new DatafeedTimingStats(JOB_ID, 5, 10, 1100.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10, 1000.0), new DatafeedTimingStats(JOB_ID, 5, 10, 1120.0)),
            is(true));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10, 10000.0), new DatafeedTimingStats(JOB_ID, 5, 10, 11000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10, 10000.0), new DatafeedTimingStats(JOB_ID, 5, 10, 11200.0)),
            is(true));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10, 100000.0), new DatafeedTimingStats(JOB_ID, 5, 10, 110000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10, 100000.0), new DatafeedTimingStats(JOB_ID, 5, 10, 110001.0)),
            is(true));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10, 100000.0), new DatafeedTimingStats(JOB_ID, 50, 10, 100000.0)),
            is(true));
    }
}
