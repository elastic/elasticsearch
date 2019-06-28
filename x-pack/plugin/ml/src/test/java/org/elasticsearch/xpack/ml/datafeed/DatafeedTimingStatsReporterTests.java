/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.junit.Before;
import org.mockito.InOrder;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

public class DatafeedTimingStatsReporterTests extends ESTestCase {

    private static final String JOB_ID = "my-job-id";
    private static final TimeValue ONE_SECOND = TimeValue.timeValueSeconds(1);

    private JobResultsPersister jobResultsPersister;

    @Before
    public void setUpTests() {
        jobResultsPersister = mock(JobResultsPersister.class);
    }

    public void testReportSearchDuration() {
        DatafeedTimingStats timingStats = new DatafeedTimingStats(JOB_ID, 3, 10000.0);
        DatafeedTimingStatsReporter timingStatsReporter = new DatafeedTimingStatsReporter(timingStats, jobResultsPersister);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 3, 10000.0)));

        timingStatsReporter.reportSearchDuration(ONE_SECOND);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 4, 11000.0)));

        timingStatsReporter.reportSearchDuration(ONE_SECOND);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 5, 12000.0)));

        timingStatsReporter.reportSearchDuration(ONE_SECOND);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 6, 13000.0)));

        timingStatsReporter.reportSearchDuration(ONE_SECOND);
        assertThat(timingStatsReporter.getCurrentTimingStats(), equalTo(new DatafeedTimingStats(JOB_ID, 7, 14000.0)));

        InOrder inOrder = inOrder(jobResultsPersister);
        inOrder.verify(jobResultsPersister).persistDatafeedTimingStats(new DatafeedTimingStats(JOB_ID, 5, 12000.0));
        inOrder.verify(jobResultsPersister).persistDatafeedTimingStats(new DatafeedTimingStats(JOB_ID, 7, 14000.0));
        inOrder.verifyNoMoreInteractions();
    }

    public void testTimingStatsDifferSignificantly() {
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 1000.0), new DatafeedTimingStats(JOB_ID, 5, 1000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 1000.0), new DatafeedTimingStats(JOB_ID, 5, 1100.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 1000.0), new DatafeedTimingStats(JOB_ID, 5, 1120.0)),
            is(true));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10000.0), new DatafeedTimingStats(JOB_ID, 5, 11000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 10000.0), new DatafeedTimingStats(JOB_ID, 5, 11200.0)),
            is(true));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 100000.0), new DatafeedTimingStats(JOB_ID, 5, 110000.0)),
            is(false));
        assertThat(
            DatafeedTimingStatsReporter.differSignificantly(
                new DatafeedTimingStats(JOB_ID, 5, 100000.0), new DatafeedTimingStats(JOB_ID, 5, 110001.0)),
            is(true));
    }
}
