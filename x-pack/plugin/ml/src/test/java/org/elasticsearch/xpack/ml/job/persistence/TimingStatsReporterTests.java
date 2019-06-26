/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.junit.Before;
import org.mockito.InOrder;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TimingStatsReporterTests extends ESTestCase {

    private static final String JOB_ID = "my-job-id";

    private JobResultsPersister.Builder bulkResultsPersister;

    @Before
    public void setUpTests() {
        bulkResultsPersister = mock(JobResultsPersister.Builder.class);
    }

    public void testGetCurrentTimingStats() {
        TimingStats stats = new TimingStats(JOB_ID, 7, 1.0, 2.0, 1.23, 7.89);
        TimingStatsReporter reporter = new TimingStatsReporter(stats, bulkResultsPersister);
        assertThat(reporter.getCurrentTimingStats(), equalTo(stats));

        verifyZeroInteractions(bulkResultsPersister);
    }

    public void testReporting() {
        TimingStatsReporter reporter = new TimingStatsReporter(new TimingStats(JOB_ID), bulkResultsPersister);
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID)));

        reporter.reportBucketProcessingTime(10);
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0)));

        reporter.reportBucketProcessingTime(20);
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID, 2, 10.0, 20.0, 15.0, 10.1)));

        reporter.reportBucketProcessingTime(15);
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID, 3, 10.0, 20.0, 15.0, 10.149)));

        InOrder inOrder = inOrder(bulkResultsPersister);
        inOrder.verify(bulkResultsPersister).persistTimingStats(new TimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0));
        inOrder.verify(bulkResultsPersister).persistTimingStats(new TimingStats(JOB_ID, 2, 10.0, 20.0, 15.0, 10.1));
        inOrder.verifyNoMoreInteractions();
    }

    public void testFlush() {
        TimingStatsReporter reporter = new TimingStatsReporter(new TimingStats(JOB_ID), bulkResultsPersister);
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID)));

        reporter.reportBucketProcessingTime(10);
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0)));

        reporter.reportBucketProcessingTime(10);
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID, 2, 10.0, 10.0, 10.0, 10.0)));

        reporter.reportBucketProcessingTime(10);
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID, 3, 10.0, 10.0, 10.0, 10.0)));

        reporter.flush();
        assertThat(reporter.getCurrentTimingStats(), equalTo(new TimingStats(JOB_ID, 3, 10.0, 10.0, 10.0, 10.0)));

        InOrder inOrder = inOrder(bulkResultsPersister);
        inOrder.verify(bulkResultsPersister).persistTimingStats(new TimingStats(JOB_ID, 1, 10.0, 10.0, 10.0, 10.0));
        inOrder.verify(bulkResultsPersister).persistTimingStats(new TimingStats(JOB_ID, 3, 10.0, 10.0, 10.0, 10.0));
        inOrder.verifyNoMoreInteractions();
    }

    public void testTimingStatsDifferSignificantly() {
        assertThat(
            TimingStatsReporter.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0)),
            is(false));
        assertThat(
            TimingStatsReporter.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), new TimingStats(JOB_ID, 10, 10.0, 11.0, 1.0, 10.0)),
            is(false));
        assertThat(
            TimingStatsReporter.differSignificantly(
                new TimingStats(JOB_ID, 10, 10.0, 10.0, 1.0, 10.0), new TimingStats(JOB_ID, 10, 10.0, 12.0, 1.0, 10.0)),
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
}
