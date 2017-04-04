/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class DataCountsReporterTests extends ESTestCase {
    private static final int MAX_PERCENT_DATE_PARSE_ERRORS = 40;
    private static final int MAX_PERCENT_OUT_OF_ORDER_ERRORS = 30;

    private Job job;
    private JobDataCountsPersister jobDataCountsPersister;
    private Settings settings;

    @Before
    public void setUpMocks() {
        settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(DataCountsReporter.ACCEPTABLE_PERCENTAGE_DATE_PARSE_ERRORS_SETTING.getKey(), MAX_PERCENT_DATE_PARSE_ERRORS)
                .put(DataCountsReporter.ACCEPTABLE_PERCENTAGE_OUT_OF_ORDER_ERRORS_SETTING.getKey(), MAX_PERCENT_OUT_OF_ORDER_ERRORS)
                .build();

        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Arrays.asList(new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(TimeValue.timeValueSeconds(300));
        acBuilder.setLatency(TimeValue.ZERO);
        acBuilder.setDetectors(Arrays.asList(new Detector.Builder("metric", "field").build()));

        Job.Builder builder = new Job.Builder("sr");
        builder.setAnalysisConfig(acBuilder);
        job = builder.build(new Date());

        jobDataCountsPersister = Mockito.mock(JobDataCountsPersister.class);
    }

    public void testSettingAcceptablePercentages() throws IOException {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(settings, job, new DataCounts(job.getId()),
                jobDataCountsPersister);
        assertEquals(dataCountsReporter.getAcceptablePercentDateParseErrors(), MAX_PERCENT_DATE_PARSE_ERRORS);
        assertEquals(dataCountsReporter.getAcceptablePercentOutOfOrderErrors(), MAX_PERCENT_OUT_OF_ORDER_ERRORS);
    }

    public void testSimpleConstructor() throws Exception {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(settings, job, new DataCounts(job.getId()),
                jobDataCountsPersister);
        DataCounts stats = dataCountsReporter.incrementalStats();
        assertNotNull(stats);
        assertAllCountFieldsEqualZero(stats);
    }

    public void testComplexConstructor() throws Exception {
        DataCounts counts = new DataCounts("foo", 1L, 1L, 2L, 0L, 3L, 4L, 5L, 6L, 7L, 8L,
                new Date(), new Date(), new Date(), new Date(), new Date());

        DataCountsReporter dataCountsReporter =
                new DataCountsReporter(settings, job, counts, jobDataCountsPersister);
        DataCounts stats = dataCountsReporter.incrementalStats();
        assertNotNull(stats);
        assertAllCountFieldsEqualZero(stats);

        assertEquals(1, dataCountsReporter.getProcessedRecordCount());
        assertEquals(2, dataCountsReporter.getBytesRead());
        assertEquals(3, dataCountsReporter.getDateParseErrorsCount());
        assertEquals(4, dataCountsReporter.getMissingFieldErrorCount());
        assertEquals(5, dataCountsReporter.getOutOfOrderRecordCount());
        assertEquals(6, dataCountsReporter.getEmptyBucketCount());
        assertEquals(7, dataCountsReporter.getSparseBucketCount());
        assertEquals(8, dataCountsReporter.getBucketCount());
        assertNull(stats.getEarliestRecordTimeStamp());
    }

    public void testResetIncrementalCounts() throws Exception {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(settings, job, new DataCounts(job.getId()),
                jobDataCountsPersister);
        DataCounts stats = dataCountsReporter.incrementalStats();
        assertNotNull(stats);
        assertAllCountFieldsEqualZero(stats);

        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        dataCountsReporter.reportRecordWritten(5, 1000);
        dataCountsReporter.reportRecordWritten(5, 1000);
        assertEquals(2, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(10, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(2, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(6, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(1000, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(dataCountsReporter.incrementalStats(), dataCountsReporter.runningTotalStats());

        dataCountsReporter.startNewIncrementalCount();
        stats = dataCountsReporter.incrementalStats();
        assertNotNull(stats);
        assertAllCountFieldsEqualZero(stats);

        // write some more data
        dataCountsReporter.reportRecordWritten(5, 302000);
        dataCountsReporter.reportRecordWritten(5, 302000);
        assertEquals(2, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(10, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(2, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(6, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(302000, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        // check total stats
        assertEquals(4, dataCountsReporter.runningTotalStats().getInputRecordCount());
        assertEquals(20, dataCountsReporter.runningTotalStats().getInputFieldCount());
        assertEquals(4, dataCountsReporter.runningTotalStats().getProcessedRecordCount());
        assertEquals(12, dataCountsReporter.runningTotalStats().getProcessedFieldCount());
        assertEquals(302000, dataCountsReporter.runningTotalStats().getLatestRecordTimeStamp().getTime());

        // send 'flush' signal
        dataCountsReporter.finishReporting();
        assertEquals(2, dataCountsReporter.runningTotalStats().getBucketCount());
        assertEquals(0, dataCountsReporter.runningTotalStats().getEmptyBucketCount());
        assertEquals(0, dataCountsReporter.runningTotalStats().getSparseBucketCount());
    }

    public void testReportLatestTimeIncrementalStats() throws IOException {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(settings, job, new DataCounts(job.getId()),
                jobDataCountsPersister);
        dataCountsReporter.startNewIncrementalCount();
        dataCountsReporter.reportLatestTimeIncrementalStats(5001L);
        assertEquals(5001L, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());
    }

    public void testReportRecordsWritten() {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(settings, job, new DataCounts(job.getId()),
                jobDataCountsPersister);
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        dataCountsReporter.reportRecordWritten(5, 2000);
        assertEquals(1, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(5, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(1, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(3, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(2000, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        dataCountsReporter.reportRecordWritten(5, 3000);
        dataCountsReporter.reportMissingField();
        assertEquals(2, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(10, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(2, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(5, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(3000, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(dataCountsReporter.incrementalStats(), dataCountsReporter.runningTotalStats());

        verify(jobDataCountsPersister, never()).persistDataCounts(anyString(), any(DataCounts.class), any());
    }

    public void testReportRecordsWritten_Given100Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 101; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(101, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(505, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(101, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(303, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(101, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(1, dataCountsReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given1000Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();

        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 1001; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(1001, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(5005, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(1001, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(3003, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(1001, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(10, dataCountsReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given2000Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 2001; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(2001, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(10005, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(2001, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(6003, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(2001, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(11, dataCountsReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given20000Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 20001; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(20001, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(100005, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(20001, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(60003, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(20001, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(29, dataCountsReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given30000Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 30001; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(30001, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(150005, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(30001, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(90003, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(30001, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(30, dataCountsReporter.getLogStatusCallCount());
    }

    public void testFinishReporting() {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(settings, job, new DataCounts(job.getId()),
                jobDataCountsPersister);

        dataCountsReporter.setAnalysedFieldsPerRecord(3);
        Date now = new Date();
        DataCounts dc = new DataCounts(job.getId(), 2L, 5L, 0L, 10L, 0L, 1L, 0L, 0L, 0L, 1L, new Date(2000), new Date(3000),
                now, (Date) null, (Date) null);
        dataCountsReporter.reportRecordWritten(5, 2000);
        dataCountsReporter.reportRecordWritten(5, 3000);
        dataCountsReporter.reportMissingField();
        dataCountsReporter.finishReporting();

        long lastReportedTimeMs = dataCountsReporter.incrementalStats().getLastDataTimeStamp().getTime();
        // check last data time is equal to now give or take a second
        assertTrue(lastReportedTimeMs >= now.getTime()
                && lastReportedTimeMs <= now.getTime() + TimeUnit.SECONDS.toMillis(1));
        assertEquals(dataCountsReporter.incrementalStats().getLastDataTimeStamp(),
                dataCountsReporter.runningTotalStats().getLastDataTimeStamp());

        dc.setLastDataTimeStamp(dataCountsReporter.incrementalStats().getLastDataTimeStamp());
        Mockito.verify(jobDataCountsPersister, Mockito.times(1)).persistDataCounts(eq("sr"), eq(dc), any());
        assertEquals(dc, dataCountsReporter.incrementalStats());
    }

    private void assertAllCountFieldsEqualZero(DataCounts stats) throws Exception {
        assertEquals(0L, stats.getProcessedRecordCount());
        assertEquals(0L, stats.getProcessedFieldCount());
        assertEquals(0L, stats.getInputBytes());
        assertEquals(0L, stats.getInputFieldCount());
        assertEquals(0L, stats.getInputRecordCount());
        assertEquals(0L, stats.getInvalidDateCount());
        assertEquals(0L, stats.getMissingFieldCount());
        assertEquals(0L, stats.getOutOfOrderTimeStampCount());
        assertEquals(0L, stats.getEmptyBucketCount());
        assertEquals(0L, stats.getSparseBucketCount());
    }
}
