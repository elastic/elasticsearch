/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DataCountsReporterTests extends ESTestCase {

    private Job job;
    private JobDataCountsPersister jobDataCountsPersister;
    private TimeValue bucketSpan = TimeValue.timeValueSeconds(300);

    @Before
    public void setUpMocks() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Arrays.asList(new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(bucketSpan);
        acBuilder.setLatency(TimeValue.ZERO);
        acBuilder.setDetectors(Arrays.asList(new Detector.Builder("metric", "field").build()));


        Job.Builder builder = new Job.Builder("sr");
        builder.setAnalysisConfig(acBuilder);
        builder.setDataDescription(new DataDescription.Builder());
        job = builder.build(new Date());

        jobDataCountsPersister = Mockito.mock(JobDataCountsPersister.class);
    }

    public void testSimpleConstructor() {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(job, new DataCounts(job.getId()), jobDataCountsPersister);
        DataCounts stats = dataCountsReporter.incrementalStats();
        assertNotNull(stats);
        assertAllCountFieldsEqualZero(stats);
    }

    public void testComplexConstructor() {
        DataCounts counts = new DataCounts("foo", 1L, 1L, 2L, 0L, 3L, 4L, 5L, 6L, 7L, 8L,
                new Date(), new Date(), new Date(), new Date(), new Date());

        DataCountsReporter dataCountsReporter = new DataCountsReporter(job, counts, jobDataCountsPersister);
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

    public void testResetIncrementalCounts() {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(job, new DataCounts(job.getId()), jobDataCountsPersister);
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
        // skip a bucket so there is a non-zero empty bucket count
        long timeStamp = bucketSpan.millis() * 2 + 2000;
        dataCountsReporter.reportRecordWritten(5, timeStamp);
        dataCountsReporter.reportRecordWritten(5, timeStamp);
        assertEquals(2, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(10, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(2, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(6, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(602000, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        // check total stats
        assertEquals(4, dataCountsReporter.runningTotalStats().getInputRecordCount());
        assertEquals(20, dataCountsReporter.runningTotalStats().getInputFieldCount());
        assertEquals(4, dataCountsReporter.runningTotalStats().getProcessedRecordCount());
        assertEquals(12, dataCountsReporter.runningTotalStats().getProcessedFieldCount());
        assertEquals(602000, dataCountsReporter.runningTotalStats().getLatestRecordTimeStamp().getTime());

        // send 'flush' signal
        dataCountsReporter.finishReporting();
        assertEquals(2, dataCountsReporter.runningTotalStats().getBucketCount());
        assertEquals(1, dataCountsReporter.runningTotalStats().getEmptyBucketCount());
        assertEquals(0, dataCountsReporter.runningTotalStats().getSparseBucketCount());

        assertEquals(2, dataCountsReporter.incrementalStats().getBucketCount());
        assertEquals(1, dataCountsReporter.incrementalStats().getEmptyBucketCount());
        assertEquals(0, dataCountsReporter.incrementalStats().getSparseBucketCount());
    }

    public void testReportLatestTimeIncrementalStats() {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(job, new DataCounts(job.getId()), jobDataCountsPersister);
        dataCountsReporter.startNewIncrementalCount();
        dataCountsReporter.reportLatestTimeIncrementalStats(5001L);
        assertEquals(5001L, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());
    }

    public void testReportRecordsWritten() {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(job, new DataCounts(job.getId()), jobDataCountsPersister);
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

        verify(jobDataCountsPersister, never()).persistDataCounts(anyString(), any(DataCounts.class));
    }

    public void testReportRecordsWritten_Given9999Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 9999; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(9999, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(49995, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(9999, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(29997, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(9999, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(0, dataCountsReporter.getLogStatusCallCount());
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

        assertEquals(3, dataCountsReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given100_000Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 100000; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(100000, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(500000, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(100000, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(300000, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(100000, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(10, dataCountsReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given1_000_000Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 1_000_000; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(1_000_000, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(5_000_000, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(1_000_000, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(3_000_000, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(1_000_000, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(19, dataCountsReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given2_000_000Records() {
        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter();
        dataCountsReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 2_000_000; i++) {
            dataCountsReporter.reportRecordWritten(5, i);
        }

        assertEquals(2000000, dataCountsReporter.incrementalStats().getInputRecordCount());
        assertEquals(10000000, dataCountsReporter.incrementalStats().getInputFieldCount());
        assertEquals(2000000, dataCountsReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(6000000, dataCountsReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(2000000, dataCountsReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(20, dataCountsReporter.getLogStatusCallCount());
    }


    public void testFinishReporting() {
        DataCountsReporter dataCountsReporter = new DataCountsReporter(job, new DataCounts(job.getId()), jobDataCountsPersister);

        dataCountsReporter.setAnalysedFieldsPerRecord(3);
        Date now = new Date();
        DataCounts dc = new DataCounts(job.getId(), 2L, 5L, 0L, 10L, 0L, 1L, 0L, 0L, 0L, 0L, new Date(2000), new Date(3000),
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
        verify(jobDataCountsPersister, times(1)).persistDataCounts(eq("sr"), eq(dc));
        assertEquals(dc, dataCountsReporter.incrementalStats());
    }

    private void assertAllCountFieldsEqualZero(DataCounts stats) {
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
