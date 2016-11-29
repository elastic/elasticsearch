/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.status;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.prelert.job.usage.UsageReporter;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatusReporterTests extends ESTestCase {
    private static final String JOB_ID = "SR";
    private static final int MAX_PERCENT_DATE_PARSE_ERRORS = 40;
    private static final int MAX_PERCENT_OUT_OF_ORDER_ERRORS = 30;

    private UsageReporter usageReporter;
    private JobDataCountsPersister jobDataCountsPersister;
    private StatusReporter statusReporter;
    private ThreadPool threadPool;
    private Settings settings;

    @Before
    public void setUpMocks() {
        settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(StatusReporter.ACCEPTABLE_PERCENTAGE_DATE_PARSE_ERRORS_SETTING.getKey(), MAX_PERCENT_DATE_PARSE_ERRORS)
                .put(StatusReporter.ACCEPTABLE_PERCENTAGE_OUT_OF_ORDER_ERRORS_SETTING.getKey(), MAX_PERCENT_OUT_OF_ORDER_ERRORS).build();
        usageReporter = Mockito.mock(UsageReporter.class);
        jobDataCountsPersister = Mockito.mock(JobDataCountsPersister.class);
        threadPool = Mockito.mock(ThreadPool.class);

        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(new ThreadPool.Cancellable() {
            @Override
            public void cancel() {
            }

            @Override
            public boolean isCancelled() {
                return false;
            }
        });
    }

    public void testSettingAcceptablePercentages() throws IOException {
        StatusReporter statusReporter =
                new StatusReporter(threadPool, settings, JOB_ID, new DataCounts(JOB_ID), usageReporter, jobDataCountsPersister);
        assertEquals(statusReporter.getAcceptablePercentDateParseErrors(), MAX_PERCENT_DATE_PARSE_ERRORS);
        assertEquals(statusReporter.getAcceptablePercentOutOfOrderErrors(), MAX_PERCENT_OUT_OF_ORDER_ERRORS);
    }

    public void testSimpleConstructor() throws Exception {
        try (StatusReporter statusReporter = new StatusReporter(threadPool, settings, JOB_ID, new DataCounts(JOB_ID), usageReporter,
                jobDataCountsPersister)) {
            DataCounts stats = statusReporter.incrementalStats();
            assertNotNull(stats);
            assertAllCountFieldsEqualZero(stats);
        }
    }

    public void testComplexConstructor() throws Exception {
        Environment env = new Environment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build());
        DataCounts counts = new DataCounts("foo", 1L, 1L, 2L, 0L, 3L, 4L, 5L, new Date(), new Date());

        try (StatusReporter statusReporter =
                     new StatusReporter(threadPool, settings, JOB_ID, counts, usageReporter, jobDataCountsPersister)) {
            DataCounts stats = statusReporter.incrementalStats();
            assertNotNull(stats);
            assertAllCountFieldsEqualZero(stats);

            assertEquals(1, statusReporter.getProcessedRecordCount());
            assertEquals(2, statusReporter.getBytesRead());
            assertEquals(3, statusReporter.getDateParseErrorsCount());
            assertEquals(4, statusReporter.getMissingFieldErrorCount());
            assertEquals(5, statusReporter.getOutOfOrderRecordCount());
            assertNull(stats.getEarliestRecordTimeStamp());
        }
    }

    public void testResetIncrementalCounts() throws Exception {
        try (StatusReporter statusReporter = new StatusReporter(threadPool, settings, JOB_ID, new DataCounts(JOB_ID), usageReporter,
                jobDataCountsPersister)) {
            DataCounts stats = statusReporter.incrementalStats();
            assertNotNull(stats);
            assertAllCountFieldsEqualZero(stats);

            statusReporter.setAnalysedFieldsPerRecord(3);

            statusReporter.reportRecordWritten(5, 1000);
            statusReporter.reportRecordWritten(5, 1000);
            assertEquals(2, statusReporter.incrementalStats().getInputRecordCount());
            assertEquals(10, statusReporter.incrementalStats().getInputFieldCount());
            assertEquals(2, statusReporter.incrementalStats().getProcessedRecordCount());
            assertEquals(6, statusReporter.incrementalStats().getProcessedFieldCount());
            assertEquals(1000, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

            assertEquals(statusReporter.incrementalStats(), statusReporter.runningTotalStats());

            statusReporter.startNewIncrementalCount();
            stats = statusReporter.incrementalStats();
            assertNotNull(stats);
            assertAllCountFieldsEqualZero(stats);
        }
    }

    public void testReportLatestTimeIncrementalStats() throws IOException {
        try (StatusReporter statusReporter = new StatusReporter(threadPool, settings, JOB_ID, new DataCounts(JOB_ID), usageReporter,
                jobDataCountsPersister)) {
            statusReporter.startNewIncrementalCount();
            statusReporter.reportLatestTimeIncrementalStats(5001L);
            assertEquals(5001L, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());
        }
    }

    public void testReportRecordsWritten() {
        try (StatusReporter statusReporter = new StatusReporter(threadPool, settings, JOB_ID, new DataCounts(JOB_ID), usageReporter,
                jobDataCountsPersister)) {
            statusReporter.setAnalysedFieldsPerRecord(3);

            statusReporter.reportRecordWritten(5, 2000);
            assertEquals(1, statusReporter.incrementalStats().getInputRecordCount());
            assertEquals(5, statusReporter.incrementalStats().getInputFieldCount());
            assertEquals(1, statusReporter.incrementalStats().getProcessedRecordCount());
            assertEquals(3, statusReporter.incrementalStats().getProcessedFieldCount());
            assertEquals(2000, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

            statusReporter.reportRecordWritten(5, 3000);
            statusReporter.reportMissingField();
            assertEquals(2, statusReporter.incrementalStats().getInputRecordCount());
            assertEquals(10, statusReporter.incrementalStats().getInputFieldCount());
            assertEquals(2, statusReporter.incrementalStats().getProcessedRecordCount());
            assertEquals(5, statusReporter.incrementalStats().getProcessedFieldCount());
            assertEquals(3000, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

            assertEquals(statusReporter.incrementalStats(), statusReporter.runningTotalStats());

            verify(jobDataCountsPersister, never()).persistDataCounts(anyString(), any(DataCounts.class));
        }
    }

    public void testReportRecordsWritten_Given100Records() {
        DummyStatusReporter statusReporter = new DummyStatusReporter(usageReporter);
        statusReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 101; i++) {
            statusReporter.reportRecordWritten(5, i);
        }

        assertEquals(101, statusReporter.incrementalStats().getInputRecordCount());
        assertEquals(505, statusReporter.incrementalStats().getInputFieldCount());
        assertEquals(101, statusReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(303, statusReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(101, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());


        assertEquals(1, statusReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given1000Records() {
        DummyStatusReporter statusReporter = new DummyStatusReporter(usageReporter);

        statusReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 1001; i++) {
            statusReporter.reportRecordWritten(5, i);
        }

        assertEquals(1001, statusReporter.incrementalStats().getInputRecordCount());
        assertEquals(5005, statusReporter.incrementalStats().getInputFieldCount());
        assertEquals(1001, statusReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(3003, statusReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(1001, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(10, statusReporter.getLogStatusCallCount());

    }

    public void testReportRecordsWritten_Given2000Records() {
        DummyStatusReporter statusReporter = new DummyStatusReporter(usageReporter);
        statusReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 2001; i++) {
            statusReporter.reportRecordWritten(5, i);
        }

        assertEquals(2001, statusReporter.incrementalStats().getInputRecordCount());
        assertEquals(10005, statusReporter.incrementalStats().getInputFieldCount());
        assertEquals(2001, statusReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(6003, statusReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(2001, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(11, statusReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given20000Records() {
        DummyStatusReporter statusReporter = new DummyStatusReporter(usageReporter);
        statusReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 20001; i++) {
            statusReporter.reportRecordWritten(5, i);
        }

        assertEquals(20001, statusReporter.incrementalStats().getInputRecordCount());
        assertEquals(100005, statusReporter.incrementalStats().getInputFieldCount());
        assertEquals(20001, statusReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(60003, statusReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(20001, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(29, statusReporter.getLogStatusCallCount());
    }

    public void testReportRecordsWritten_Given30000Records() {
        DummyStatusReporter statusReporter = new DummyStatusReporter(usageReporter);
        statusReporter.setAnalysedFieldsPerRecord(3);

        for (int i = 1; i <= 30001; i++) {
            statusReporter.reportRecordWritten(5, i);
        }

        assertEquals(30001, statusReporter.incrementalStats().getInputRecordCount());
        assertEquals(150005, statusReporter.incrementalStats().getInputFieldCount());
        assertEquals(30001, statusReporter.incrementalStats().getProcessedRecordCount());
        assertEquals(90003, statusReporter.incrementalStats().getProcessedFieldCount());
        assertEquals(30001, statusReporter.incrementalStats().getLatestRecordTimeStamp().getTime());

        assertEquals(30, statusReporter.getLogStatusCallCount());
    }

    public void testFinishReporting() {
        try (StatusReporter statusReporter = new StatusReporter(threadPool, settings, JOB_ID, new DataCounts(JOB_ID), usageReporter,
                jobDataCountsPersister)) {

            statusReporter.setAnalysedFieldsPerRecord(3);

            DataCounts dc = new DataCounts(JOB_ID, 2L, 5L, 0L, 10L, 0L, 1L, 0L, new Date(2000), new Date(3000));
            statusReporter.reportRecordWritten(5, 2000);
            statusReporter.reportRecordWritten(5, 3000);
            statusReporter.reportMissingField();
            statusReporter.finishReporting();

            Mockito.verify(usageReporter, Mockito.times(1)).reportUsage();
            Mockito.verify(jobDataCountsPersister, Mockito.times(1)).persistDataCounts(eq("SR"), eq(dc));
            assertEquals(dc, statusReporter.incrementalStats());
        }
    }

    public void testPersistenceTimeOut() {

        ThreadPool mockThreadPool = Mockito.mock(ThreadPool.class);
        ArgumentCaptor<Runnable> argumentCaptor = ArgumentCaptor.forClass(Runnable.class);

        when(mockThreadPool.scheduleWithFixedDelay(argumentCaptor.capture(), any(), any())).thenReturn(new ThreadPool.Cancellable() {
            @Override
            public void cancel() {

            }

            @Override
            public boolean isCancelled() {
                return false;
            }
        });

        ExecutorService executorService = mock(ExecutorService.class);
        ArgumentCaptor<Runnable> persistTaskCapture = ArgumentCaptor.forClass(Runnable.class);
        when(executorService.submit(persistTaskCapture.capture())).thenReturn(null);

        when(mockThreadPool.generic()).thenReturn(executorService);

        try (StatusReporter statusReporter = new StatusReporter(mockThreadPool, settings, JOB_ID, new DataCounts(JOB_ID), usageReporter,
                jobDataCountsPersister)) {

            statusReporter.setAnalysedFieldsPerRecord(3);

            statusReporter.reportRecordWritten(5, 2000);
            statusReporter.reportRecordWritten(5, 3000);

            Mockito.verify(jobDataCountsPersister, Mockito.times(0)).persistDataCounts(eq("SR"), any());
            argumentCaptor.getValue().run();
            statusReporter.reportRecordWritten(5, 4000);
            DataCounts dc = new DataCounts(JOB_ID, 2L, 6L, 0L, 10L, 0L, 0L, 0L, new Date(2000), new Date(4000));
            // verify threadpool executor service to do the persistence is launched
            Mockito.verify(mockThreadPool, Mockito.times(1)).generic();

            // run the captured persist task
            persistTaskCapture.getValue().run();
            Mockito.verify(jobDataCountsPersister, Mockito.times(1)).persistDataCounts(eq("SR"), any());
        }
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
    }
}
