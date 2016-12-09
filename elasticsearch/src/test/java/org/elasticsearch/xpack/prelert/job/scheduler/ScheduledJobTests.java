/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.action.FlushJobAction;
import org.elasticsearch.xpack.prelert.action.JobDataAction;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Optional;
import java.util.function.Supplier;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScheduledJobTests extends ESTestCase {

    private Auditor auditor;
    private DataExtractor dataExtractor;
    private Client client;
    private ActionFuture<FlushJobAction.Response> flushJobFuture;

    private long currentTime;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        auditor = mock(Auditor.class);
        dataExtractor = mock(DataExtractor.class);
        client = mock(Client.class);
        ActionFuture<JobDataAction.Response> jobDataFuture = mock(ActionFuture.class);
        flushJobFuture = mock(ActionFuture.class);
        currentTime = 0;

        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream inputStream = new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8));
        when(dataExtractor.next()).thenReturn(Optional.of(inputStream));
        DataCounts dataCounts = new DataCounts("_job_id", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        when(client.execute(same(JobDataAction.INSTANCE), eq(new JobDataAction.Request("_job_id")))).thenReturn(jobDataFuture);
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        when(jobDataFuture.get()).thenReturn(new JobDataAction.Response(dataCounts));
    }

    public void testLookBackRunWithEndTime() throws Exception {
        ScheduledJob scheduledJob = createScheduledJob(1000, 500, -1, -1);
        assertNull(scheduledJob.runLookBack(0L, 1000L));

        verify(dataExtractor).newSearch(eq(0L), eq(1000L), any());
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testLookBackRunWithNoEndTime() throws Exception {
        currentTime = 2000L;
        long frequencyMs = 1000;
        long queryDelayMs = 500;
        ScheduledJob scheduledJob = createScheduledJob(frequencyMs, queryDelayMs, -1, -1);
        long next = scheduledJob.runLookBack(0L, null);
        assertEquals(2000 + frequencyMs + 100, next);

        verify(dataExtractor).newSearch(eq(0L), eq(1500L), any());
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testLookBackRunWithOverrideStartTime() throws Exception {
        currentTime = 10000L;
        long latestFinalBucketEndTimeMs = -1;
        long latestRecordTimeMs = -1;
        if (randomBoolean()) {
            latestFinalBucketEndTimeMs = 5000;
        } else {
            latestRecordTimeMs = 5000;
        }

        long frequencyMs = 1000;
        long queryDelayMs = 500;
        ScheduledJob scheduledJob = createScheduledJob(frequencyMs, queryDelayMs, latestFinalBucketEndTimeMs, latestRecordTimeMs);
        long next = scheduledJob.runLookBack(0L, null);
        assertEquals(10000 + frequencyMs + 100, next);

        verify(dataExtractor).newSearch(eq(5000 + 1L), eq(currentTime - queryDelayMs), any());
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testRealtimeRun() throws Exception {
        currentTime = 60000L;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        ScheduledJob scheduledJob = createScheduledJob(frequencyMs, queryDelayMs, 1000, -1);
        long next = scheduledJob.runRealtime();
        assertEquals(currentTime + frequencyMs + 100, next);

        verify(dataExtractor).newSearch(eq(1000L + 1L), eq(currentTime - queryDelayMs), any());
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        flushRequest.setAdvanceTime("1000");
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testEmptyDataCount() throws Exception {
        dataExtractor = mock(DataExtractor.class);
        when(dataExtractor.hasNext()).thenReturn(false);

        ScheduledJob scheduledJob = createScheduledJob(1000, 500, -1, -1);
        expectThrows(ScheduledJob.EmptyDataCountException.class, () -> scheduledJob.runLookBack(0L, 1000L));
    }

    public void testExtractionProblem() throws Exception {
        dataExtractor = mock(DataExtractor.class);
        when(dataExtractor.hasNext()).thenReturn(true);
        when(dataExtractor.next()).thenThrow(new IOException());

        ScheduledJob scheduledJob = createScheduledJob(1000, 500, -1, -1);
        expectThrows(ScheduledJob.ExtractionProblemException.class, () -> scheduledJob.runLookBack(0L, 1000L));

        currentTime = 3001;
        expectThrows(ScheduledJob.ExtractionProblemException.class, scheduledJob::runRealtime);

        ArgumentCaptor<Long> startTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> endTimeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(dataExtractor, times(2)).newSearch(startTimeCaptor.capture(), endTimeCaptor.capture(), any());
        assertEquals(0L, startTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(1000L, startTimeCaptor.getAllValues().get(1).longValue());
        assertEquals(1000L, endTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(2000L, endTimeCaptor.getAllValues().get(1).longValue());
    }

    public void testAnalysisProblem() throws Exception {
        client = mock(Client.class);
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        when(client.execute(same(JobDataAction.INSTANCE), eq(new JobDataAction.Request("_job_id")))).thenThrow(new RuntimeException());

        ScheduledJob scheduledJob = createScheduledJob(1000, 500, -1, -1);
        expectThrows(ScheduledJob.AnalysisProblemException.class, () -> scheduledJob.runLookBack(0L, 1000L));

        currentTime = 3001;
        expectThrows(ScheduledJob.EmptyDataCountException.class, scheduledJob::runRealtime);

        ArgumentCaptor<Long> startTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> endTimeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(dataExtractor, times(2)).newSearch(startTimeCaptor.capture(), endTimeCaptor.capture(), any());
        assertEquals(0L, startTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(1000L, startTimeCaptor.getAllValues().get(1).longValue());
        assertEquals(1000L, endTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(2000L, endTimeCaptor.getAllValues().get(1).longValue());
        verify(client, times(0)).execute(same(FlushJobAction.INSTANCE), any());
    }

    private ScheduledJob createScheduledJob(long frequencyMs, long queryDelayMs, long latestFinalBucketEndTimeMs,
                                            long latestRecordTimeMs) {
        Supplier<Long> currentTimeSupplier = () -> currentTime;
        return new ScheduledJob("_job_id", frequencyMs, queryDelayMs, dataExtractor, client, auditor,
                currentTimeSupplier, latestFinalBucketEndTimeMs, latestRecordTimeMs);
    }

}
