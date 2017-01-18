/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.job.DataCounts;
import org.elasticsearch.xpack.ml.job.DataDescription;
import org.elasticsearch.xpack.ml.job.audit.Auditor;
import org.elasticsearch.xpack.ml.scheduler.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.scheduler.extractor.DataExtractorFactory;
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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScheduledJobTests extends ESTestCase {

    private Auditor auditor;
    private DataExtractorFactory dataExtractorFactory;
    private DataExtractor dataExtractor;
    private Client client;
    private DataDescription.Builder dataDescription;
    private ActionFuture<FlushJobAction.Response> flushJobFuture;

    private long currentTime;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        auditor = mock(Auditor.class);
        dataExtractorFactory = mock(DataExtractorFactory.class);
        dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        client = mock(Client.class);
        dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.JSON);
        ActionFuture<PostDataAction.Response> jobDataFuture = mock(ActionFuture.class);
        flushJobFuture = mock(ActionFuture.class);
        currentTime = 0;

        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream inputStream = new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8));
        when(dataExtractor.next()).thenReturn(Optional.of(inputStream));
        DataCounts dataCounts = new DataCounts("_job_id", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));

        PostDataAction.Request expectedRequest = new PostDataAction.Request("_job_id");
        expectedRequest.setDataDescription(dataDescription.build());
        when(client.execute(same(PostDataAction.INSTANCE), eq(expectedRequest))).thenReturn(jobDataFuture);
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        when(jobDataFuture.get()).thenReturn(new PostDataAction.Response(dataCounts));
    }

    public void testLookBackRunWithEndTime() throws Exception {
        ScheduledJob scheduledJob = createScheduledJob(1000, 500, -1, -1);
        assertNull(scheduledJob.runLookBack(0L, 1000L));

        verify(dataExtractorFactory).newExtractor(0L, 1000L);
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

        verify(dataExtractorFactory).newExtractor(0L, 1500L);
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

        verify(dataExtractorFactory).newExtractor(5000 + 1L, currentTime - queryDelayMs);
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

        verify(dataExtractorFactory).newExtractor(1000L + 1L, currentTime - queryDelayMs);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        flushRequest.setAdvanceTime("1000");
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testEmptyDataCount() throws Exception {
        when(dataExtractor.hasNext()).thenReturn(false);

        ScheduledJob scheduledJob = createScheduledJob(1000, 500, -1, -1);
        expectThrows(ScheduledJob.EmptyDataCountException.class, () -> scheduledJob.runLookBack(0L, 1000L));
    }

    public void testExtractionProblem() throws Exception {
        when(dataExtractor.hasNext()).thenReturn(true);
        when(dataExtractor.next()).thenThrow(new IOException());

        ScheduledJob scheduledJob = createScheduledJob(1000, 500, -1, -1);
        expectThrows(ScheduledJob.ExtractionProblemException.class, () -> scheduledJob.runLookBack(0L, 1000L));

        currentTime = 3001;
        expectThrows(ScheduledJob.ExtractionProblemException.class, scheduledJob::runRealtime);

        ArgumentCaptor<Long> startTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> endTimeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(dataExtractorFactory, times(2)).newExtractor(startTimeCaptor.capture(), endTimeCaptor.capture());
        assertEquals(0L, startTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(1000L, startTimeCaptor.getAllValues().get(1).longValue());
        assertEquals(1000L, endTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(2000L, endTimeCaptor.getAllValues().get(1).longValue());
    }

    public void testAnalysisProblem() throws Exception {
        client = mock(Client.class);
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        when(client.execute(same(PostDataAction.INSTANCE), eq(new PostDataAction.Request("_job_id")))).thenThrow(new RuntimeException());

        ScheduledJob scheduledJob = createScheduledJob(1000, 500, -1, -1);
        expectThrows(ScheduledJob.AnalysisProblemException.class, () -> scheduledJob.runLookBack(0L, 1000L));

        currentTime = 3001;
        expectThrows(ScheduledJob.EmptyDataCountException.class, scheduledJob::runRealtime);

        ArgumentCaptor<Long> startTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> endTimeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(dataExtractorFactory, times(2)).newExtractor(startTimeCaptor.capture(), endTimeCaptor.capture());
        assertEquals(0L, startTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(1000L, startTimeCaptor.getAllValues().get(1).longValue());
        assertEquals(1000L, endTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(2000L, endTimeCaptor.getAllValues().get(1).longValue());
        verify(client, times(0)).execute(same(FlushJobAction.INSTANCE), any());
    }

    private ScheduledJob createScheduledJob(long frequencyMs, long queryDelayMs, long latestFinalBucketEndTimeMs,
                                            long latestRecordTimeMs) {
        Supplier<Long> currentTimeSupplier = () -> currentTime;
        return new ScheduledJob("_job_id", dataDescription.build(), frequencyMs, queryDelayMs, dataExtractorFactory, client, auditor,
                currentTimeSupplier, latestFinalBucketEndTimeMs, latestRecordTimeMs);
    }

}
