/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedJobTests extends ESTestCase {

    private Auditor auditor;
    private DataExtractorFactory dataExtractorFactory;
    private DataExtractor dataExtractor;
    private Client client;
    private DataDescription.Builder dataDescription;
    ActionFuture<PostDataAction.Response> postDataFuture;
    private ActionFuture<FlushJobAction.Response> flushJobFuture;
    private ArgumentCaptor<FlushJobAction.Request> flushJobRequests;

    private long currentTime;
    private XContentType xContentType;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        auditor = mock(Auditor.class);
        dataExtractorFactory = mock(DataExtractorFactory.class);
        dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.XCONTENT);
        postDataFuture = mock(ActionFuture.class);
        flushJobFuture = mock(ActionFuture.class);
        currentTime = 0;
        xContentType = XContentType.JSON;

        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        byte[] contentBytes = "content".getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.next()).thenReturn(Optional.of(inputStream));
        DataCounts dataCounts = new DataCounts("_job_id", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0), 
                new Date(0), new Date(0), new Date(0));

        PostDataAction.Request expectedRequest = new PostDataAction.Request("_job_id");
        expectedRequest.setDataDescription(dataDescription.build());
        expectedRequest.setContent(new BytesArray(contentBytes), xContentType);
        when(client.execute(same(PostDataAction.INSTANCE), eq(expectedRequest))).thenReturn(postDataFuture);
        when(postDataFuture.actionGet()).thenReturn(new PostDataAction.Response(dataCounts));

        flushJobRequests = ArgumentCaptor.forClass(FlushJobAction.Request.class);
        when(client.execute(same(FlushJobAction.INSTANCE), flushJobRequests.capture())).thenReturn(flushJobFuture);
    }

    public void testLookBackRunWithEndTime() throws Exception {
        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        verify(dataExtractorFactory).newExtractor(0L, 1000L);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testSetIsolated() throws Exception {
        currentTime = 2000L;
        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1);
        datafeedJob.isolate();
        assertNull(datafeedJob.runLookBack(0L, null));

        verify(dataExtractorFactory).newExtractor(0L, 1500L);
        verify(client, never()).execute(same(FlushJobAction.INSTANCE), any());
    }

    public void testLookBackRunWithNoEndTime() throws Exception {
        currentTime = 2000L;
        long frequencyMs = 1000;
        long queryDelayMs = 500;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, -1, -1);
        long next = datafeedJob.runLookBack(0L, null);
        assertEquals(2000 + frequencyMs + queryDelayMs + 100, next);

        verify(dataExtractorFactory).newExtractor(0L, 1500L);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testLookBackRunWithStartTimeEarlierThanResumePoint() throws Exception {
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
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, latestFinalBucketEndTimeMs, latestRecordTimeMs);
        long next = datafeedJob.runLookBack(0L, null);
        assertEquals(10000 + frequencyMs + queryDelayMs + 100, next);

        verify(dataExtractorFactory).newExtractor(5000 + 1L, currentTime - queryDelayMs);
        assertThat(flushJobRequests.getAllValues().size(), equalTo(1));
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testContinueFromNow() throws Exception {
        // We need to return empty counts so that the lookback doesn't update the last end time
        when(postDataFuture.actionGet()).thenReturn(new PostDataAction.Response(new DataCounts("_job_id")));

        currentTime = 9999L;
        long latestFinalBucketEndTimeMs = 5000;
        long latestRecordTimeMs = 5000;

        FlushJobAction.Response skipTimeResponse = new FlushJobAction.Response(true, new Date(10000L));
        when(flushJobFuture.actionGet()).thenReturn(skipTimeResponse);

        long frequencyMs = 1000;
        long queryDelayMs = 500;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, latestFinalBucketEndTimeMs, latestRecordTimeMs);
        datafeedJob.runLookBack(currentTime, null);

        // advance time
        currentTime = 12000L;

        expectThrows(DatafeedJob.EmptyDataCountException.class, () -> datafeedJob.runRealtime());

        verify(dataExtractorFactory, times(1)).newExtractor(10000L, 11000L);
        List<FlushJobAction.Request> capturedFlushJobRequests = flushJobRequests.getAllValues();
        assertThat(capturedFlushJobRequests.size(), equalTo(2));
        assertThat(capturedFlushJobRequests.get(0).getCalcInterim(), is(false));
        assertThat(capturedFlushJobRequests.get(0).getSkipTime(), equalTo("9999"));
        assertThat(capturedFlushJobRequests.get(1).getCalcInterim(), is(true));
        assertThat(capturedFlushJobRequests.get(1).getSkipTime(), is(nullValue()));
        assertThat(capturedFlushJobRequests.get(1).getAdvanceTime(), equalTo("11000"));
        Mockito.verifyNoMoreInteractions(dataExtractorFactory);
    }

    public void testRealtimeRun() throws Exception {
        currentTime = 60000L;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, 1000, -1);
        long next = datafeedJob.runRealtime();
        assertEquals(currentTime + frequencyMs + queryDelayMs + 100, next);

        verify(dataExtractorFactory).newExtractor(1000L + 1L, currentTime - queryDelayMs);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request("_job_id");
        flushRequest.setCalcInterim(true);
        flushRequest.setAdvanceTime("59000");
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
    }

    public void testEmptyDataCountGivenlookback() throws Exception {
        when(dataExtractor.hasNext()).thenReturn(false);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1);
        expectThrows(DatafeedJob.EmptyDataCountException.class, () -> datafeedJob.runLookBack(0L, 1000L));
        verify(client, times(1)).execute(same(FlushJobAction.INSTANCE), any());
        assertThat(flushJobRequests.getValue().getAdvanceTime(), is(nullValue()));
    }

    public void testExtractionProblem() throws Exception {
        when(dataExtractor.hasNext()).thenReturn(true);
        when(dataExtractor.next()).thenThrow(new IOException());

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1);
        expectThrows(DatafeedJob.ExtractionProblemException.class, () -> datafeedJob.runLookBack(0L, 1000L));

        currentTime = 3001;
        expectThrows(DatafeedJob.ExtractionProblemException.class, datafeedJob::runRealtime);

        ArgumentCaptor<Long> startTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> endTimeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(dataExtractorFactory, times(2)).newExtractor(startTimeCaptor.capture(), endTimeCaptor.capture());
        assertEquals(0L, startTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(0L, startTimeCaptor.getAllValues().get(1).longValue());
        assertEquals(1000L, endTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(2000L, endTimeCaptor.getAllValues().get(1).longValue());
        assertThat(flushJobRequests.getAllValues().isEmpty(), is(true));
    }

    public void testPostAnalysisProblem() throws Exception {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        when(client.execute(same(PostDataAction.INSTANCE), any())).thenThrow(new RuntimeException());

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1);
        DatafeedJob.AnalysisProblemException analysisProblemException =
                expectThrows(DatafeedJob.AnalysisProblemException.class, () -> datafeedJob.runLookBack(0L, 1000L));
        assertThat(analysisProblemException.shouldStop, is(false));

        currentTime = 3001;
        expectThrows(DatafeedJob.EmptyDataCountException.class, datafeedJob::runRealtime);

        ArgumentCaptor<Long> startTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> endTimeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(dataExtractorFactory, times(2)).newExtractor(startTimeCaptor.capture(), endTimeCaptor.capture());
        assertEquals(0L, startTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(1000L, startTimeCaptor.getAllValues().get(1).longValue());
        assertEquals(1000L, endTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(2000L, endTimeCaptor.getAllValues().get(1).longValue());
        verify(client, times(1)).execute(same(FlushJobAction.INSTANCE), any());
    }

    public void testPostAnalysisProblemIsConflict() throws Exception {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        when(client.execute(same(PostDataAction.INSTANCE), any())).thenThrow(ExceptionsHelper.conflictStatusException("conflict"));

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1);
        DatafeedJob.AnalysisProblemException analysisProblemException =
                expectThrows(DatafeedJob.AnalysisProblemException.class, () -> datafeedJob.runLookBack(0L, 1000L));
        assertThat(analysisProblemException.shouldStop, is(true));

        currentTime = 3001;
        expectThrows(DatafeedJob.EmptyDataCountException.class, datafeedJob::runRealtime);

        ArgumentCaptor<Long> startTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> endTimeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(dataExtractorFactory, times(2)).newExtractor(startTimeCaptor.capture(), endTimeCaptor.capture());
        assertEquals(0L, startTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(1000L, startTimeCaptor.getAllValues().get(1).longValue());
        assertEquals(1000L, endTimeCaptor.getAllValues().get(0).longValue());
        assertEquals(2000L, endTimeCaptor.getAllValues().get(1).longValue());
        verify(client, times(1)).execute(same(FlushJobAction.INSTANCE), any());
    }

    public void testFlushAnalysisProblem() throws Exception {
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenThrow(new RuntimeException());

        currentTime = 60000L;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, 1000, -1);
        DatafeedJob.AnalysisProblemException analysisProblemException =
                expectThrows(DatafeedJob.AnalysisProblemException.class, () -> datafeedJob.runRealtime());
        assertThat(analysisProblemException.shouldStop, is(false));
    }

    public void testFlushAnalysisProblemIsConflict() throws Exception {
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenThrow(ExceptionsHelper.conflictStatusException("conflict"));

        currentTime = 60000L;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, 1000, -1);
        DatafeedJob.AnalysisProblemException analysisProblemException =
                expectThrows(DatafeedJob.AnalysisProblemException.class, () -> datafeedJob.runRealtime());
        assertThat(analysisProblemException.shouldStop, is(true));
    }

    private DatafeedJob createDatafeedJob(long frequencyMs, long queryDelayMs, long latestFinalBucketEndTimeMs,
                                            long latestRecordTimeMs) {
        Supplier<Long> currentTimeSupplier = () -> currentTime;
        return new DatafeedJob("_job_id", dataDescription.build(), frequencyMs, queryDelayMs, dataExtractorFactory, client, auditor,
                currentTimeSupplier, latestFinalBucketEndTimeMs, latestRecordTimeMs);
    }
}
