/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetector;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
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
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedJobTests extends ESTestCase {

    private static final String jobId = "_job_id";

    private AnomalyDetectionAuditor auditor;
    private DataExtractorFactory dataExtractorFactory;
    private DataExtractor dataExtractor;
    private DatafeedTimingStatsReporter timingStatsReporter;
    private Client client;
    private DelayedDataDetector delayedDataDetector;
    private DataDescription.Builder dataDescription;
    private ActionFuture<PostDataAction.Response> postDataFuture;
    private ActionFuture<FlushJobAction.Response> flushJobFuture;
    private ActionFuture<IndexResponse> indexFuture;
    private ArgumentCaptor<FlushJobAction.Request> flushJobRequests;
    private FlushJobAction.Response flushJobResponse;
    private String annotationDocId;

    private long currentTime;
    private XContentType xContentType;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        auditor = mock(AnomalyDetectionAuditor.class);
        dataExtractorFactory = mock(DataExtractorFactory.class);
        dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.XCONTENT);
        postDataFuture = mock(ActionFuture.class);
        flushJobFuture = mock(ActionFuture.class);
        indexFuture = mock(ActionFuture.class);
        annotationDocId = "AnnotationDocId";
        flushJobResponse = new FlushJobAction.Response(true, new Date());
        delayedDataDetector = mock(DelayedDataDetector.class);
        when(delayedDataDetector.getWindow()).thenReturn(DatafeedJob.MISSING_DATA_CHECK_INTERVAL_MS);
        currentTime = 0;
        xContentType = XContentType.JSON;

        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        byte[] contentBytes = "content".getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.next()).thenReturn(Optional.of(inputStream));
        DataCounts dataCounts = new DataCounts(jobId, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0),
                new Date(0), new Date(0), new Date(0));

        PostDataAction.Request expectedRequest = new PostDataAction.Request(jobId);
        expectedRequest.setDataDescription(dataDescription.build());
        expectedRequest.setContent(new BytesArray(contentBytes), xContentType);
        when(client.execute(same(PostDataAction.INSTANCE), eq(expectedRequest))).thenReturn(postDataFuture);
        when(postDataFuture.actionGet()).thenReturn(new PostDataAction.Response(dataCounts));

        flushJobRequests = ArgumentCaptor.forClass(FlushJobAction.Request.class);
        when(flushJobFuture.actionGet()).thenReturn(flushJobResponse);
        when(client.execute(same(FlushJobAction.INSTANCE), flushJobRequests.capture())).thenReturn(flushJobFuture);

        when(indexFuture.actionGet()).thenReturn(new IndexResponse(new ShardId("index", "uuid", 0), annotationDocId, 0, 0, 0, true));
        when(client.index(any())).thenReturn(indexFuture);
    }

    public void testLookBackRunWithEndTime() throws Exception {
        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, randomBoolean());
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        verify(dataExtractorFactory).newExtractor(0L, 1000L);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request(jobId);
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
        verify(client, never()).execute(same(PersistJobAction.INSTANCE), any());
    }

    public void testSetIsolated() throws Exception {
        currentTime = 2000L;
        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, randomBoolean());
        datafeedJob.isolate();
        assertNull(datafeedJob.runLookBack(0L, null));

        verify(dataExtractorFactory).newExtractor(0L, 1500L);
        verify(client, never()).execute(same(FlushJobAction.INSTANCE), any());
        verify(client, never()).execute(same(PersistJobAction.INSTANCE), any());
    }

    public void testLookBackRunWithNoEndTime() throws Exception {
        currentTime = 2000L;
        long frequencyMs = 1000;
        long queryDelayMs = 500;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, -1, -1, randomBoolean());
        long next = datafeedJob.runLookBack(0L, null);
        assertEquals(2000 + frequencyMs + queryDelayMs + 100, next);

        verify(dataExtractorFactory).newExtractor(0L, 1500L);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request(jobId);
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
        verify(client).execute(same(PersistJobAction.INSTANCE), eq(new PersistJobAction.Request(jobId)));
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
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, latestFinalBucketEndTimeMs, latestRecordTimeMs, true);
        long next = datafeedJob.runLookBack(0L, null);
        assertEquals(10000 + frequencyMs + queryDelayMs + 100, next);

        verify(dataExtractorFactory).newExtractor(5000 + 1L, currentTime - queryDelayMs);
        assertThat(flushJobRequests.getAllValues().size(), equalTo(1));
        FlushJobAction.Request flushRequest = new FlushJobAction.Request(jobId);
        flushRequest.setCalcInterim(true);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
        verify(client).execute(same(PersistJobAction.INSTANCE), eq(new PersistJobAction.Request(jobId)));
    }

    public void testContinueFromNow() throws Exception {
        // We need to return empty counts so that the lookback doesn't update the last end time
        when(postDataFuture.actionGet()).thenReturn(new PostDataAction.Response(new DataCounts(jobId)));

        currentTime = 9999L;
        long latestFinalBucketEndTimeMs = 5000;
        long latestRecordTimeMs = 5000;

        FlushJobAction.Response skipTimeResponse = new FlushJobAction.Response(true, new Date(10000L));
        when(flushJobFuture.actionGet()).thenReturn(skipTimeResponse);

        long frequencyMs = 1000;
        long queryDelayMs = 500;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, latestFinalBucketEndTimeMs, latestRecordTimeMs, true);
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
        flushJobResponse = new FlushJobAction.Response(true, new Date(2000));
        Bucket bucket = mock(Bucket.class);
        when(bucket.getTimestamp()).thenReturn(new Date(2000));
        when(bucket.getEpoch()).thenReturn(2L);
        when(bucket.getBucketSpan()).thenReturn(4L);
        when(flushJobFuture.actionGet()).thenReturn(flushJobResponse);
        when(client.execute(same(FlushJobAction.INSTANCE), flushJobRequests.capture())).thenReturn(flushJobFuture);
        when(delayedDataDetector.detectMissingData(2000))
            .thenReturn(Collections.singletonList(BucketWithMissingData.fromMissingAndBucket(10, bucket)));
        currentTime = 60000L;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, 1000, -1, false);
        long next = datafeedJob.runRealtime();
        assertEquals(currentTime + frequencyMs + 100, next);

        verify(dataExtractorFactory).newExtractor(1000L + 1L, currentTime - queryDelayMs);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request(jobId);
        flushRequest.setCalcInterim(true);
        flushRequest.setAdvanceTime("59000");
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
        verify(client, never()).execute(same(PersistJobAction.INSTANCE), any());

        // Execute a second valid time, but do so in a smaller window than the interval
        currentTime = 62000L;
        byte[] contentBytes = "content".getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(Optional.of(inputStream));
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        datafeedJob.runRealtime();

        // Execute a third time, but this time make sure we exceed the data check interval, but keep the delayedDataDetector response
        // the same
        currentTime = 62000L + DatafeedJob.MISSING_DATA_CHECK_INTERVAL_MS + 1;
        inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(Optional.of(inputStream));
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        datafeedJob.runRealtime();

        String msg = Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_MISSING_DATA,
            10,
            XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(2000));

        Annotation expectedAnnotation = new Annotation(msg,
            new Date(currentTime),
            XPackUser.NAME,
            bucket.getTimestamp(),
            new Date((bucket.getEpoch() + bucket.getBucketSpan()) * 1000),
            jobId,
            new Date(currentTime),
            XPackUser.NAME,
            "annotation");

        IndexRequest request = new IndexRequest(AnnotationIndex.WRITE_ALIAS_NAME);
        try (XContentBuilder xContentBuilder = expectedAnnotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
            request.source(xContentBuilder);
        }
        ArgumentCaptor<IndexRequest> indexRequestArgumentCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(client, atMost(2)).index(indexRequestArgumentCaptor.capture());
        assertThat(request.index(), equalTo(indexRequestArgumentCaptor.getValue().index()));
        assertThat(request.source(), equalTo(indexRequestArgumentCaptor.getValue().source()));

        // Execute a fourth time, this time we return a new delayedDataDetector response to verify annotation gets updated
        Bucket bucket2 = mock(Bucket.class);
        when(bucket2.getTimestamp()).thenReturn(new Date(6000));
        when(bucket2.getEpoch()).thenReturn(6L);
        when(bucket2.getBucketSpan()).thenReturn(4L);
        when(delayedDataDetector.detectMissingData(2000))
            .thenReturn(Arrays.asList(BucketWithMissingData.fromMissingAndBucket(10, bucket),
                BucketWithMissingData.fromMissingAndBucket(5, bucket2)));
        currentTime = currentTime + DatafeedJob.MISSING_DATA_CHECK_INTERVAL_MS + 1;
        inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(Optional.of(inputStream));
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        datafeedJob.runRealtime();

        msg = Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_MISSING_DATA,
            15,
            XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(6000));
        // What we expect the updated annotation to be indexed as
        IndexRequest indexRequest = new IndexRequest(AnnotationIndex.WRITE_ALIAS_NAME);
        indexRequest.id(annotationDocId);
        Annotation updatedAnnotation = new Annotation(expectedAnnotation);
        updatedAnnotation.setAnnotation(msg);
        updatedAnnotation.setModifiedTime(new Date(currentTime));
        updatedAnnotation.setModifiedUsername(XPackUser.NAME);
        updatedAnnotation.setEndTimestamp(new Date((bucket2.getEpoch() + bucket2.getBucketSpan()) * 1000));
        try (XContentBuilder xContentBuilder = updatedAnnotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
            indexRequest.source(xContentBuilder);
        }

        ArgumentCaptor<IndexRequest> updateRequestArgumentCaptor = ArgumentCaptor.forClass(IndexRequest.class);

        verify(client, atMost(2)).index(updateRequestArgumentCaptor.capture());
        assertThat(indexRequest.index(), equalTo(updateRequestArgumentCaptor.getValue().index()));
        assertThat(indexRequest.id(), equalTo(updateRequestArgumentCaptor.getValue().id()));
        assertThat(indexRequest.source().utf8ToString(),
            equalTo(updateRequestArgumentCaptor.getValue().source().utf8ToString()));
        assertThat(updateRequestArgumentCaptor.getValue().opType(), equalTo(DocWriteRequest.OpType.INDEX));

        // Execute a fifth time, no changes should occur as annotation is the same
        currentTime = currentTime + DatafeedJob.MISSING_DATA_CHECK_INTERVAL_MS + 1;
        inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(Optional.of(inputStream));
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        datafeedJob.runRealtime();

        // We should not get 3 index requests for the annotations
        verify(client, atMost(2)).index(any());
    }

    public void testEmptyDataCountGivenlookback() throws Exception {
        when(dataExtractor.hasNext()).thenReturn(false);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false);
        expectThrows(DatafeedJob.EmptyDataCountException.class, () -> datafeedJob.runLookBack(0L, 1000L));
        verify(client, times(1)).execute(same(FlushJobAction.INSTANCE), any());
        verify(client, never()).execute(same(PersistJobAction.INSTANCE), any());
        assertThat(flushJobRequests.getValue().getAdvanceTime(), is(nullValue()));
    }

    public void testExtractionProblem() throws Exception {
        when(dataExtractor.hasNext()).thenReturn(true);
        when(dataExtractor.next()).thenThrow(new IOException());

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, randomBoolean());
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
        verify(client, never()).execute(same(PersistJobAction.INSTANCE), any());
    }

    public void testPostAnalysisProblem() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        when(client.execute(same(PostDataAction.INSTANCE), any())).thenThrow(new RuntimeException());

        when(dataExtractor.getEndTime()).thenReturn(1000L);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, randomBoolean());
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
        verify(client, never()).execute(same(PersistJobAction.INSTANCE), any());
    }

    public void testPostAnalysisProblemIsConflict() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        when(client.execute(same(PostDataAction.INSTANCE), any())).thenThrow(ExceptionsHelper.conflictStatusException("conflict"));

        when(dataExtractor.getEndTime()).thenReturn(1000L);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, randomBoolean());
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
        verify(client, never()).execute(same(PersistJobAction.INSTANCE), any());
    }

    public void testFlushAnalysisProblem() {
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenThrow(new RuntimeException());

        currentTime = 60000L;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, 1000, -1, randomBoolean());
        DatafeedJob.AnalysisProblemException analysisProblemException =
                expectThrows(DatafeedJob.AnalysisProblemException.class, () -> datafeedJob.runRealtime());
        assertThat(analysisProblemException.shouldStop, is(false));
    }

    public void testFlushAnalysisProblemIsConflict() {
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenThrow(ExceptionsHelper.conflictStatusException("conflict"));

        currentTime = 60000L;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, 1000, -1, randomBoolean());
        DatafeedJob.AnalysisProblemException analysisProblemException =
                expectThrows(DatafeedJob.AnalysisProblemException.class, () -> datafeedJob.runRealtime());
        assertThat(analysisProblemException.shouldStop, is(true));
    }

    private DatafeedJob createDatafeedJob(long frequencyMs, long queryDelayMs, long latestFinalBucketEndTimeMs,
                                          long latestRecordTimeMs, boolean haveSeenDataPreviously) {
        Supplier<Long> currentTimeSupplier = () -> currentTime;
        return new DatafeedJob(jobId, dataDescription.build(), frequencyMs, queryDelayMs, dataExtractorFactory, timingStatsReporter,
            client, auditor, currentTimeSupplier, delayedDataDetector, null, latestFinalBucketEndTimeMs, latestRecordTimeMs,
            haveSeenDataPreviously);
    }
}
