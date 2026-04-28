/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetector;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterServiceTests;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.xpack.ml.MachineLearning.DELAYED_DATA_CHECK_FREQ;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedJobTests extends ESTestCase {

    private static final String jobId = "_job_id";

    private static final long DELAYED_DATA_WINDOW = TimeValue.timeValueMinutes(15).millis();
    private static final long DELAYED_DATA_FREQ = TimeValue.timeValueMinutes(2).millis();
    private static final long DELAYED_DATA_FREQ_HALF = TimeValue.timeValueMinutes(1).millis();

    private AnomalyDetectionAuditor auditor;
    private DataExtractorFactory dataExtractorFactory;
    private DataExtractor dataExtractor;
    private DatafeedTimingStatsReporter timingStatsReporter;
    private Client client;
    private ResultsPersisterService resultsPersisterService;
    private DelayedDataDetector delayedDataDetector;
    private DataDescription.Builder dataDescription;
    private ActionFuture<PostDataAction.Response> postDataFuture;
    private ActionFuture<FlushJobAction.Response> flushJobFuture;
    private ArgumentCaptor<FlushJobAction.Request> flushJobRequests;
    private FlushJobAction.Response flushJobResponse;
    private String annotationDocId;

    private long currentTime;

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
        resultsPersisterService = ResultsPersisterServiceTests.buildResultsPersisterService(
            new OriginSettingClient(client, ClientHelper.ML_ORIGIN)
        );
        dataDescription = new DataDescription.Builder();
        postDataFuture = mock(ActionFuture.class);
        flushJobFuture = mock(ActionFuture.class);
        annotationDocId = "AnnotationDocId";
        flushJobResponse = new FlushJobAction.Response(true, Instant.now());
        delayedDataDetector = mock(DelayedDataDetector.class);
        when(delayedDataDetector.getWindow()).thenReturn(DELAYED_DATA_WINDOW);
        currentTime = 0;
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        byte[] contentBytes = "content".getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.next()).thenReturn(
            new DataExtractor.Result(new SearchInterval(1000L, 2000L), Optional.of(inputStream), List.of())
        );
        DataCounts dataCounts = new DataCounts(
            jobId,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            new Date(0),
            new Date(0),
            new Date(0),
            new Date(0),
            new Date(0),
            Instant.now()
        );

        PostDataAction.Request expectedRequest = new PostDataAction.Request(jobId);
        expectedRequest.setDataDescription(dataDescription.build());
        expectedRequest.setContent(new BytesArray(contentBytes), XContentType.JSON);
        when(client.execute(same(PostDataAction.INSTANCE), eq(expectedRequest))).thenReturn(postDataFuture);
        when(postDataFuture.actionGet()).thenReturn(new PostDataAction.Response(dataCounts));

        flushJobRequests = ArgumentCaptor.forClass(FlushJobAction.Request.class);
        when(flushJobFuture.actionGet()).thenReturn(flushJobResponse);
        when(client.execute(same(FlushJobAction.INSTANCE), flushJobRequests.capture())).thenReturn(flushJobFuture);

        doAnswer(withResponse(new BulkResponse(new BulkItemResponse[] { bulkItemSuccess(annotationDocId) }, 0L))).when(client)
            .execute(eq(TransportBulkAction.TYPE), any(), any());
    }

    public void testLookBackRunWithEndTime() throws Exception {
        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, randomBoolean());
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        verify(dataExtractorFactory).newExtractor(0L, 1000L);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request(jobId);
        flushRequest.setCalcInterim(true);
        flushRequest.setRefreshRequired(false);
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
        flushRequest.setRefreshRequired(false);
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
        flushRequest.setRefreshRequired(false);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
        verify(client).execute(same(PersistJobAction.INSTANCE), eq(new PersistJobAction.Request(jobId)));
    }

    public void testContinueFromNow() throws Exception {
        // We need to return empty counts so that the lookback doesn't update the last end time
        when(postDataFuture.actionGet()).thenReturn(new PostDataAction.Response(new DataCounts(jobId)));

        currentTime = 9999L;
        long latestFinalBucketEndTimeMs = 5000;
        long latestRecordTimeMs = 5000;

        FlushJobAction.Response skipTimeResponse = new FlushJobAction.Response(true, Instant.ofEpochMilli(10000L));
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
        flushJobResponse = new FlushJobAction.Response(true, Instant.ofEpochMilli(2000));
        Bucket bucket = mock(Bucket.class);
        when(bucket.getTimestamp()).thenReturn(new Date(2000));
        when(bucket.getEpoch()).thenReturn(2L);
        when(bucket.getBucketSpan()).thenReturn(4L);
        when(flushJobFuture.actionGet()).thenReturn(flushJobResponse);
        when(client.execute(same(FlushJobAction.INSTANCE), flushJobRequests.capture())).thenReturn(flushJobFuture);
        when(delayedDataDetector.detectMissingData(2000)).thenReturn(
            Collections.singletonList(BucketWithMissingData.fromMissingAndBucket(10, bucket))
        );
        currentTime = DELAYED_DATA_FREQ_HALF;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, 1000, -1, false, DELAYED_DATA_FREQ);
        long next = datafeedJob.runRealtime();
        assertEquals(currentTime + frequencyMs + 100, next);

        verify(dataExtractorFactory).newExtractor(1000L + 1L, currentTime - queryDelayMs);
        FlushJobAction.Request flushRequest = new FlushJobAction.Request(jobId);
        flushRequest.setCalcInterim(true);
        flushRequest.setAdvanceTime("59000");
        flushRequest.setWaitForNormalization(false);
        flushRequest.setRefreshRequired(false);
        verify(client).execute(same(FlushJobAction.INSTANCE), eq(flushRequest));
        verify(client, never()).execute(same(PersistJobAction.INSTANCE), any());

        // Execute a second valid time, but do so in a smaller window than the interval
        currentTime = currentTime + DELAYED_DATA_FREQ_HALF - 1;
        byte[] contentBytes = "content".getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(
            new DataExtractor.Result(new SearchInterval(1000L, 2000L), Optional.of(inputStream), List.of())
        );
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        datafeedJob.runRealtime();

        // Execute a third time, but this time make sure we exceed the data check interval, but keep the delayedDataDetector response
        // the same
        currentTime = currentTime + DELAYED_DATA_FREQ_HALF;
        inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(
            new DataExtractor.Result(new SearchInterval(1000L, 2000L), Optional.of(inputStream), List.of())
        );
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        datafeedJob.runRealtime();

        String msg = Messages.getMessage(
            Messages.JOB_AUDIT_DATAFEED_MISSING_DATA,
            10,
            XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(2000))
        );

        long annotationCreateTime = currentTime;
        {  // What we expect the created annotation to be indexed as
            Annotation expectedAnnotation = new Annotation.Builder().setAnnotation(msg)
                .setCreateTime(new Date(annotationCreateTime))
                .setCreateUsername(InternalUsers.XPACK_USER.principal())
                .setTimestamp(bucket.getTimestamp())
                .setEndTimestamp(new Date((bucket.getEpoch() + bucket.getBucketSpan()) * 1000))
                .setJobId(jobId)
                .setModifiedTime(new Date(annotationCreateTime))
                .setModifiedUsername(InternalUsers.XPACK_USER.principal())
                .setType(Annotation.Type.ANNOTATION)
                .setEvent(Annotation.Event.DELAYED_DATA)
                .build();
            BytesReference expectedSource = BytesReference.bytes(
                expectedAnnotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            );

            ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
            verify(client, atMost(2)).execute(eq(TransportBulkAction.TYPE), bulkRequestArgumentCaptor.capture(), any());
            BulkRequest bulkRequest = bulkRequestArgumentCaptor.getValue();
            assertThat(bulkRequest.requests(), hasSize(1));
            IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
            assertThat(indexRequest.index(), equalTo(AnnotationIndex.WRITE_ALIAS_NAME));
            assertThat(indexRequest.id(), nullValue());
            assertThat(indexRequest.source().utf8ToString(), equalTo(expectedSource.utf8ToString()));
            assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.INDEX));
        }

        // Execute a fourth time, this time we return a new delayedDataDetector response to verify annotation gets updated
        Bucket bucket2 = mock(Bucket.class);
        when(bucket2.getTimestamp()).thenReturn(new Date(6000));
        when(bucket2.getEpoch()).thenReturn(6L);
        when(bucket2.getBucketSpan()).thenReturn(4L);
        when(delayedDataDetector.detectMissingData(2000)).thenReturn(
            Arrays.asList(BucketWithMissingData.fromMissingAndBucket(10, bucket), BucketWithMissingData.fromMissingAndBucket(5, bucket2))
        );
        currentTime = currentTime + DELAYED_DATA_WINDOW + 1;
        inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(
            new DataExtractor.Result(new SearchInterval(1000L, 2000L), Optional.of(inputStream), List.of())
        );
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        datafeedJob.runRealtime();

        msg = Messages.getMessage(
            Messages.JOB_AUDIT_DATAFEED_MISSING_DATA,
            15,
            XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(6000))
        );

        long annotationUpdateTime = currentTime;
        {  // What we expect the updated annotation to be indexed as
            Annotation expectedUpdatedAnnotation = new Annotation.Builder().setAnnotation(msg)
                .setCreateTime(new Date(annotationCreateTime))
                .setCreateUsername(InternalUsers.XPACK_USER.principal())
                .setTimestamp(bucket.getTimestamp())
                .setEndTimestamp(new Date((bucket2.getEpoch() + bucket2.getBucketSpan()) * 1000))
                .setJobId(jobId)
                .setModifiedTime(new Date(annotationUpdateTime))
                .setModifiedUsername(InternalUsers.XPACK_USER.principal())
                .setType(Annotation.Type.ANNOTATION)
                .setEvent(Annotation.Event.DELAYED_DATA)
                .build();
            BytesReference expectedSource = BytesReference.bytes(
                expectedUpdatedAnnotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            );

            ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
            verify(client, atMost(2)).execute(eq(TransportBulkAction.TYPE), bulkRequestArgumentCaptor.capture(), any());
            BulkRequest bulkRequest = bulkRequestArgumentCaptor.getValue();
            assertThat(bulkRequest.requests(), hasSize(1));
            IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
            assertThat(indexRequest.index(), equalTo(AnnotationIndex.WRITE_ALIAS_NAME));
            assertThat(indexRequest.id(), equalTo(annotationDocId));
            assertThat(indexRequest.source(), equalBytes(expectedSource));
            assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.INDEX));
        }

        // Execute a fifth time, no changes should occur as annotation is the same
        currentTime = currentTime + DELAYED_DATA_WINDOW + 1;
        inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenReturn(
            new DataExtractor.Result(new SearchInterval(1000L, 2000L), Optional.of(inputStream), List.of())
        );
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);
        datafeedJob.runRealtime();

        // We should not get 3 index requests for the annotations
        verify(client, atMost(2)).index(any());
    }

    public void testEmptyDataCountGivenlookback() {
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
        DatafeedJob.AnalysisProblemException analysisProblemException = expectThrows(
            DatafeedJob.AnalysisProblemException.class,
            () -> datafeedJob.runLookBack(0L, 1000L)
        );
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
        DatafeedJob.AnalysisProblemException analysisProblemException = expectThrows(
            DatafeedJob.AnalysisProblemException.class,
            () -> datafeedJob.runLookBack(0L, 1000L)
        );
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
        DatafeedJob.AnalysisProblemException analysisProblemException = expectThrows(
            DatafeedJob.AnalysisProblemException.class,
            () -> datafeedJob.runRealtime()
        );
        assertThat(analysisProblemException.shouldStop, is(false));
    }

    public void testFlushAnalysisProblemIsConflict() {
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenThrow(ExceptionsHelper.conflictStatusException("conflict"));

        currentTime = 60000L;
        long frequencyMs = 100;
        long queryDelayMs = 1000;
        DatafeedJob datafeedJob = createDatafeedJob(frequencyMs, queryDelayMs, 1000, -1, randomBoolean());
        DatafeedJob.AnalysisProblemException analysisProblemException = expectThrows(
            DatafeedJob.AnalysisProblemException.class,
            () -> datafeedJob.runRealtime()
        );
        assertThat(analysisProblemException.shouldStop, is(true));
    }

    public void testNonCcsDatafeedDoesNotTriggerScopeChange() throws Exception {
        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, randomBoolean());
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        verify(auditor, never()).warning(eq(jobId), argThat(msg -> msg.contains("scope changed")));
        verify(client, never()).execute(same(GetBucketsAction.INSTANCE), any());
    }

    public void testScopeChangeAnnotationAndAnomalyLookback() throws Exception {
        CrossClusterSearchStats stats = new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime));

        List<LinkedClusterState> baseline = List.of(new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10));
        List<LinkedClusterState> withNewProject = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("new_project", LinkedClusterState.Status.AVAILABLE, null, 20)
        );

        // Establish baseline
        currentTime = 1_000_000L;
        stats.update(baseline);

        // Build up 11 consecutive presences spanning > 5 minutes
        for (int i = 0; i < 11; i++) {
            currentTime += 30_000;
            stats.update(withNewProject);
        }

        // The next cycle (12th) will confirm the scope change
        currentTime += 30_000;

        // Set up extractor to return withNewProject states
        resetExtractorForCcsTest(withNewProject);

        // Mock GetBucketsAction to return one elevated bucket
        @SuppressWarnings("unchecked")
        ActionFuture<GetBucketsAction.Response> getBucketsFuture = mock(ActionFuture.class);
        Bucket elevatedBucket = mock(Bucket.class);
        when(elevatedBucket.getTimestamp()).thenReturn(new Date(currentTime - 10_000));
        GetBucketsAction.Response bucketsResponse = new GetBucketsAction.Response(
            new QueryPage<>(List.of(elevatedBucket), 1, Bucket.RESULTS_FIELD)
        );
        when(getBucketsFuture.actionGet()).thenReturn(bucketsResponse);
        when(client.execute(same(GetBucketsAction.INSTANCE), any())).thenReturn(getBucketsFuture);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false, DELAYED_DATA_FREQ, stats);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        // Verify scope change annotation was persisted
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, atMost(2)).execute(eq(TransportBulkAction.TYPE), bulkCaptor.capture(), any());
        BulkRequest annotationBulk = bulkCaptor.getValue();
        assertThat(annotationBulk.requests(), hasSize(1));
        IndexRequest indexRequest = (IndexRequest) annotationBulk.requests().get(0);
        String annotationSource = indexRequest.source().utf8ToString();
        assertThat(annotationSource, containsString("search_scope_changed"));
        assertThat(annotationSource, containsString("new_project"));

        // Verify scope change warning was emitted
        ArgumentCaptor<String> warningCaptor = ArgumentCaptor.forClass(String.class);
        verify(auditor, times(2)).warning(eq(jobId), warningCaptor.capture());
        List<String> warnings = warningCaptor.getAllValues();
        assertThat(warnings.get(0), containsString("new_project"));
        assertThat(warnings.get(0), containsString("linked"));

        // Verify anomaly correlation warning was emitted
        assertThat(warnings.get(1), containsString("Elevated anomaly scores"));
        assertThat(warnings.get(1), containsString("1"));

        // Verify GetBucketsAction was called with correct parameters
        ArgumentCaptor<GetBucketsAction.Request> bucketsRequestCaptor = ArgumentCaptor.forClass(GetBucketsAction.Request.class);
        verify(client).execute(same(GetBucketsAction.INSTANCE), bucketsRequestCaptor.capture());
        GetBucketsAction.Request bucketsRequest = bucketsRequestCaptor.getValue();
        assertThat(bucketsRequest.getJobId(), equalTo(jobId));
        assertThat(bucketsRequest.isExcludeInterim(), is(true));
        assertThat(bucketsRequest.getAnomalyScore(), equalTo(75.0));
    }

    public void testScopeChangeNoAnomaliesEmitsOnlyOneWarning() throws Exception {
        CrossClusterSearchStats stats = new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime));

        List<LinkedClusterState> baseline = List.of(new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10));
        List<LinkedClusterState> withNewProject = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("new_project", LinkedClusterState.Status.AVAILABLE, null, 20)
        );

        currentTime = 1_000_000L;
        stats.update(baseline);
        for (int i = 0; i < 11; i++) {
            currentTime += 30_000;
            stats.update(withNewProject);
        }
        currentTime += 30_000;

        resetExtractorForCcsTest(withNewProject);

        // Mock GetBucketsAction to return zero elevated buckets
        @SuppressWarnings("unchecked")
        ActionFuture<GetBucketsAction.Response> getBucketsFuture = mock(ActionFuture.class);
        GetBucketsAction.Response emptyResponse = new GetBucketsAction.Response(new QueryPage<>(List.of(), 0, Bucket.RESULTS_FIELD));
        when(getBucketsFuture.actionGet()).thenReturn(emptyResponse);
        when(client.execute(same(GetBucketsAction.INSTANCE), any())).thenReturn(getBucketsFuture);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false, DELAYED_DATA_FREQ, stats);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        // Only one warning (scope change), no anomaly warning
        verify(auditor, times(1)).warning(eq(jobId), argThat(msg -> msg.contains("linked")));
        verify(auditor, never()).warning(eq(jobId), argThat(msg -> msg.contains("Elevated anomaly")));
    }

    public void testUnlinkAnomalyCorrelationWarning() throws Exception {
        CrossClusterSearchStats stats = new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime));

        List<LinkedClusterState> baseline = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("removed_project", LinkedClusterState.Status.AVAILABLE, null, 15)
        );
        List<LinkedClusterState> afterUnlink = List.of(new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10));

        currentTime = 1_000_000L;
        stats.update(baseline);

        for (int i = 0; i < 11; i++) {
            currentTime += 30_000;
            stats.update(afterUnlink);
        }
        currentTime += 30_000;

        resetExtractorForCcsTest(afterUnlink);

        @SuppressWarnings("unchecked")
        ActionFuture<GetBucketsAction.Response> getBucketsFuture = mock(ActionFuture.class);
        Bucket b1 = mock(Bucket.class);
        when(b1.getTimestamp()).thenReturn(new Date(currentTime - 20_000));
        Bucket b2 = mock(Bucket.class);
        when(b2.getTimestamp()).thenReturn(new Date(currentTime - 10_000));
        Bucket b3 = mock(Bucket.class);
        when(b3.getTimestamp()).thenReturn(new Date(currentTime - 5_000));
        GetBucketsAction.Response bucketsResponse = new GetBucketsAction.Response(
            new QueryPage<>(List.of(b1, b2, b3), 3, Bucket.RESULTS_FIELD)
        );
        when(getBucketsFuture.actionGet()).thenReturn(bucketsResponse);
        when(client.execute(same(GetBucketsAction.INSTANCE), any())).thenReturn(getBucketsFuture);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false, DELAYED_DATA_FREQ, stats);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        ArgumentCaptor<String> warningCaptor = ArgumentCaptor.forClass(String.class);
        verify(auditor, times(2)).warning(eq(jobId), warningCaptor.capture());
        List<String> warnings = warningCaptor.getAllValues();

        // First warning: scope change with unlink
        assertThat(warnings.get(0), containsString("unlinked"));
        assertThat(warnings.get(0), containsString("removed_project"));

        // Second warning: anomaly correlation referencing the unlinked project and bucket count
        assertThat(warnings.get(1), containsString("Elevated anomaly scores"));
        assertThat(warnings.get(1), containsString("removed_project unlinked"));
        assertThat(warnings.get(1), containsString("3"));

        // Verify GetBucketsAction request parameters
        ArgumentCaptor<GetBucketsAction.Request> bucketsRequestCaptor = ArgumentCaptor.forClass(GetBucketsAction.Request.class);
        verify(client).execute(same(GetBucketsAction.INSTANCE), bucketsRequestCaptor.capture());
        GetBucketsAction.Request bucketsRequest = bucketsRequestCaptor.getValue();
        assertThat(bucketsRequest.getJobId(), equalTo(jobId));
        assertThat(bucketsRequest.isExcludeInterim(), is(true));
        assertThat(bucketsRequest.getAnomalyScore(), equalTo(75.0));
    }

    public void testAnomalyCorrelationWithSimultaneousLinkAndUnlink() throws Exception {
        CrossClusterSearchStats stats = new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime));

        List<LinkedClusterState> baseline = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("departing", LinkedClusterState.Status.AVAILABLE, null, 15)
        );
        // "departing" leaves and "arriving" appears at the same time
        List<LinkedClusterState> afterSwap = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("arriving", LinkedClusterState.Status.AVAILABLE, null, 20)
        );

        currentTime = 1_000_000L;
        stats.update(baseline);

        for (int i = 0; i < 11; i++) {
            currentTime += 30_000;
            stats.update(afterSwap);
        }
        currentTime += 30_000;

        resetExtractorForCcsTest(afterSwap);

        @SuppressWarnings("unchecked")
        ActionFuture<GetBucketsAction.Response> getBucketsFuture = mock(ActionFuture.class);
        Bucket elevated = mock(Bucket.class);
        when(elevated.getTimestamp()).thenReturn(new Date(currentTime - 5_000));
        GetBucketsAction.Response bucketsResponse = new GetBucketsAction.Response(
            new QueryPage<>(List.of(elevated), 1, Bucket.RESULTS_FIELD)
        );
        when(getBucketsFuture.actionGet()).thenReturn(bucketsResponse);
        when(client.execute(same(GetBucketsAction.INSTANCE), any())).thenReturn(getBucketsFuture);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false, DELAYED_DATA_FREQ, stats);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        ArgumentCaptor<String> warningCaptor = ArgumentCaptor.forClass(String.class);
        verify(auditor, times(2)).warning(eq(jobId), warningCaptor.capture());
        List<String> warnings = warningCaptor.getAllValues();

        // Scope change warning should mention both linked and unlinked
        assertThat(warnings.get(0), containsString("arriving"));
        assertThat(warnings.get(0), containsString("linked"));
        assertThat(warnings.get(0), containsString("departing"));
        assertThat(warnings.get(0), containsString("unlinked"));

        // Anomaly correlation warning should contain the combined summary
        assertThat(warnings.get(1), containsString("Elevated anomaly scores"));
        assertThat(warnings.get(1), containsString("arriving linked"));
        assertThat(warnings.get(1), containsString("departing unlinked"));
        assertThat(warnings.get(1), containsString("1"));
    }

    public void testAnomalyLookbackErrorDoesNotPreventScopeChangeAnnotation() throws Exception {
        CrossClusterSearchStats stats = new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime));

        List<LinkedClusterState> baseline = List.of(new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10));
        List<LinkedClusterState> withNewProject = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("new_project", LinkedClusterState.Status.AVAILABLE, null, 20)
        );

        currentTime = 1_000_000L;
        stats.update(baseline);
        for (int i = 0; i < 11; i++) {
            currentTime += 30_000;
            stats.update(withNewProject);
        }
        currentTime += 30_000;

        resetExtractorForCcsTest(withNewProject);

        // GetBucketsAction throws - simulating an internal error during anomaly lookback
        @SuppressWarnings("unchecked")
        ActionFuture<GetBucketsAction.Response> getBucketsFuture = mock(ActionFuture.class);
        when(getBucketsFuture.actionGet()).thenThrow(new RuntimeException("simulated bucket lookup failure"));
        when(client.execute(same(GetBucketsAction.INSTANCE), any())).thenReturn(getBucketsFuture);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false, DELAYED_DATA_FREQ, stats);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        // Scope change annotation should still be persisted despite bucket lookup failure
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, atMost(2)).execute(eq(TransportBulkAction.TYPE), bulkCaptor.capture(), any());
        BulkRequest annotationBulk = bulkCaptor.getValue();
        assertThat(annotationBulk.requests(), hasSize(1));
        IndexRequest indexRequest = (IndexRequest) annotationBulk.requests().get(0);
        String annotationSource = indexRequest.source().utf8ToString();
        assertThat(annotationSource, containsString("search_scope_changed"));
        assertThat(annotationSource, containsString("new_project"));

        // Scope change warning emitted, but no anomaly warning (due to the error)
        verify(auditor, times(1)).warning(eq(jobId), argThat(msg -> msg.contains("linked")));
        verify(auditor, never()).warning(eq(jobId), argThat(msg -> msg.contains("Elevated anomaly")));
    }

    public void testScopeChangeAnnotationOnUnlink() throws Exception {
        CrossClusterSearchStats stats = new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime));

        List<LinkedClusterState> baseline = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("departing", LinkedClusterState.Status.AVAILABLE, null, 15)
        );
        List<LinkedClusterState> withoutDeparting = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10)
        );

        currentTime = 1_000_000L;
        stats.update(baseline);

        // Build up 11 consecutive absences spanning > 5 minutes
        for (int i = 0; i < 11; i++) {
            currentTime += 30_000;
            stats.update(withoutDeparting);
        }

        // The 12th cycle confirms the unlink
        currentTime += 30_000;
        resetExtractorForCcsTest(withoutDeparting);

        @SuppressWarnings("unchecked")
        ActionFuture<GetBucketsAction.Response> getBucketsFuture = mock(ActionFuture.class);
        GetBucketsAction.Response emptyBuckets = new GetBucketsAction.Response(new QueryPage<>(List.of(), 0, Bucket.RESULTS_FIELD));
        when(getBucketsFuture.actionGet()).thenReturn(emptyBuckets);
        when(client.execute(same(GetBucketsAction.INSTANCE), any())).thenReturn(getBucketsFuture);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false, DELAYED_DATA_FREQ, stats);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        // Verify annotation was persisted with unlinked event
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, atMost(2)).execute(eq(TransportBulkAction.TYPE), bulkCaptor.capture(), any());
        BulkRequest annotationBulk = bulkCaptor.getValue();
        assertThat(annotationBulk.requests(), hasSize(1));
        IndexRequest indexRequest = (IndexRequest) annotationBulk.requests().get(0);
        String annotationSource = indexRequest.source().utf8ToString();
        assertThat(annotationSource, containsString("search_scope_changed"));
        assertThat(annotationSource, containsString("departing"));

        // Verify the warning message references unlinking
        ArgumentCaptor<String> warningCaptor = ArgumentCaptor.forClass(String.class);
        verify(auditor, times(1)).warning(eq(jobId), warningCaptor.capture());
        assertThat(warningCaptor.getValue(), containsString("unlinked"));
        assertThat(warningCaptor.getValue(), containsString("departing"));
    }

    public void testScopeChangeAnnotationOnRelink() throws Exception {
        CrossClusterSearchStats stats = new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime));

        List<LinkedClusterState> baseline = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("flapping", LinkedClusterState.Status.AVAILABLE, null, 15)
        );
        List<LinkedClusterState> withoutFlapping = List.of(new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10));
        List<LinkedClusterState> withFlapping = List.of(
            new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10),
            new LinkedClusterState("flapping", LinkedClusterState.Status.AVAILABLE, null, 15)
        );

        // Phase 1: Establish baseline with both projects
        currentTime = 1_000_000L;
        stats.update(baseline);

        // Phase 2: "flapping" disappears and stabilizes as unlinked
        for (int i = 0; i < 12; i++) {
            currentTime += 30_000;
            CrossClusterSearchStats.ScopeChangeResult r = stats.update(withoutFlapping);
            if (i == 11) {
                assertTrue("12th cycle should confirm unlink", r.scopeChanged());
                assertThat(r.confirmedUnlinks(), equalTo(java.util.Set.of("flapping")));
            }
        }

        // Phase 3: "flapping" reappears and stabilizes as linked again
        for (int i = 0; i < 11; i++) {
            currentTime += 30_000;
            stats.update(withFlapping);
        }

        // The 12th relink cycle will confirm the scope change
        currentTime += 30_000;
        resetExtractorForCcsTest(withFlapping);

        @SuppressWarnings("unchecked")
        ActionFuture<GetBucketsAction.Response> getBucketsFuture = mock(ActionFuture.class);
        GetBucketsAction.Response emptyBuckets = new GetBucketsAction.Response(new QueryPage<>(List.of(), 0, Bucket.RESULTS_FIELD));
        when(getBucketsFuture.actionGet()).thenReturn(emptyBuckets);
        when(client.execute(same(GetBucketsAction.INSTANCE), any())).thenReturn(getBucketsFuture);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false, DELAYED_DATA_FREQ, stats);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        // Verify annotation was persisted for the relink
        ArgumentCaptor<BulkRequest> bulkCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        verify(client, atMost(2)).execute(eq(TransportBulkAction.TYPE), bulkCaptor.capture(), any());
        BulkRequest annotationBulk = bulkCaptor.getValue();
        assertThat(annotationBulk.requests(), hasSize(1));
        IndexRequest indexRequest = (IndexRequest) annotationBulk.requests().get(0);
        String annotationSource = indexRequest.source().utf8ToString();
        assertThat(annotationSource, containsString("search_scope_changed"));
        assertThat(annotationSource, containsString("flapping"));

        // Verify the warning message references linking (this is the relink event)
        ArgumentCaptor<String> warningCaptor = ArgumentCaptor.forClass(String.class);
        verify(auditor, times(1)).warning(eq(jobId), warningCaptor.capture());
        assertThat(warningCaptor.getValue(), containsString("linked"));
        assertThat(warningCaptor.getValue(), containsString("flapping"));
    }

    public void testBaselineCycleDoesNotTriggerAnnotation() throws Exception {
        CrossClusterSearchStats stats = new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime));

        List<LinkedClusterState> projects = List.of(new LinkedClusterState("origin", LinkedClusterState.Status.AVAILABLE, null, 10));

        currentTime = 1_000_000L;
        resetExtractorForCcsTest(projects);

        DatafeedJob datafeedJob = createDatafeedJob(1000, 500, -1, -1, false, DELAYED_DATA_FREQ, stats);
        assertNull(datafeedJob.runLookBack(0L, 1000L));

        // No scope change warnings on baseline
        verify(auditor, never()).warning(eq(jobId), argThat(msg -> msg.contains("scope changed")));
        verify(client, never()).execute(same(GetBucketsAction.INSTANCE), any());
    }

    @SuppressWarnings("unchecked")
    private void resetExtractorForCcsTest(List<LinkedClusterState> linkedClusterStates) throws IOException {
        dataExtractor = mock(DataExtractor.class);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        byte[] contentBytes = "content".getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(contentBytes);
        when(dataExtractor.next()).thenReturn(
            new DataExtractor.Result(new SearchInterval(1000L, 2000L), Optional.of(inputStream), linkedClusterStates)
        );
        when(dataExtractorFactory.newExtractor(anyLong(), anyLong())).thenReturn(dataExtractor);

        DataCounts dataCounts = new DataCounts(
            jobId,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            new Date(0),
            new Date(0),
            new Date(0),
            new Date(0),
            new Date(0),
            Instant.now()
        );

        PostDataAction.Request expectedRequest = new PostDataAction.Request(jobId);
        expectedRequest.setDataDescription(dataDescription.build());
        expectedRequest.setContent(new BytesArray(contentBytes), XContentType.JSON);
        when(client.execute(same(PostDataAction.INSTANCE), eq(expectedRequest))).thenReturn(postDataFuture);
        when(postDataFuture.actionGet()).thenReturn(new PostDataAction.Response(dataCounts));
    }

    private DatafeedJob createDatafeedJob(
        long frequencyMs,
        long queryDelayMs,
        long latestFinalBucketEndTimeMs,
        long latestRecordTimeMs,
        boolean haveSeenDataPreviously
    ) {
        return createDatafeedJob(
            frequencyMs,
            queryDelayMs,
            latestFinalBucketEndTimeMs,
            latestRecordTimeMs,
            haveSeenDataPreviously,
            DELAYED_DATA_CHECK_FREQ.get(Settings.EMPTY).millis(),
            new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime))
        );
    }

    private DatafeedJob createDatafeedJob(
        long frequencyMs,
        long queryDelayMs,
        long latestFinalBucketEndTimeMs,
        long latestRecordTimeMs,
        boolean haveSeenDataPreviously,
        long delayedDataFreq
    ) {
        return createDatafeedJob(
            frequencyMs,
            queryDelayMs,
            latestFinalBucketEndTimeMs,
            latestRecordTimeMs,
            haveSeenDataPreviously,
            delayedDataFreq,
            new CrossClusterSearchStats(() -> Instant.ofEpochMilli(currentTime))
        );
    }

    private DatafeedJob createDatafeedJob(
        long frequencyMs,
        long queryDelayMs,
        long latestFinalBucketEndTimeMs,
        long latestRecordTimeMs,
        boolean haveSeenDataPreviously,
        long delayedDataFreq,
        CrossClusterSearchStats crossClusterSearchStats
    ) {
        Supplier<Long> currentTimeSupplier = () -> currentTime;
        return new DatafeedJob(
            jobId,
            dataDescription.build(),
            frequencyMs,
            queryDelayMs,
            dataExtractorFactory,
            timingStatsReporter,
            client,
            auditor,
            new AnnotationPersister(resultsPersisterService),
            currentTimeSupplier,
            delayedDataDetector,
            null,
            latestFinalBucketEndTimeMs,
            latestRecordTimeMs,
            haveSeenDataPreviously,
            delayedDataFreq,
            crossClusterSearchStats
        );
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    private static BulkItemResponse bulkItemSuccess(String docId) {
        return BulkItemResponse.success(
            1,
            DocWriteRequest.OpType.INDEX,
            new IndexResponse(new ShardId(AnnotationIndex.WRITE_ALIAS_NAME, "uuid", 1), docId, 0, 0, 1, true)
        );
    }
}
