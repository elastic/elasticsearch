/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationTests;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutodetectResultProcessorTests extends ESTestCase {

    private static final String JOB_ID = "valid_id";
    private static final long BUCKET_SPAN_MS = 1000;
    private static final Instant CURRENT_TIME = Instant.ofEpochMilli(2000000000);

    private Client client;
    private AnomalyDetectionAuditor auditor;
    private Renormalizer renormalizer;
    private JobResultsPersister persister;
    private JobResultsPersister.Builder bulkResultsPersister;
    private AnnotationPersister annotationPersister;
    private AnnotationPersister.Builder bulkAnnotationsPersister;
    private AutodetectProcess process;
    private FlushListener flushListener;
    private AutodetectResultProcessor processorUnderTest;
    private ScheduledThreadPoolExecutor executor;

    @Before
    public void setUpMocks() {
        executor = new Scheduler.SafeScheduledThreadPoolExecutor(1);
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        auditor = mock(AnomalyDetectionAuditor.class);
        renormalizer = mock(Renormalizer.class);
        persister = mock(JobResultsPersister.class);
        bulkResultsPersister = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(eq(JOB_ID), any())).thenReturn(bulkResultsPersister);
        annotationPersister = mock(AnnotationPersister.class);
        bulkAnnotationsPersister = mock(AnnotationPersister.Builder.class);
        when(annotationPersister.bulkPersisterBuilder(eq(JOB_ID), any())).thenReturn(bulkAnnotationsPersister);
        process = mock(AutodetectProcess.class);
        flushListener = mock(FlushListener.class);
        processorUnderTest = new AutodetectResultProcessor(
            client,
            auditor,
            JOB_ID,
            renormalizer,
            persister,
            annotationPersister,
            process,
            new ModelSizeStats.Builder(JOB_ID).setTimestamp(new Date(BUCKET_SPAN_MS)).build(),
            new TimingStats(JOB_ID),
            Clock.fixed(CURRENT_TIME, ZoneId.systemDefault()),
            flushListener
        );
    }

    @After
    public void cleanup() {
        verify(annotationPersister).bulkPersisterBuilder(eq(JOB_ID), any());
        verifyNoMoreInteractions(auditor, renormalizer, persister, annotationPersister);
        executor.shutdown();
    }

    public void testProcess() throws Exception {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        when(process.readAutodetectResults()).thenReturn(Collections.singletonList(autodetectResult).iterator());

        processorUnderTest.process();
        processorUnderTest.awaitCompletion();
        assertThat(processorUnderTest.completionLatch.getCount(), is(equalTo(0L)));

        verify(renormalizer).waitUntilIdle();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).commitWrites(JOB_ID, EnumSet.allOf(JobResultsPersister.CommitType.class));
    }

    public void testProcessResult_bucket() {
        when(bulkResultsPersister.persistTimingStats(any(TimingStats.class))).thenReturn(bulkResultsPersister);
        when(bulkResultsPersister.persistBucket(any(Bucket.class))).thenReturn(bulkResultsPersister);
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = new Bucket(JOB_ID, new Date(), BUCKET_SPAN_MS);
        when(result.getBucket()).thenReturn(bucket);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(bulkResultsPersister).persistTimingStats(any(TimingStats.class));
        verify(bulkResultsPersister).persistBucket(bucket);
        verify(bulkResultsPersister, never()).executeRequest();
        verify(persister, never()).deleteInterimResults(JOB_ID);
    }

    public void testProcessResult_bucket_deleteInterimRequired() {
        when(bulkResultsPersister.persistTimingStats(any(TimingStats.class))).thenReturn(bulkResultsPersister);
        when(bulkResultsPersister.persistBucket(any(Bucket.class))).thenReturn(bulkResultsPersister);
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = new Bucket(JOB_ID, new Date(), BUCKET_SPAN_MS);
        when(result.getBucket()).thenReturn(bucket);

        processorUnderTest.processResult(result);
        assertEquals(1L, processorUnderTest.getCurrentRunBucketCount());
        assertFalse(processorUnderTest.isDeleteInterimRequired());

        verify(bulkResultsPersister).persistTimingStats(any(TimingStats.class));
        verify(bulkResultsPersister).persistBucket(bucket);
        verify(bulkResultsPersister).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).deleteInterimResults(JOB_ID);
    }

    public void testProcessResult_bucket_isInterim() {
        when(bulkResultsPersister.persistTimingStats(any(TimingStats.class))).thenReturn(bulkResultsPersister);
        when(bulkResultsPersister.persistBucket(any(Bucket.class))).thenReturn(bulkResultsPersister);
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = new Bucket(JOB_ID, new Date(), BUCKET_SPAN_MS);
        bucket.setInterim(true);
        when(result.getBucket()).thenReturn(bucket);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);
        assertEquals(0L, processorUnderTest.getCurrentRunBucketCount());

        verify(bulkResultsPersister, never()).persistTimingStats(any(TimingStats.class));
        verify(bulkResultsPersister).persistBucket(bucket);
        verify(bulkResultsPersister).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
    }

    public void testProcessResult_records() {
        AutodetectResult result = mock(AutodetectResult.class);
        List<AnomalyRecord> records = Arrays.asList(
            new AnomalyRecord(JOB_ID, new Date(123), 123),
            new AnomalyRecord(JOB_ID, new Date(123), 123)
        );
        when(result.getRecords()).thenReturn(records);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(bulkResultsPersister).persistRecords(records);
        verify(bulkResultsPersister, never()).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
    }

    public void testProcessResult_influencers() {
        AutodetectResult result = mock(AutodetectResult.class);
        List<Influencer> influencers = Arrays.asList(
            new Influencer(JOB_ID, "infField", "infValue", new Date(123), 123),
            new Influencer(JOB_ID, "infField2", "infValue2", new Date(123), 123)
        );
        when(result.getInfluencers()).thenReturn(influencers);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(bulkResultsPersister).persistInfluencers(influencers);
        verify(bulkResultsPersister, never()).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
    }

    public void testProcessResult_categoryDefinition() {
        AutodetectResult result = mock(AutodetectResult.class);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(categoryDefinition.getCategoryId()).thenReturn(1L);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(bulkResultsPersister).persistCategoryDefinition(eq(categoryDefinition));
        verify(bulkResultsPersister, never()).executeRequest();
    }

    public void testProcessResult_flushAcknowledgementWithRefresh() {
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(flushAcknowledgement.getShouldRefresh()).thenReturn(true);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);
        assertTrue(processorUnderTest.isDeleteInterimRequired());

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(flushListener).acknowledgeFlush(flushAcknowledgement, null);
        verify(persister).commitWrites(
            JOB_ID,
            EnumSet.of(JobResultsPersister.CommitType.RESULTS, JobResultsPersister.CommitType.ANNOTATIONS)
        );
        verify(bulkResultsPersister).executeRequest();
    }

    public void testProcessResult_flushAcknowledgement() {
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);
        assertTrue(processorUnderTest.isDeleteInterimRequired());

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(flushListener).acknowledgeFlush(flushAcknowledgement, null);
        verify(persister, never()).commitWrites(
            JOB_ID,
            EnumSet.of(JobResultsPersister.CommitType.RESULTS, JobResultsPersister.CommitType.ANNOTATIONS)
        );
        verify(bulkResultsPersister).executeRequest();
    }

    public void testProcessResult_flushAcknowledgementMustBeProcessedLast() {
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(Integer.valueOf(randomInt(100)).toString());
        when(flushAcknowledgement.getShouldRefresh()).thenReturn(true);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(categoryDefinition.getCategoryId()).thenReturn(1L);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);
        assertTrue(processorUnderTest.isDeleteInterimRequired());

        InOrder inOrder = inOrder(persister, bulkResultsPersister, flushListener);
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        inOrder.verify(bulkResultsPersister).persistCategoryDefinition(eq(categoryDefinition));
        inOrder.verify(bulkResultsPersister).executeRequest();
        verify(persister).commitWrites(
            JOB_ID,
            EnumSet.of(JobResultsPersister.CommitType.RESULTS, JobResultsPersister.CommitType.ANNOTATIONS)
        );
        inOrder.verify(flushListener).acknowledgeFlush(flushAcknowledgement, null);
    }

    public void testProcessResult_modelPlot() {
        AutodetectResult result = mock(AutodetectResult.class);
        ModelPlot modelPlot = mock(ModelPlot.class);
        when(result.getModelPlot()).thenReturn(modelPlot);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(bulkResultsPersister).persistModelPlot(modelPlot);
    }

    public void testProcessResult_annotation() {
        AutodetectResult result = mock(AutodetectResult.class);
        Annotation annotation = AnnotationTests.randomAnnotation(JOB_ID);
        when(result.getAnnotation()).thenReturn(annotation);

        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(bulkAnnotationsPersister).persistAnnotation(annotation);
        if (annotation.getEvent() == Annotation.Event.CATEGORIZATION_STATUS_CHANGE) {
            verify(auditor).warning(eq(JOB_ID), anyString());
        }
    }

    public void testProcessResult_modelSizeStats() {
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSizeStats modelSizeStats = mock(ModelSizeStats.class);
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);
        assertThat(processorUnderTest.modelSizeStats(), is(equalTo(modelSizeStats)));

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(bulkResultsPersister).persistModelSizeStats(eq(modelSizeStats));
    }

    public void testProcessResult_modelSizeStatsWithMemoryStatusChanges() {
        AutodetectResult result = mock(AutodetectResult.class);
        processorUnderTest.setDeleteInterimRequired(false);

        // First one with soft_limit
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder(JOB_ID).setMemoryStatus(ModelSizeStats.MemoryStatus.SOFT_LIMIT).build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(result);

        // Another with soft_limit
        modelSizeStats = new ModelSizeStats.Builder(JOB_ID).setMemoryStatus(ModelSizeStats.MemoryStatus.SOFT_LIMIT).build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(result);

        // Now with hard_limit
        modelSizeStats = new ModelSizeStats.Builder(JOB_ID).setMemoryStatus(ModelSizeStats.MemoryStatus.HARD_LIMIT)
            .setModelBytesMemoryLimit(ByteSizeValue.ofMb(512).getBytes())
            .setModelBytesExceeded(ByteSizeValue.ofKb(1).getBytes())
            .build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(result);

        // And another with hard_limit
        modelSizeStats = new ModelSizeStats.Builder(JOB_ID).setMemoryStatus(ModelSizeStats.MemoryStatus.HARD_LIMIT).build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(bulkResultsPersister, times(4)).persistModelSizeStats(any(ModelSizeStats.class));
        // We should have only fired two notifications: one for soft_limit and one for hard_limit
        verify(auditor).warning(JOB_ID, Messages.getMessage(Messages.JOB_AUDIT_MEMORY_STATUS_SOFT_LIMIT));
        verify(auditor).error(JOB_ID, Messages.getMessage(Messages.JOB_AUDIT_MEMORY_STATUS_HARD_LIMIT, "512mb", "1kb"));
    }

    public void testProcessResult_categorizationStatusChangeAnnotationCausesNotification() {
        AutodetectResult result = mock(AutodetectResult.class);
        processorUnderTest.setDeleteInterimRequired(false);

        Annotation annotation = new Annotation.Builder().setType(Annotation.Type.ANNOTATION)
            .setJobId(JOB_ID)
            .setAnnotation("Categorization status changed to 'warn' for partition 'foo'")
            .setEvent(Annotation.Event.CATEGORIZATION_STATUS_CHANGE)
            .setCreateTime(new Date())
            .setCreateUsername(InternalUsers.XPACK_USER.principal())
            .setTimestamp(new Date())
            .setPartitionFieldName("part")
            .setPartitionFieldValue("foo")
            .build();
        when(result.getAnnotation()).thenReturn(annotation);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(bulkAnnotationsPersister).persistAnnotation(annotation);
        verify(auditor).warning(JOB_ID, "Categorization status changed to 'warn' for partition 'foo' after 0 buckets");
    }

    public void testProcessResult_modelSnapshot() {
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(JOB_ID).setSnapshotId("a_snapshot_id")
            .setLatestResultTimeStamp(Date.from(Instant.ofEpochMilli(1000_000_000)))
            .setTimestamp(Date.from(Instant.ofEpochMilli(2000_000_000)))
            .setMinVersion(Version.CURRENT)
            .build();
        when(result.getModelSnapshot()).thenReturn(modelSnapshot);
        IndexResponse indexResponse = new IndexResponse(new ShardId("ml", "uid", 0), "1", 0L, 0L, 0L, true);

        when(persister.persistModelSnapshot(any(), any(), any())).thenReturn(
            new BulkResponse(new BulkItemResponse[] { BulkItemResponse.success(0, DocWriteRequest.OpType.INDEX, indexResponse) }, 0)
        );

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        Annotation expectedAnnotation = new Annotation.Builder().setAnnotation("Job model snapshot with id [a_snapshot_id] stored")
            .setCreateTime(Date.from(CURRENT_TIME))
            .setCreateUsername(InternalUsers.XPACK_USER.principal())
            .setTimestamp(Date.from(Instant.ofEpochMilli(1000_000_000)))
            .setEndTimestamp(Date.from(Instant.ofEpochMilli(1000_000_000)))
            .setJobId(JOB_ID)
            .setModifiedTime(Date.from(CURRENT_TIME))
            .setModifiedUsername(InternalUsers.XPACK_USER.principal())
            .setType(Annotation.Type.ANNOTATION)
            .setEvent(Annotation.Event.MODEL_SNAPSHOT_STORED)
            .build();
        UpdateJobAction.Request expectedJobUpdateRequest = UpdateJobAction.Request.internal(
            JOB_ID,
            new JobUpdate.Builder(JOB_ID).setModelSnapshotId("a_snapshot_id").build()
        );

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).persistModelSnapshot(eq(modelSnapshot), eq(WriteRequest.RefreshPolicy.IMMEDIATE), any());
        verify(bulkAnnotationsPersister).persistAnnotation(ModelSnapshot.annotationDocumentId(modelSnapshot), expectedAnnotation);
        verify(client).execute(same(UpdateJobAction.INSTANCE), eq(expectedJobUpdateRequest), any());
    }

    public void testProcessResult_quantiles_givenRenormalizationIsEnabled() {
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        when(renormalizer.isEnabled()).thenReturn(true);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).persistQuantiles(eq(quantiles), any());
        verify(renormalizer).isEnabled();
        verify(renormalizer).renormalize(eq(quantiles), any(Runnable.class));
    }

    public void testProcessResult_quantiles_givenRenormalizationIsDisabled() {
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        when(renormalizer.isEnabled()).thenReturn(false);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).persistQuantiles(eq(quantiles), any());
        verify(bulkResultsPersister, never()).executeRequest();
        verify(renormalizer).isEnabled();
    }

    public void testAwaitCompletion() throws Exception {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        when(process.readAutodetectResults()).thenReturn(Collections.singletonList(autodetectResult).iterator());

        processorUnderTest.process();
        processorUnderTest.awaitCompletion();
        assertThat(processorUnderTest.completionLatch.getCount(), is(equalTo(0L)));
        assertThat(processorUnderTest.updateModelSnapshotSemaphore.availablePermits(), is(equalTo(1)));

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).commitWrites(JOB_ID, EnumSet.allOf(JobResultsPersister.CommitType.class));
        verify(renormalizer).waitUntilIdle();
    }

    public void testPersisterThrowingDoesntBlockProcessing() {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        ModelSnapshot modelSnapshot = mock(ModelSnapshot.class);
        when(autodetectResult.getModelSnapshot()).thenReturn(modelSnapshot);

        when(process.isProcessAlive()).thenReturn(true);
        when(process.isProcessAliveAfterWaiting()).thenReturn(true);
        when(process.readAutodetectResults()).thenReturn(Arrays.asList(autodetectResult, autodetectResult).iterator());

        doThrow(new ElasticsearchException("this test throws")).when(persister).persistModelSnapshot(any(), any(), any());

        processorUnderTest.process();

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister, times(2)).persistModelSnapshot(any(), eq(WriteRequest.RefreshPolicy.IMMEDIATE), any());
    }

    public void testParsingErrorSetsFailed() throws Exception {
        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(iterator.hasNext()).thenThrow(new ElasticsearchParseException("this test throws"));
        when(process.readAutodetectResults()).thenReturn(iterator);

        assertFalse(processorUnderTest.isFailed());
        processorUnderTest.process();
        assertTrue(processorUnderTest.isFailed());

        // Wait for flush should return immediately
        FlushAcknowledgement flushAcknowledgement = processorUnderTest.waitForFlushAcknowledgement(
            JOB_ID,
            Duration.of(300, ChronoUnit.SECONDS)
        );
        assertThat(flushAcknowledgement, is(nullValue()));

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
    }

    public void testKill() throws Exception {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        when(process.readAutodetectResults()).thenReturn(Collections.singletonList(autodetectResult).iterator());

        processorUnderTest.setProcessKilled();
        processorUnderTest.process();
        processorUnderTest.awaitCompletion();
        assertThat(processorUnderTest.completionLatch.getCount(), is(equalTo(0L)));
        assertThat(processorUnderTest.updateModelSnapshotSemaphore.availablePermits(), is(equalTo(1)));

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).commitWrites(JOB_ID, EnumSet.allOf(JobResultsPersister.CommitType.class));
        verify(renormalizer, never()).renormalize(any(), any());
        verify(renormalizer).shutdown();
        verify(renormalizer).waitUntilIdle();
        verify(flushListener).clear();
    }

    public void testProcessingOpenedForecasts() {
        when(bulkResultsPersister.persistForecastRequestStats(any(ForecastRequestStats.class))).thenReturn(bulkResultsPersister);
        AutodetectResult result = mock(AutodetectResult.class);
        ForecastRequestStats forecastRequestStats = new ForecastRequestStats("foo", "forecast");
        forecastRequestStats.setStatus(ForecastRequestStats.ForecastRequestStatus.OK);
        when(result.getForecastRequestStats()).thenReturn(forecastRequestStats);

        ArgumentCaptor<ForecastRequestStats> argument = ArgumentCaptor.forClass(ForecastRequestStats.class);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        processorUnderTest.handleOpenForecasts();

        verify(bulkResultsPersister, times(2)).persistForecastRequestStats(argument.capture());
        verify(bulkResultsPersister, times(1)).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister, never()).deleteInterimResults(JOB_ID);

        // Get all values is in reverse call order
        List<ForecastRequestStats> stats = argument.getAllValues();
        assertThat(stats.get(0).getStatus(), equalTo(ForecastRequestStats.ForecastRequestStatus.OK));
        assertThat(stats.get(1).getStatus(), equalTo(ForecastRequestStats.ForecastRequestStatus.FAILED));
    }

    public void testProcessingForecasts() {
        when(bulkResultsPersister.persistForecastRequestStats(any(ForecastRequestStats.class))).thenReturn(bulkResultsPersister);
        AutodetectResult result = mock(AutodetectResult.class);
        ForecastRequestStats forecastRequestStats = new ForecastRequestStats("foo", "forecast");
        forecastRequestStats.setStatus(ForecastRequestStats.ForecastRequestStatus.OK);
        when(result.getForecastRequestStats()).thenReturn(forecastRequestStats);

        ArgumentCaptor<ForecastRequestStats> argument = ArgumentCaptor.forClass(ForecastRequestStats.class);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        result = mock(AutodetectResult.class);
        forecastRequestStats = new ForecastRequestStats("foo", "forecast");
        forecastRequestStats.setStatus(ForecastRequestStats.ForecastRequestStatus.FINISHED);
        when(result.getForecastRequestStats()).thenReturn(forecastRequestStats);

        processorUnderTest.processResult(result);
        // There shouldn't be any opened forecasts. This call should do nothing
        processorUnderTest.handleOpenForecasts();

        verify(bulkResultsPersister, times(2)).persistForecastRequestStats(argument.capture());
        verify(bulkResultsPersister, times(1)).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister, never()).deleteInterimResults(JOB_ID);

        List<ForecastRequestStats> stats = argument.getAllValues();
        assertThat(stats.get(0).getStatus(), equalTo(ForecastRequestStats.ForecastRequestStatus.OK));
        assertThat(stats.get(1).getStatus(), equalTo(ForecastRequestStats.ForecastRequestStatus.FINISHED));
    }

}
