/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
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
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.After;
import org.junit.Before;
import org.mockito.InOrder;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
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

    private ThreadPool threadPool;
    private Client client;
    private AnomalyDetectionAuditor auditor;
    private Renormalizer renormalizer;
    private JobResultsPersister persister;
    private JobResultsPersister.Builder bulkBuilder;
    private AutodetectProcess process;
    private FlushListener flushListener;
    private AutodetectResultProcessor processorUnderTest;
    private ScheduledThreadPoolExecutor executor;

    @Before
    public void setUpMocks() {
        executor = new Scheduler.SafeScheduledThreadPoolExecutor(1);
        client = mock(Client.class);
        threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        auditor = mock(AnomalyDetectionAuditor.class);
        renormalizer = mock(Renormalizer.class);
        persister = mock(JobResultsPersister.class);
        bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(eq(JOB_ID), any())).thenReturn(bulkBuilder);
        process = mock(AutodetectProcess.class);
        flushListener = mock(FlushListener.class);
        processorUnderTest = new AutodetectResultProcessor(
            client,
            auditor,
            JOB_ID,
            renormalizer,
            persister,
            process,
            new ModelSizeStats.Builder(JOB_ID).setTimestamp(new Date(BUCKET_SPAN_MS)).build(),
            new TimingStats(JOB_ID),
            flushListener);
    }

    @After
    public void cleanup() {
        verifyNoMoreInteractions(auditor, renormalizer, persister);
        executor.shutdown();
    }

    public void testProcess() throws TimeoutException {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        when(process.readAutodetectResults()).thenReturn(Arrays.asList(autodetectResult).iterator());

        processorUnderTest.process();
        processorUnderTest.awaitCompletion();
        assertThat(processorUnderTest.completionLatch.getCount(), is(equalTo(0L)));

        verify(renormalizer).waitUntilIdle();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).commitResultWrites(JOB_ID);
        verify(persister).commitStateWrites(JOB_ID);
    }

    public void testProcessResult_bucket() throws Exception {
        when(bulkBuilder.persistTimingStats(any(TimingStats.class))).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = new Bucket(JOB_ID, new Date(), BUCKET_SPAN_MS);
        when(result.getBucket()).thenReturn(bucket);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(bulkBuilder).persistTimingStats(any(TimingStats.class));
        verify(bulkBuilder).persistBucket(bucket);
        verify(bulkBuilder).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister, never()).deleteInterimResults(JOB_ID);
    }

    public void testProcessResult_bucket_deleteInterimRequired() throws Exception {
        when(bulkBuilder.persistTimingStats(any(TimingStats.class))).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = new Bucket(JOB_ID, new Date(), BUCKET_SPAN_MS);
        when(result.getBucket()).thenReturn(bucket);

        processorUnderTest.processResult(result);
        assertFalse(processorUnderTest.isDeleteInterimRequired());

        verify(bulkBuilder).persistTimingStats(any(TimingStats.class));
        verify(bulkBuilder).persistBucket(bucket);
        verify(bulkBuilder).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).deleteInterimResults(JOB_ID);
    }

    public void testProcessResult_records() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        List<AnomalyRecord> records =
            Arrays.asList(
                new AnomalyRecord(JOB_ID, new Date(123), 123),
                new AnomalyRecord(JOB_ID, new Date(123), 123));
        when(result.getRecords()).thenReturn(records);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(bulkBuilder).persistRecords(records);
        verify(bulkBuilder, never()).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
    }

    public void testProcessResult_influencers() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        List<Influencer> influencers =
            Arrays.asList(
                new Influencer(JOB_ID, "infField", "infValue", new Date(123), 123),
                new Influencer(JOB_ID, "infField2", "infValue2", new Date(123), 123));
        when(result.getInfluencers()).thenReturn(influencers);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(bulkBuilder).persistInfluencers(influencers);
        verify(bulkBuilder, never()).executeRequest();
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
    }

    public void testProcessResult_categoryDefinition() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(bulkBuilder, never()).executeRequest();
        verify(persister).persistCategoryDefinition(eq(categoryDefinition), any());
        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
    }

    public void testProcessResult_flushAcknowledgement() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);
        assertTrue(processorUnderTest.isDeleteInterimRequired());

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(flushListener).acknowledgeFlush(flushAcknowledgement, null);
        verify(persister).commitResultWrites(JOB_ID);
        verify(bulkBuilder).executeRequest();
    }

    public void testProcessResult_flushAcknowledgementMustBeProcessedLast() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);
        assertTrue(processorUnderTest.isDeleteInterimRequired());

        InOrder inOrder = inOrder(persister, bulkBuilder, flushListener);
        inOrder.verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        inOrder.verify(persister).persistCategoryDefinition(eq(categoryDefinition), any());
        inOrder.verify(bulkBuilder).executeRequest();
        inOrder.verify(persister).commitResultWrites(JOB_ID);
        inOrder.verify(flushListener).acknowledgeFlush(flushAcknowledgement, null);
    }

    public void testProcessResult_modelPlot() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        ModelPlot modelPlot = mock(ModelPlot.class);
        when(result.getModelPlot()).thenReturn(modelPlot);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(bulkBuilder).persistModelPlot(modelPlot);
    }

    public void testProcessResult_modelSizeStats() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSizeStats modelSizeStats = mock(ModelSizeStats.class);
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);
        assertThat(processorUnderTest.modelSizeStats(), is(equalTo(modelSizeStats)));

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).persistModelSizeStats(eq(modelSizeStats), any());
    }

    public void testProcessResult_modelSizeStatsWithMemoryStatusChanges() throws Exception {
        TimeValue delay = TimeValue.timeValueSeconds(5);
        // Set up schedule delay time
        when(threadPool.schedule(any(Runnable.class), any(TimeValue.class), anyString()))
            .thenAnswer(i -> executor.schedule((Runnable) i.getArguments()[0], delay.nanos(), TimeUnit.NANOSECONDS));

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
        modelSizeStats = new ModelSizeStats.Builder(JOB_ID)
                .setMemoryStatus(ModelSizeStats.MemoryStatus.HARD_LIMIT)
                .setModelBytesMemoryLimit(new ByteSizeValue(512, ByteSizeUnit.MB).getBytes())
                .setModelBytesExceeded(new ByteSizeValue(1, ByteSizeUnit.KB).getBytes())
                .build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(result);

        // And another with hard_limit
        modelSizeStats = new ModelSizeStats.Builder(JOB_ID).setMemoryStatus(ModelSizeStats.MemoryStatus.HARD_LIMIT).build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister, times(4)).persistModelSizeStats(any(ModelSizeStats.class), any());
        // We should have only fired two notifications: one for soft_limit and one for hard_limit
        verify(auditor).warning(JOB_ID, Messages.getMessage(Messages.JOB_AUDIT_MEMORY_STATUS_SOFT_LIMIT));
        verify(auditor).error(JOB_ID, Messages.getMessage(Messages.JOB_AUDIT_MEMORY_STATUS_HARD_LIMIT, "512mb", "1kb"));
    }

    public void testProcessResult_modelSnapshot() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(JOB_ID)
            .setSnapshotId("a_snapshot_id")
            .setMinVersion(Version.CURRENT)
            .build();
        when(result.getModelSnapshot()).thenReturn(modelSnapshot);
        IndexResponse indexResponse = new IndexResponse(new ShardId("ml", "uid", 0), "1", 0L, 0L, 0L, true);

        when(persister.persistModelSnapshot(any(), any(), any()))
            .thenReturn(new BulkResponse(new BulkItemResponse[]{new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, indexResponse)}, 0));

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).persistModelSnapshot(eq(modelSnapshot), eq(WriteRequest.RefreshPolicy.IMMEDIATE), any());

        UpdateJobAction.Request expectedJobUpdateRequest = UpdateJobAction.Request.internal(JOB_ID,
                new JobUpdate.Builder(JOB_ID).setModelSnapshotId("a_snapshot_id").build());

        verify(client).execute(same(UpdateJobAction.INSTANCE), eq(expectedJobUpdateRequest), any());
    }

    public void testProcessResult_quantiles_givenRenormalizationIsEnabled() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        when(renormalizer.isEnabled()).thenReturn(true);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).persistQuantiles(eq(quantiles), any());
        verify(bulkBuilder).executeRequest();
        verify(persister).commitResultWrites(JOB_ID);
        verify(renormalizer).isEnabled();
        verify(renormalizer).renormalize(quantiles);
    }

    public void testProcessResult_quantiles_givenRenormalizationIsDisabled() throws Exception {
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        when(renormalizer.isEnabled()).thenReturn(false);

        processorUnderTest.setDeleteInterimRequired(false);
        processorUnderTest.processResult(result);

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).persistQuantiles(eq(quantiles), any());
        verify(bulkBuilder).executeRequest();
        verify(renormalizer).isEnabled();
    }

    public void testAwaitCompletion() throws TimeoutException {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        when(process.readAutodetectResults()).thenReturn(Arrays.asList(autodetectResult).iterator());

        processorUnderTest.process();
        processorUnderTest.awaitCompletion();
        assertThat(processorUnderTest.completionLatch.getCount(), is(equalTo(0L)));
        assertThat(processorUnderTest.updateModelSnapshotSemaphore.availablePermits(), is(equalTo(1)));

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).commitResultWrites(JOB_ID);
        verify(persister).commitStateWrites(JOB_ID);
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
        FlushAcknowledgement flushAcknowledgement =
            processorUnderTest.waitForFlushAcknowledgement(JOB_ID, Duration.of(300, ChronoUnit.SECONDS));
        assertThat(flushAcknowledgement, is(nullValue()));

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
    }

    public void testKill() throws TimeoutException {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        when(process.readAutodetectResults()).thenReturn(Arrays.asList(autodetectResult).iterator());

        processorUnderTest.setProcessKilled();
        processorUnderTest.process();
        processorUnderTest.awaitCompletion();
        assertThat(processorUnderTest.completionLatch.getCount(), is(equalTo(0L)));
        assertThat(processorUnderTest.updateModelSnapshotSemaphore.availablePermits(), is(equalTo(1)));

        verify(persister).bulkPersisterBuilder(eq(JOB_ID), any());
        verify(persister).commitResultWrites(JOB_ID);
        verify(persister).commitStateWrites(JOB_ID);
        verify(renormalizer, never()).renormalize(any());
        verify(renormalizer).shutdown();
        verify(renormalizer).waitUntilIdle();
        verify(flushListener).clear();
    }

}
