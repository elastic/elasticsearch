/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.notifications.Auditor;
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
import java.util.function.Consumer;

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

public class AutoDetectResultProcessorTests extends ESTestCase {

    private static final String JOB_ID = "_id";
    private static final long BUCKET_SPAN_MS = 1000;

    private ThreadPool threadPool;
    private Client client;
    private Auditor auditor;
    private Renormalizer renormalizer;
    private JobResultsPersister persister;
    private JobResultsProvider jobResultsProvider;
    private FlushListener flushListener;
    private AutoDetectResultProcessor processorUnderTest;
    private ScheduledThreadPoolExecutor executor;

    @Before
    public void setUpMocks() {
        executor = new ScheduledThreadPoolExecutor(1);
        client = mock(Client.class);
        auditor = mock(Auditor.class);
        threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        renormalizer = mock(Renormalizer.class);
        persister = mock(JobResultsPersister.class);
        jobResultsProvider = mock(JobResultsProvider.class);
        flushListener = mock(FlushListener.class);
        processorUnderTest = new AutoDetectResultProcessor(client, auditor, JOB_ID, renormalizer, persister, jobResultsProvider,
                new ModelSizeStats.Builder(JOB_ID).setTimestamp(new Date(BUCKET_SPAN_MS)).build(), false, flushListener);
    }

    @After
    public void cleanup() {
        executor.shutdown();
    }

    public void testProcess() throws TimeoutException {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(autodetectResult);
        AutodetectProcess process = mock(AutodetectProcess.class);
        when(process.readAutodetectResults()).thenReturn(iterator);
        processorUnderTest.process(process);
        processorUnderTest.awaitCompletion();
        verify(renormalizer, times(1)).waitUntilIdle();
        assertEquals(0, processorUnderTest.completionLatch.getCount());
    }

    public void testProcessResult_bucket() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistBucket(bucket);
        verify(bulkBuilder, times(1)).executeRequest();
        verify(persister, never()).deleteInterimResults(JOB_ID);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_bucket_deleteInterimRequired() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = true;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistBucket(bucket);
        verify(bulkBuilder, times(1)).executeRequest();
        verify(persister, times(1)).deleteInterimResults(JOB_ID);
        verifyNoMoreInteractions(persister);
        assertFalse(context.deleteInterimRequired);
    }

    public void testProcessResult_records() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("foo", bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        AnomalyRecord record1 = new AnomalyRecord("foo", new Date(123), 123);
        AnomalyRecord record2 = new AnomalyRecord("foo", new Date(123), 123);
        List<AnomalyRecord> records = Arrays.asList(record1, record2);
        when(result.getRecords()).thenReturn(records);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistRecords(records);
        verify(bulkBuilder, never()).executeRequest();
        verifyNoMoreInteractions(persister);
    }


    public void testProcessResult_influencers() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Influencer influencer1 = new Influencer(JOB_ID, "infField", "infValue", new Date(123), 123);
        Influencer influencer2 = new Influencer(JOB_ID, "infField2", "infValue2", new Date(123), 123);
        List<Influencer> influencers = Arrays.asList(influencer1, influencer2);
        when(result.getInfluencers()).thenReturn(influencers);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistInfluencers(influencers);
        verify(bulkBuilder, never()).executeRequest();
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_categoryDefinition() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, never()).executeRequest();
        verify(persister, times(1)).persistCategoryDefinition(categoryDefinition);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_flushAcknowledgement() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        processorUnderTest.processResult(context, result);

        verify(flushListener, times(1)).acknowledgeFlush(flushAcknowledgement);
        verify(persister, times(1)).commitResultWrites(JOB_ID);
        verify(bulkBuilder, times(1)).executeRequest();
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_flushAcknowledgementMustBeProcessedLast() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);

        InOrder inOrder = inOrder(persister, bulkBuilder, flushListener);
        processorUnderTest.processResult(context, result);

        inOrder.verify(persister, times(1)).persistCategoryDefinition(categoryDefinition);
        inOrder.verify(bulkBuilder, times(1)).executeRequest();
        inOrder.verify(persister, times(1)).commitResultWrites(JOB_ID);
        inOrder.verify(flushListener, times(1)).acknowledgeFlush(flushAcknowledgement);
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_modelPlot() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelPlot modelPlot = mock(ModelPlot.class);
        when(result.getModelPlot()).thenReturn(modelPlot);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistModelPlot(modelPlot);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_modelSizeStats() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSizeStats modelSizeStats = mock(ModelSizeStats.class);
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(context, result);

        verify(persister, times(1)).persistModelSizeStats(modelSizeStats);
        verifyNoMoreInteractions(persister);
        // No interactions with the jobResultsProvider confirms that the established memory calculation did not run
        verifyNoMoreInteractions(jobResultsProvider, auditor);
        assertEquals(modelSizeStats, processorUnderTest.modelSizeStats());
    }

    public void testProcessResult_modelSizeStatsWithMemoryStatusChanges() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        setupScheduleDelayTime(TimeValue.timeValueSeconds(5));

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);

        // First one with soft_limit
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder(JOB_ID).setMemoryStatus(ModelSizeStats.MemoryStatus.SOFT_LIMIT).build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(context, result);

        // Another with soft_limit
        modelSizeStats = new ModelSizeStats.Builder(JOB_ID).setMemoryStatus(ModelSizeStats.MemoryStatus.SOFT_LIMIT).build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(context, result);

        // Now with hard_limit
        modelSizeStats = new ModelSizeStats.Builder(JOB_ID)
                .setMemoryStatus(ModelSizeStats.MemoryStatus.HARD_LIMIT)
                .setModelBytes(new ByteSizeValue(512, ByteSizeUnit.MB).getBytes())
                .build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(context, result);

        // And another with hard_limit
        modelSizeStats = new ModelSizeStats.Builder(JOB_ID).setMemoryStatus(ModelSizeStats.MemoryStatus.HARD_LIMIT).build();
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(context, result);

        // We should have only fired to notifications: one for soft_limit and one for hard_limit
        verify(auditor).warning(JOB_ID, Messages.getMessage(Messages.JOB_AUDIT_MEMORY_STATUS_SOFT_LIMIT));
        verify(auditor).error(JOB_ID, Messages.getMessage(Messages.JOB_AUDIT_MEMORY_STATUS_HARD_LIMIT, "512mb"));
        verifyNoMoreInteractions(auditor);
    }

    public void testProcessResult_modelSizeStatsAfterManyBuckets() throws Exception {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);

        // To avoid slowing down the test this is using a delay of 1 nanosecond rather than the 5 seconds used in production
        setupScheduleDelayTime(TimeValue.timeValueNanos(1));

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        for (int i = 0; i < JobResultsProvider.BUCKETS_FOR_ESTABLISHED_MEMORY_SIZE; ++i) {
            AutodetectResult result = mock(AutodetectResult.class);
            Bucket bucket = mock(Bucket.class);
            when(result.getBucket()).thenReturn(bucket);
            processorUnderTest.processResult(context, result);
        }

        AutodetectResult result = mock(AutodetectResult.class);
        ModelSizeStats modelSizeStats = mock(ModelSizeStats.class);
        Date timestamp = new Date(BUCKET_SPAN_MS);
        when(modelSizeStats.getTimestamp()).thenReturn(timestamp);
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(context, result);

        // Some calls will be made 1 nanosecond later in a different thread, hence the assertBusy()
        assertBusy(() -> {
            verify(persister, times(1)).persistModelSizeStats(modelSizeStats);
            verify(persister, times(1)).commitResultWrites(JOB_ID);
            verifyNoMoreInteractions(persister);
            verify(jobResultsProvider, times(1)).getEstablishedMemoryUsage(eq(JOB_ID), eq(timestamp),
                    eq(modelSizeStats), any(Consumer.class), any(Consumer.class));
            verifyNoMoreInteractions(jobResultsProvider);
            assertEquals(modelSizeStats, processorUnderTest.modelSizeStats());
        });
    }

    public void testProcessResult_manyModelSizeStatsInQuickSuccession() throws Exception {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);

        setupScheduleDelayTime(TimeValue.timeValueSeconds(1));

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        ModelSizeStats modelSizeStats = null;
        for (int i = 1; i <= JobResultsProvider.BUCKETS_FOR_ESTABLISHED_MEMORY_SIZE + 5; ++i) {
            AutodetectResult result = mock(AutodetectResult.class);
            Bucket bucket = mock(Bucket.class);
            when(bucket.getTimestamp()).thenReturn(new Date(BUCKET_SPAN_MS * i));
            when(result.getBucket()).thenReturn(bucket);
            processorUnderTest.processResult(context, result);
            if (i > JobResultsProvider.BUCKETS_FOR_ESTABLISHED_MEMORY_SIZE) {
                result = mock(AutodetectResult.class);
                modelSizeStats = mock(ModelSizeStats.class);
                when(modelSizeStats.getTimestamp()).thenReturn(new Date(BUCKET_SPAN_MS * i));
                when(result.getModelSizeStats()).thenReturn(modelSizeStats);
                processorUnderTest.processResult(context, result);
            }
        }

        ModelSizeStats lastModelSizeStats = modelSizeStats;
        assertNotNull(lastModelSizeStats);
        Date lastTimestamp = lastModelSizeStats.getTimestamp();

        // Some calls will be made 1 second later in a different thread, hence the assertBusy()
        assertBusy(() -> {
            // All the model size stats should be persisted to the index...
            verify(persister, times(5)).persistModelSizeStats(any(ModelSizeStats.class));
            // ...but only the last should trigger an established model memory update
            verify(persister, times(1)).commitResultWrites(JOB_ID);
            verifyNoMoreInteractions(persister);
            verify(jobResultsProvider, times(1)).getEstablishedMemoryUsage(eq(JOB_ID), eq(lastTimestamp), eq(lastModelSizeStats),
                any(Consumer.class), any(Consumer.class));
            verifyNoMoreInteractions(jobResultsProvider);
            assertEquals(lastModelSizeStats, processorUnderTest.modelSizeStats());
        });
    }

    public void testProcessResult_modelSnapshot() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(JOB_ID)
                .setSnapshotId("a_snapshot_id")
                .setMinVersion(Version.CURRENT)
                .build();
        when(result.getModelSnapshot()).thenReturn(modelSnapshot);
        processorUnderTest.processResult(context, result);

        verify(persister, times(1)).persistModelSnapshot(modelSnapshot, WriteRequest.RefreshPolicy.IMMEDIATE);
        UpdateJobAction.Request expectedJobUpdateRequest = UpdateJobAction.Request.internal(JOB_ID,
                new JobUpdate.Builder(JOB_ID).setModelSnapshotId("a_snapshot_id").setModelSnapshotMinVersion(Version.CURRENT).build());

        verify(client).execute(same(UpdateJobAction.INSTANCE), eq(expectedJobUpdateRequest), any());
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_quantiles_givenRenormalizationIsEnabled() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        when(renormalizer.isEnabled()).thenReturn(true);
        processorUnderTest.processResult(context, result);

        verify(persister, times(1)).persistQuantiles(quantiles);
        verify(bulkBuilder).executeRequest();
        verify(persister).commitResultWrites(JOB_ID);
        verify(renormalizer, times(1)).isEnabled();
        verify(renormalizer, times(1)).renormalize(quantiles);
        verifyNoMoreInteractions(persister);
        verifyNoMoreInteractions(renormalizer);
    }

    public void testProcessResult_quantiles_givenRenormalizationIsDisabled() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        when(renormalizer.isEnabled()).thenReturn(false);
        processorUnderTest.processResult(context, result);

        verify(persister, times(1)).persistQuantiles(quantiles);
        verify(bulkBuilder).executeRequest();
        verify(renormalizer, times(1)).isEnabled();
        verifyNoMoreInteractions(persister);
        verifyNoMoreInteractions(renormalizer);
    }

    public void testAwaitCompletion() throws TimeoutException {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(autodetectResult);
        AutodetectProcess process = mock(AutodetectProcess.class);
        when(process.readAutodetectResults()).thenReturn(iterator);
        processorUnderTest.process(process);

        processorUnderTest.awaitCompletion();
        assertEquals(0, processorUnderTest.completionLatch.getCount());
        assertEquals(1, processorUnderTest.updateModelSnapshotSemaphore.availablePermits());
    }

    public void testPersisterThrowingDoesntBlockProcessing() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        ModelSnapshot modelSnapshot = mock(ModelSnapshot.class);
        when(autodetectResult.getModelSnapshot()).thenReturn(modelSnapshot);

        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(autodetectResult);
        AutodetectProcess process = mock(AutodetectProcess.class);
        when(process.isProcessAlive()).thenReturn(true);
        when(process.isProcessAliveAfterWaiting()).thenReturn(true);
        when(process.readAutodetectResults()).thenReturn(iterator);

        doThrow(new ElasticsearchException("this test throws")).when(persister).persistModelSnapshot(any(), any());

        processorUnderTest.process(process);
        verify(persister, times(2)).persistModelSnapshot(any(), eq(WriteRequest.RefreshPolicy.IMMEDIATE));
    }

    public void testParsingErrorSetsFailed() throws InterruptedException {
        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(iterator.hasNext()).thenThrow(new ElasticsearchParseException("this test throws"));
        AutodetectProcess process = mock(AutodetectProcess.class);
        when(process.readAutodetectResults()).thenReturn(iterator);

        assertFalse(processorUnderTest.isFailed());
        processorUnderTest.process(process);
        assertTrue(processorUnderTest.isFailed());

        // Wait for flush should return immediately
        FlushAcknowledgement flushAcknowledgement = processorUnderTest.waitForFlushAcknowledgement(
                "foo", Duration.of(300, ChronoUnit.SECONDS));
        assertThat(flushAcknowledgement, is(nullValue()));
    }

    public void testKill() throws TimeoutException {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(autodetectResult);
        AutodetectProcess process = mock(AutodetectProcess.class);
        when(process.readAutodetectResults()).thenReturn(iterator);
        processorUnderTest.setProcessKilled();
        processorUnderTest.process(process);

        processorUnderTest.awaitCompletion();
        assertEquals(0, processorUnderTest.completionLatch.getCount());
        assertEquals(1, processorUnderTest.updateModelSnapshotSemaphore.availablePermits());

        verify(persister, times(1)).commitResultWrites(JOB_ID);
        verify(persister, times(1)).commitStateWrites(JOB_ID);
        verify(renormalizer, never()).renormalize(any());
        verify(renormalizer).shutdown();
        verify(renormalizer, times(1)).waitUntilIdle();
        verify(flushListener, times(1)).clear();
    }

    private void setupScheduleDelayTime(TimeValue delay) {
        when(threadPool.schedule(any(TimeValue.class), anyString(), any(Runnable.class)))
            .thenAnswer(i -> executor.schedule((Runnable) i.getArguments()[2], delay.nanos(), TimeUnit.NANOSECONDS));
    }
}
