/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.client.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.ml.job.results.PerPartitionMaxProbabilities;
import org.junit.Before;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoDetectResultProcessorTests extends ESTestCase {

    private static final String JOB_ID = "_id";

    private Client client;
    private Renormalizer renormalizer;
    private JobResultsPersister persister;
    private FlushListener flushListener;
    private AutoDetectResultProcessor processorUnderTest;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        renormalizer = mock(Renormalizer.class);
        persister = mock(JobResultsPersister.class);
        flushListener = mock(FlushListener.class);
        processorUnderTest = new AutoDetectResultProcessor(client, JOB_ID, renormalizer, persister, flushListener);
    }

    public void testProcess() {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(autodetectResult);
        AutodetectProcess process = mock(AutodetectProcess.class);
        when(process.readAutodetectResults()).thenReturn(iterator);
        processorUnderTest.process(process, randomBoolean());
        verify(renormalizer, times(1)).waitUntilIdle();
        assertEquals(0, processorUnderTest.completionLatch.getCount());
    }

    public void testProcessResult_bucket() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistBucket(bucket);
        verify(bulkBuilder, times(1)).executeRequest();
        verify(persister, times(1)).bulkPersisterBuilder(JOB_ID);
        verify(persister, never()).deleteInterimResults(JOB_ID);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_bucket_deleteInterimRequired() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = true;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistBucket(bucket);
        verify(bulkBuilder, times(1)).executeRequest();
        verify(persister, times(1)).deleteInterimResults(JOB_ID);
        verify(persister, times(1)).bulkPersisterBuilder(JOB_ID);
        verifyNoMoreInteractions(persister);
        assertFalse(context.deleteInterimRequired);
    }

    public void testProcessResult_records() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("foo", false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        AnomalyRecord record1 = new AnomalyRecord("foo", new Date(123), 123, 1);
        AnomalyRecord record2 = new AnomalyRecord("foo", new Date(123), 123, 2);
        List<AnomalyRecord> records = Arrays.asList(record1, record2);
        when(result.getRecords()).thenReturn(records);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistRecords(records);
        verify(bulkBuilder, never()).executeRequest();
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_records_isPerPartitionNormalization() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("foo", true, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        AnomalyRecord record1 = new AnomalyRecord("foo", new Date(123), 123, 1);
        record1.setPartitionFieldValue("pValue");
        AnomalyRecord record2 = new AnomalyRecord("foo", new Date(123), 123, 2);
        record2.setPartitionFieldValue("pValue");
        List<AnomalyRecord> records = Arrays.asList(record1, record2);
        when(result.getRecords()).thenReturn(records);
        processorUnderTest.processResult(context, result);

        verify(bulkBuilder, times(1)).persistPerPartitionMaxProbabilities(any(PerPartitionMaxProbabilities.class));
        verify(bulkBuilder, times(1)).persistRecords(records);
        verify(bulkBuilder, never()).executeRequest();
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_influencers() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Influencer influencer1 = new Influencer(JOB_ID, "infField", "infValue", new Date(123), 123, 1);
        Influencer influencer2 = new Influencer(JOB_ID, "infField2", "infValue2", new Date(123), 123, 1);
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

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
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

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        processorUnderTest.processResult(context, result);

        verify(flushListener, times(1)).acknowledgeFlush(JOB_ID);
        verify(persister, times(1)).commitResultWrites(JOB_ID);
        verify(bulkBuilder, times(1)).executeRequest();
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_flushAcknowledgementMustBeProcessedLast() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
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
        inOrder.verify(flushListener, times(1)).acknowledgeFlush(JOB_ID);
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_modelPlot() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelPlot modelPlot = mock(ModelPlot.class);
        when(result.getModelPlot()).thenReturn(modelPlot);
        processorUnderTest.processResult(context, result);

        verify(persister, times(1)).persistModelPlot(modelPlot);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_modelSizeStats() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSizeStats modelSizeStats = mock(ModelSizeStats.class);
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processorUnderTest.processResult(context, result);

        verify(persister, times(1)).persistModelSizeStats(modelSizeStats);
        verifyNoMoreInteractions(persister);
        assertEquals(modelSizeStats, processorUnderTest.modelSizeStats());
    }

    public void testProcessResult_modelSnapshot() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSnapshot modelSnapshot = new ModelSnapshot.Builder(JOB_ID)
                .setSnapshotId("a_snapshot_id").build();
        when(result.getModelSnapshot()).thenReturn(modelSnapshot);
        processorUnderTest.processResult(context, result);

        verify(persister, times(1)).persistModelSnapshot(modelSnapshot);
        UpdateJobAction.Request expectedJobUpdateRequest = new UpdateJobAction.Request(JOB_ID,
                new JobUpdate.Builder(JOB_ID).setModelSnapshotId("a_snapshot_id").build());

        verify(client).execute(same(UpdateJobAction.INSTANCE), eq(expectedJobUpdateRequest), any());
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_quantiles() {
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        processorUnderTest.processResult(context, result);

        verify(persister, times(1)).persistQuantiles(quantiles);
        verify(bulkBuilder).executeRequest();
        verify(persister).commitResultWrites(JOB_ID);
        verify(renormalizer, times(1)).renormalize(quantiles);
        verifyNoMoreInteractions(persister);
        verifyNoMoreInteractions(renormalizer);
    }
}
