/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.output;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.process.normalizer.Renormaliser;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.AutodetectResult;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.CategoryDefinition;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.ModelDebugOutput;
import org.elasticsearch.xpack.prelert.utils.CloseableIterator;
import org.mockito.InOrder;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoDetectResultProcessorTests extends ESTestCase {

    public void testProcess() {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        @SuppressWarnings("unchecked")
        CloseableIterator<AutodetectResult> iterator = mock(CloseableIterator.class);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(autodetectResult);
        AutodetectResultsParser parser = mock(AutodetectResultsParser.class);
        when(parser.parseResults(any())).thenReturn(iterator);

        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, parser);
        processor.process("_id", mock(InputStream.class), randomBoolean());
        verify(renormaliser, times(1)).shutdown();
        assertEquals(0, processor.completionLatch.getCount());
    }

    public void testProcessResult_bucket() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processor.processResult(context, result);

        verify(persister, times(1)).persistBucket(bucket);
        verify(persister, never()).deleteInterimResults("_id");
        verify(bucket, never()).calcMaxNormalizedProbabilityPerPartition();
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_bucket_isPerPartitionNormalization() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", true);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processor.processResult(context, result);

        verify(persister, times(1)).persistBucket(bucket);
        verify(persister, never()).deleteInterimResults("_id");
        verify(bucket, times(1)).calcMaxNormalizedProbabilityPerPartition();
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_bucket_deleteInterimRequired() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processor.processResult(context, result);

        verify(persister, times(1)).persistBucket(bucket);
        verify(persister, times(1)).deleteInterimResults("_id");
        verify(bucket, never()).calcMaxNormalizedProbabilityPerPartition();
        verifyNoMoreInteractions(persister);
        assertFalse(context.deleteInterimRequired);
    }

    public void testProcessResult_records() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("foo", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        AnomalyRecord record1 = new AnomalyRecord("foo");
        AnomalyRecord record2 = new AnomalyRecord("foo");
        List<AnomalyRecord> records = Arrays.asList(record1, record2);
        when(result.getRecords()).thenReturn(records);
        processor.processResult(context, result);

        verify(persister, times(1)).persistRecords(records);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_influencers() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("foo", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Influencer influencer1 = new Influencer("foo", "infField", "infValue");
        Influencer influencer2 = new Influencer("foo", "infField2", "infValue2");
        List<Influencer> influencers = Arrays.asList(influencer1, influencer2);
        when(result.getInfluencers()).thenReturn(influencers);
        processor.processResult(context, result);

        verify(persister, times(1)).persistInfluencers(influencers);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_categoryDefinition() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);
        processor.processResult(context, result);

        verify(persister, times(1)).persistCategoryDefinition(categoryDefinition);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_flushAcknowledgement() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        FlushListener flushListener = mock(FlushListener.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null, flushListener);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn("_id");
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        processor.processResult(context, result);

        verify(flushListener, times(1)).acknowledgeFlush("_id");
        verify(persister, times(1)).commitWrites("_id");
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_flushAcknowledgementMustBeProcessedLast() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        FlushListener flushListener = mock(FlushListener.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null, flushListener);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn("_id");
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);

        InOrder inOrder = inOrder(persister, flushListener);
        processor.processResult(context, result);

        inOrder.verify(persister, times(1)).persistCategoryDefinition(categoryDefinition);
        inOrder.verify(persister, times(1)).commitWrites("_id");
        inOrder.verify(flushListener, times(1)).acknowledgeFlush("_id");
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_modelDebugOutput() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelDebugOutput modelDebugOutput = mock(ModelDebugOutput.class);
        when(result.getModelDebugOutput()).thenReturn(modelDebugOutput);
        processor.processResult(context, result);

        verify(persister, times(1)).persistModelDebugOutput(modelDebugOutput);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_modelSizeStats() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSizeStats modelSizeStats = mock(ModelSizeStats.class);
        when(result.getModelSizeStats()).thenReturn(modelSizeStats);
        processor.processResult(context, result);

        verify(persister, times(1)).persistModelSizeStats(modelSizeStats);
        verifyNoMoreInteractions(persister);
        assertEquals(modelSizeStats, processor.modelSizeStats().get());
    }

    public void testProcessResult_modelSnapshot() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        ModelSnapshot modelSnapshot = mock(ModelSnapshot.class);
        when(result.getModelSnapshot()).thenReturn(modelSnapshot);
        processor.processResult(context, result);

        verify(persister, times(1)).persistModelSnapshot(modelSnapshot);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_quantiles() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        processor.processResult(context, result);

        verify(persister, times(1)).persistQuantiles(quantiles);
        verify(renormaliser, times(1)).renormalise(quantiles);
        verifyNoMoreInteractions(persister);
        verifyNoMoreInteractions(renormaliser);
    }

    public void testProcessResult_quantiles_isPerPartitionNormalization() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("_id", true);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        processor.processResult(context, result);

        verify(persister, times(1)).persistQuantiles(quantiles);
        verify(renormaliser, times(1)).renormaliseWithPartition(quantiles);
        verifyNoMoreInteractions(persister);
        verifyNoMoreInteractions(renormaliser);
    }

}