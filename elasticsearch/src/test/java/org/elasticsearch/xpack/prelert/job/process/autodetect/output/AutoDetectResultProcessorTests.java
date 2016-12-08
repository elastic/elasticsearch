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
import org.elasticsearch.xpack.prelert.job.results.PerPartitionMaxProbabilities;
import org.mockito.InOrder;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoDetectResultProcessorTests extends ESTestCase {

    private static final String JOB_ID = "_id";

    public void testProcess() {
        AutodetectResult autodetectResult = mock(AutodetectResult.class);
        @SuppressWarnings("unchecked")
        Stream<AutodetectResult> stream = mock(Stream.class);
        @SuppressWarnings("unchecked")
        Iterator<AutodetectResult> iterator = mock(Iterator.class);
        when(stream.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(autodetectResult);
        AutodetectResultsParser parser = mock(AutodetectResultsParser.class);
        when(parser.parseResults(any())).thenReturn(stream);

        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, parser);
        processor.process(JOB_ID, mock(InputStream.class), randomBoolean());
        verify(renormaliser, times(1)).shutdown();
        assertEquals(0, processor.completionLatch.getCount());
    }

    public void testProcessResult_bucket() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processor.processResult(context, result);

        verify(bulkBuilder, times(1)).persistBucket(bucket);
        verify(bulkBuilder, times(1)).executeRequest();
        verify(persister, times(1)).bulkPersisterBuilder(JOB_ID);
        verify(persister, never()).deleteInterimResults(JOB_ID);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_bucket_deleteInterimRequired() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        when(bulkBuilder.persistBucket(any(Bucket.class))).thenReturn(bulkBuilder);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processor.processResult(context, result);

        verify(bulkBuilder, times(1)).persistBucket(bucket);
        verify(bulkBuilder, times(1)).executeRequest();
        verify(persister, times(1)).deleteInterimResults(JOB_ID);
        verify(persister, times(1)).bulkPersisterBuilder(JOB_ID);
        verifyNoMoreInteractions(persister);
        assertFalse(context.deleteInterimRequired);
    }

    public void testProcessResult_records() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("foo", false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        AnomalyRecord record1 = new AnomalyRecord("foo", new Date(123), 123, 1);
        AnomalyRecord record2 = new AnomalyRecord("foo", new Date(123), 123, 2);
        List<AnomalyRecord> records = Arrays.asList(record1, record2);
        when(result.getRecords()).thenReturn(records);
        processor.processResult(context, result);

        verify(bulkBuilder, times(1)).persistRecords(records);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_records_isPerPartitionNormalization() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context("foo", true, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        AnomalyRecord record1 = new AnomalyRecord("foo", new Date(123), 123, 1);
        record1.setPartitionFieldValue("pValue");
        AnomalyRecord record2 = new AnomalyRecord("foo", new Date(123), 123, 2);
        record2.setPartitionFieldValue("pValue");
        List<AnomalyRecord> records = Arrays.asList(record1, record2);
        when(result.getRecords()).thenReturn(records);
        processor.processResult(context, result);

        verify(bulkBuilder, times(1)).persistPerPartitionMaxProbabilities(any(PerPartitionMaxProbabilities.class));
        verify(bulkBuilder, times(1)).persistRecords(records);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_influencers() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        when(persister.bulkPersisterBuilder(JOB_ID)).thenReturn(bulkBuilder);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Influencer influencer1 = new Influencer(JOB_ID, "infField", "infValue", new Date(123), 123, 1);
        Influencer influencer2 = new Influencer(JOB_ID, "infField2", "infValue2", new Date(123), 123, 1);
        List<Influencer> influencers = Arrays.asList(influencer1, influencer2);
        when(result.getInfluencers()).thenReturn(influencers);
        processor.processResult(context, result);

        verify(bulkBuilder, times(1)).persistInfluencers(influencers);
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_categoryDefinition() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
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
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        FlushListener flushListener = mock(FlushListener.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null, flushListener);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        processor.processResult(context, result);

        verify(flushListener, times(1)).acknowledgeFlush(JOB_ID);
        verify(persister, times(1)).commitWrites(JOB_ID);
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_flushAcknowledgementMustBeProcessedLast() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        FlushListener flushListener = mock(FlushListener.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null, flushListener);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn(JOB_ID);
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        CategoryDefinition categoryDefinition = mock(CategoryDefinition.class);
        when(result.getCategoryDefinition()).thenReturn(categoryDefinition);

        InOrder inOrder = inOrder(persister, flushListener);
        processor.processResult(context, result);

        inOrder.verify(persister, times(1)).persistCategoryDefinition(categoryDefinition);
        inOrder.verify(persister, times(1)).commitWrites(JOB_ID);
        inOrder.verify(flushListener, times(1)).acknowledgeFlush(JOB_ID);
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_modelDebugOutput() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
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
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
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
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
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
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, false, bulkBuilder);
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
        JobResultsPersister.Builder bulkBuilder = mock(JobResultsPersister.Builder.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(JOB_ID, true, bulkBuilder);
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
