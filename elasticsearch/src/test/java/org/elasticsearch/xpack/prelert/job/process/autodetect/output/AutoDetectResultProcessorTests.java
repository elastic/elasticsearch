/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.output;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.prelert.job.process.normalizer.Renormaliser;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.job.results.AutodetectResult;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.CategoryDefinition;
import org.elasticsearch.xpack.prelert.job.results.ModelDebugOutput;
import org.elasticsearch.xpack.prelert.utils.CloseableIterator;
import org.mockito.InOrder;

import java.io.InputStream;

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
        Logger jobLogger = mock(Logger.class);
        processor.process(jobLogger, mock(InputStream.class), randomBoolean());
        verify(jobLogger, times(1)).info("1 buckets parsed from autodetect output - about to refresh indexes");
        verify(renormaliser, times(1)).shutdown(jobLogger);
    }

    public void testProcessResult_bucket() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processor.processResult(context, result);

        verify(persister, times(1)).persistBucket(bucket);
        verify(persister, never()).deleteInterimResults();
        verify(bucket, never()).calcMaxNormalizedProbabilityPerPartition();
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_bucket_isPerPartitionNormalization() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, true);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processor.processResult(context, result);

        verify(persister, times(1)).persistBucket(bucket);
        verify(persister, never()).deleteInterimResults();
        verify(bucket, times(1)).calcMaxNormalizedProbabilityPerPartition();
        verifyNoMoreInteractions(persister);
    }

    public void testProcessResult_bucket_deleteInterimRequired() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
        AutodetectResult result = mock(AutodetectResult.class);
        Bucket bucket = mock(Bucket.class);
        when(result.getBucket()).thenReturn(bucket);
        processor.processResult(context, result);

        verify(persister, times(1)).persistBucket(bucket);
        verify(persister, times(1)).deleteInterimResults();
        verify(bucket, never()).calcMaxNormalizedProbabilityPerPartition();
        verifyNoMoreInteractions(persister);
        assertFalse(context.deleteInterimRequired);
    }

    public void testProcessResult_categoryDefinition() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
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

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(flushAcknowledgement.getId()).thenReturn("_id");
        when(result.getFlushAcknowledgement()).thenReturn(flushAcknowledgement);
        processor.processResult(context, result);

        verify(flushListener, times(1)).acknowledgeFlush("_id");
        verify(persister, times(1)).commitWrites();
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_flushAcknowledgementMustBeProcessedLast() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        FlushListener flushListener = mock(FlushListener.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null, flushListener);

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
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
        inOrder.verify(persister, times(1)).commitWrites();
        inOrder.verify(flushListener, times(1)).acknowledgeFlush("_id");
        verifyNoMoreInteractions(persister);
        assertTrue(context.deleteInterimRequired);
    }

    public void testProcessResult_modelDebugOutput() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
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

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
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

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
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

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, false);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        processor.processResult(context, result);

        verify(persister, times(1)).persistQuantiles(quantiles);
        verify(renormaliser, times(1)).renormalise(quantiles, jobLogger);
        verifyNoMoreInteractions(persister);
        verifyNoMoreInteractions(renormaliser);
    }

    public void testProcessResult_quantiles_isPerPartitionNormalization() {
        Renormaliser renormaliser = mock(Renormaliser.class);
        JobResultsPersister persister = mock(JobResultsPersister.class);
        AutoDetectResultProcessor processor = new AutoDetectResultProcessor(renormaliser, persister, null);

        Logger jobLogger = mock(Logger.class);
        AutoDetectResultProcessor.Context context = new AutoDetectResultProcessor.Context(jobLogger, true);
        context.deleteInterimRequired = false;
        AutodetectResult result = mock(AutodetectResult.class);
        Quantiles quantiles = mock(Quantiles.class);
        when(result.getQuantiles()).thenReturn(quantiles);
        processor.processResult(context, result);

        verify(persister, times(1)).persistQuantiles(quantiles);
        verify(renormaliser, times(1)).renormaliseWithPartition(quantiles, jobLogger);
        verifyNoMoreInteractions(persister);
        verifyNoMoreInteractions(renormaliser);
    }

}