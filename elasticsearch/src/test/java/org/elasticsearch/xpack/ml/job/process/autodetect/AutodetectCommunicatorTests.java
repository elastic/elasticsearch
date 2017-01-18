/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.DataDescription;
import org.elasticsearch.xpack.ml.job.Detector;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.StateProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.ml.job.status.StatusReporter;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutodetectCommunicatorTests extends ESTestCase {

    public void testWriteResetBucketsControlMessage() throws IOException {
        DataLoadParams params = new DataLoadParams(TimeRange.builder().startTime("1").endTime("2").build(), false, Optional.empty());
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class))) {
            communicator.writeToJob(new ByteArrayInputStream(new byte[0]), params, () -> false);
            Mockito.verify(process).writeResetBucketsControlMessage(params);
        }
    }

    public void tesWriteUpdateConfigMessage() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class))) {
            String config = "";
            communicator.writeUpdateConfigMessage(config);
            Mockito.verify(process).writeUpdateConfigMessage(config);
        }
    }

    public void testFlushJob() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isProcessAlive()).thenReturn(true);
        AutoDetectResultProcessor processor = mock(AutoDetectResultProcessor.class);
        when(processor.waitForFlushAcknowledgement(anyString(), any())).thenReturn(true);
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, processor)) {
            InterimResultsParams params = InterimResultsParams.builder().build();
            communicator.flushJob(params);
            Mockito.verify(process).flushJob(params);
        }
    }

    public void testFlushJob_throwsIfProcessIsDead() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isProcessAlive()).thenReturn(false);
        when(process.readError()).thenReturn("Mock process is dead");
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class));
        InterimResultsParams params = InterimResultsParams.builder().build();
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, () -> communicator.flushJob(params));
        assertEquals("[foo] Unexpected death of autodetect: Mock process is dead", e.getMessage());
    }

    public void testFlushJob_throwsOnTimeout() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isProcessAlive()).thenReturn(true);
        when(process.readError()).thenReturn("Mock process has stalled");
        AutoDetectResultProcessor autoDetectResultProcessor = Mockito.mock(AutoDetectResultProcessor.class);
        when(autoDetectResultProcessor.waitForFlushAcknowledgement(anyString(), any())).thenReturn(false);
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class))) {
            InterimResultsParams params = InterimResultsParams.builder().build();
            ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, () -> communicator.flushJob(params, 1));
            assertEquals("[foo] Timed out flushing job. Mock process has stalled", e.getMessage());
        }
    }

    public void testClose() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class));
        communicator.close();
        Mockito.verify(process).close();
    }

    private Job createJobDetails() {
        Job.Builder builder = new Job.Builder("foo");

        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setTimeField("time_field");

        Detector.Builder detector = new Detector.Builder("metric", "value");
        detector.setByFieldName("host-metric");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));

        builder.setDataDescription(dd);
        builder.setAnalysisConfig(ac);
        return builder.build();
    }

    private AutodetectProcess mockAutodetectProcessWithOutputStream() throws IOException {
        InputStream io = Mockito.mock(InputStream.class);
        when(io.read(any(byte [].class))).thenReturn(-1);
        AutodetectProcess process = Mockito.mock(AutodetectProcess.class);
        when(process.getProcessOutStream()).thenReturn(io);
        when(process.getPersistStream()).thenReturn(io);
        when(process.isProcessAlive()).thenReturn(true);
        return process;
    }

    private AutodetectCommunicator createAutodetectCommunicator(AutodetectProcess autodetectProcess,
                                                                AutoDetectResultProcessor autoDetectResultProcessor) throws IOException {
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        StatusReporter statusReporter = mock(StatusReporter.class);
        StateProcessor stateProcessor = mock(StateProcessor.class);
        return new AutodetectCommunicator(executorService, createJobDetails(), autodetectProcess, statusReporter,
                autoDetectResultProcessor, stateProcessor);
    }

    public void testWriteToJobInUse() throws IOException {
        InputStream in = mock(InputStream.class);
        when(in.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class));

        communicator.inUse.set(new CountDownLatch(1));
        expectThrows(ElasticsearchStatusException.class,
                () -> communicator.writeToJob(in, mock(DataLoadParams.class), () -> false));

        communicator.inUse.set(null);
        communicator.writeToJob(in, new DataLoadParams(TimeRange.builder().build(), false, Optional.empty()), () -> false);
    }

    public void testFlushInUse() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutoDetectResultProcessor resultProcessor = mock(AutoDetectResultProcessor.class);
        when(resultProcessor.waitForFlushAcknowledgement(any(), any())).thenReturn(true);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, resultProcessor);

        communicator.inUse.set(new CountDownLatch(1));
        InterimResultsParams params = mock(InterimResultsParams.class);
        expectThrows(ElasticsearchStatusException.class, () -> communicator.flushJob(params));

        communicator.inUse.set(null);
        communicator.flushJob(params);
    }

    public void testCloseInUse() throws Exception {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutoDetectResultProcessor resultProcessor = mock(AutoDetectResultProcessor.class);
        when(resultProcessor.waitForFlushAcknowledgement(any(), any())).thenReturn(true);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, resultProcessor);

        CountDownLatch latch = mock(CountDownLatch.class);
        communicator.inUse.set(latch);
        communicator.close();
        verify(latch, times(1)).await();

        communicator.inUse.set(null);
        communicator.close();
    }

    public void testWriteUpdateConfigMessageInUse() throws Exception {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutoDetectResultProcessor resultProcessor = mock(AutoDetectResultProcessor.class);
        when(resultProcessor.waitForFlushAcknowledgement(any(), any())).thenReturn(true);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, resultProcessor);

        communicator.inUse.set(new CountDownLatch(1));
        expectThrows(ElasticsearchStatusException.class, () -> communicator.writeUpdateConfigMessage(""));

        communicator.inUse.set(null);
        communicator.writeUpdateConfigMessage("");
    }

}
