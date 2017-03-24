/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutoDetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutodetectCommunicatorTests extends ESTestCase {

    public void testWriteResetBucketsControlMessage() throws IOException {
        DataLoadParams params = new DataLoadParams(TimeRange.builder().startTime("1").endTime("2").build(), Optional.empty());
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class))) {
            communicator.writeToJob(new ByteArrayInputStream(new byte[0]),
                    randomFrom(XContentType.values()), params);
            Mockito.verify(process).writeResetBucketsControlMessage(params);
        }
    }

    public void tesWriteUpdateModelPlotMessage() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class))) {
            ModelPlotConfig config = new ModelPlotConfig();
            communicator.writeUpdateModelPlotMessage(config);
            Mockito.verify(process).writeUpdateModelPlotMessage(config);
        }
    }

    public void testWriteUpdateDetectorRulesMessage() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class))) {

            List<DetectionRule> rules = Collections.singletonList(mock(DetectionRule.class));
            communicator.writeUpdateDetectorRulesMessage(1, rules);
            Mockito.verify(process).writeUpdateDetectorRulesMessage(1, rules);
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

    public void testFlushJob_givenFlushWaitReturnsTrueOnSecondCall() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isProcessAlive()).thenReturn(true);
        AutoDetectResultProcessor autoDetectResultProcessor = Mockito.mock(AutoDetectResultProcessor.class);
        when(autoDetectResultProcessor.waitForFlushAcknowledgement(anyString(), eq(Duration.ofSeconds(1))))
                .thenReturn(false).thenReturn(true);
        InterimResultsParams params = InterimResultsParams.builder().build();

        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, autoDetectResultProcessor)) {
            communicator.flushJob(params);
        }

        verify(autoDetectResultProcessor, times(2)).waitForFlushAcknowledgement(anyString(), eq(Duration.ofSeconds(1)));
        // First in checkAndRun, second due to check between calls to waitForFlushAcknowledgement and third due to close()
        verify(process, times(3)).isProcessAlive();
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
        builder.setCreateTime(new Date());
        return builder.build();
    }

    private AutodetectProcess mockAutodetectProcessWithOutputStream() throws IOException {
        AutodetectProcess process = Mockito.mock(AutodetectProcess.class);
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
        DataCountsReporter dataCountsReporter = mock(DataCountsReporter.class);
        return new AutodetectCommunicator(0L, createJobDetails(), autodetectProcess,
                dataCountsReporter, autoDetectResultProcessor, e -> {
                }, new NamedXContentRegistry(Collections.emptyList()));
    }

    public void testWriteToJobInUse() throws IOException {
        InputStream in = mock(InputStream.class);
        when(in.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutoDetectResultProcessor.class));
        XContentType xContentType = randomFrom(XContentType.values());

        communicator.inUse.set(new CountDownLatch(1));
        expectThrows(ElasticsearchStatusException.class,
                () -> communicator.writeToJob(in, xContentType, mock(DataLoadParams.class)));

        communicator.inUse.set(null);
        communicator.writeToJob(in, xContentType,
                new DataLoadParams(TimeRange.builder().build(), Optional.empty()));
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

    public void testWriteUpdateModelPlotConfigMessageInUse() throws Exception {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutoDetectResultProcessor resultProcessor = mock(AutoDetectResultProcessor.class);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, resultProcessor);

        communicator.inUse.set(new CountDownLatch(1));
        expectThrows(ElasticsearchStatusException.class, () -> communicator.writeUpdateModelPlotMessage(mock(ModelPlotConfig.class)));

        communicator.inUse.set(null);
        communicator.writeUpdateModelPlotMessage(mock(ModelPlotConfig.class));
    }

    public void testWriteUpdateDetectorRulesMessageInUse() throws Exception {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutoDetectResultProcessor resultProcessor = mock(AutoDetectResultProcessor.class);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, resultProcessor);

        List<DetectionRule> rules = Collections.singletonList(mock(DetectionRule.class));
        communicator.inUse.set(new CountDownLatch(1));
        expectThrows(ElasticsearchStatusException.class, () -> communicator.writeUpdateDetectorRulesMessage(0, rules));

        communicator.inUse.set(null);
        communicator.writeUpdateDetectorRulesMessage(0, rules);
    }
}
