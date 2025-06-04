/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEventTests;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutodetectCommunicatorTests extends ESTestCase {

    private AnalysisRegistry analysisRegistry;
    private StateStreamer stateStreamer;

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(TestEnvironment.newEnvironment(settings));
        stateStreamer = mock(StateStreamer.class);
    }

    public void testWriteResetBucketsControlMessage() throws IOException {
        DataLoadParams params = new DataLoadParams(TimeRange.builder().startTime("1").endTime("2").build(), Optional.empty());
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutodetectResultProcessor.class))) {
            communicator.writeToJob(
                new ByteArrayInputStream(new byte[0]),
                analysisRegistry,
                randomFrom(XContentType.values()),
                params,
                (dataCounts, e) -> {}
            );
            verify(process).writeResetBucketsControlMessage(params);
        }
    }

    public void testWriteUpdateProcessMessage() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isReady()).thenReturn(true);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutodetectResultProcessor.class));

        DetectionRule updatedRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "bar")).build();
        List<JobUpdate.DetectorUpdate> detectorUpdates = Collections.singletonList(
            new JobUpdate.DetectorUpdate(0, "updated description", Collections.singletonList(updatedRule))
        );

        List<ScheduledEvent> events = Collections.singletonList(ScheduledEventTests.createScheduledEvent(randomAlphaOfLength(10)));
        UpdateProcessMessage.Builder updateProcessMessage = new UpdateProcessMessage.Builder().setDetectorUpdates(detectorUpdates);
        updateProcessMessage.setScheduledEvents(events);

        communicator.writeUpdateProcessMessage(updateProcessMessage.build(), ((aVoid, e) -> {}));

        verify(process).writeUpdateDetectorRulesMessage(eq(0), eq(Collections.singletonList(updatedRule)));
        verify(process).writeUpdateScheduledEventsMessage(events, AnalysisConfig.Builder.DEFAULT_BUCKET_SPAN);
        verify(process).isProcessAlive();
        verifyNoMoreInteractions(process);
    }

    public void testFlushJob() throws Exception {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutodetectResultProcessor processor = mock(AutodetectResultProcessor.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(processor.waitForFlushAcknowledgement(nullable(String.class), any())).thenReturn(flushAcknowledgement);
        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, processor)) {
            FlushJobParams params = FlushJobParams.builder().build();
            AtomicReference<FlushAcknowledgement> flushAcknowledgementHolder = new AtomicReference<>();
            communicator.flushJob(params, (f, e) -> flushAcknowledgementHolder.set(f));
            assertThat(flushAcknowledgementHolder.get(), equalTo(flushAcknowledgement));
            verify(process).flushJob(params);
        }
    }

    public void testWaitForFlushReturnsIfParserFails() throws Exception {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isProcessAlive()).thenReturn(true);
        AutodetectResultProcessor processor = mock(AutodetectResultProcessor.class);
        when(processor.isFailed()).thenReturn(true);
        when(processor.waitForFlushAcknowledgement(anyString(), any())).thenReturn(null);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, processor);
        expectThrows(ElasticsearchException.class, () -> communicator.waitFlushToCompletion("foo", true));
    }

    public void testFlushJob_throwsIfProcessIsDead() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isProcessAlive()).thenReturn(false);
        when(process.readError()).thenReturn("Mock process is dead");
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutodetectResultProcessor.class));
        FlushJobParams params = FlushJobParams.builder().build();
        Exception[] holder = new ElasticsearchException[1];
        communicator.flushJob(params, (aVoid, e1) -> holder[0] = e1);
        assertEquals("[foo] Unexpected death of autodetect: Mock process is dead", holder[0].getMessage());
    }

    public void testFlushJob_givenFlushWaitReturnsTrueOnSecondCall() throws Exception {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutodetectResultProcessor autodetectResultProcessor = mock(AutodetectResultProcessor.class);
        FlushAcknowledgement flushAcknowledgement = mock(FlushAcknowledgement.class);
        when(autodetectResultProcessor.waitForFlushAcknowledgement(nullable(String.class), eq(Duration.ofSeconds(1)))).thenReturn(null)
            .thenReturn(flushAcknowledgement);
        FlushJobParams params = FlushJobParams.builder().build();

        try (AutodetectCommunicator communicator = createAutodetectCommunicator(process, autodetectResultProcessor)) {
            communicator.flushJob(params, (aVoid, e) -> {});
        }

        verify(autodetectResultProcessor, times(2)).waitForFlushAcknowledgement(nullable(String.class), eq(Duration.ofSeconds(1)));
        // First in checkAndRun, second due to check between calls to waitForFlushAcknowledgement and third due to close()
        verify(process, times(3)).isProcessAlive();
    }

    public void testCloseGivenProcessIsReady() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isReady()).thenReturn(true);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutodetectResultProcessor.class));

        communicator.close();

        verify(process).close();
        verify(process, never()).kill(anyBoolean());
        verifyNoMoreInteractions(stateStreamer);
    }

    public void testCloseGivenProcessIsNotReady() throws IOException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        when(process.isReady()).thenReturn(false);
        AutodetectCommunicator communicator = createAutodetectCommunicator(process, mock(AutodetectResultProcessor.class));

        communicator.close();

        verify(process).kill(false);
        verify(process, never()).close();
        verify(stateStreamer).cancel();
    }

    public void testKill() throws IOException, TimeoutException {
        AutodetectProcess process = mockAutodetectProcessWithOutputStream();
        AutodetectResultProcessor resultProcessor = mock(AutodetectResultProcessor.class);
        ExecutorService executorService = mock(ExecutorService.class);

        AtomicBoolean finishCalled = new AtomicBoolean(false);
        AutodetectCommunicator communicator = createAutodetectCommunicator(
            executorService,
            process,
            resultProcessor,
            (e, b) -> finishCalled.set(true)
        );
        boolean awaitCompletion = randomBoolean();
        boolean finish = randomBoolean();
        communicator.killProcess(awaitCompletion, finish);
        verify(resultProcessor).setProcessKilled();
        verify(process).kill(awaitCompletion);
        verify(executorService).shutdownNow();
        if (awaitCompletion) {
            verify(resultProcessor).awaitCompletion();
        } else {
            verify(resultProcessor, never()).awaitCompletion();
        }
        assertEquals(finish, finishCalled.get());
    }

    private Job createJobDetails() {
        Job.Builder builder = new Job.Builder("foo");

        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setTimeField("time_field");

        Detector.Builder metric = new Detector.Builder("metric", "value");
        metric.setByFieldName("host-metric");
        Detector.Builder count = new Detector.Builder("count", null);
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(metric.build(), count.build()));

        builder.setDataDescription(dd);
        builder.setAnalysisConfig(ac);
        return builder.build(new Date());
    }

    private AutodetectProcess mockAutodetectProcessWithOutputStream() throws IOException {
        AutodetectProcess process = mock(AutodetectProcess.class);
        when(process.isProcessAlive()).thenReturn(true);
        return process;
    }

    @SuppressWarnings("unchecked")
    private AutodetectCommunicator createAutodetectCommunicator(
        ExecutorService executorService,
        AutodetectProcess autodetectProcess,
        AutodetectResultProcessor autodetectResultProcessor,
        BiConsumer<Exception, Boolean> finishHandler
    ) throws IOException {
        DataCountsReporter dataCountsReporter = mock(DataCountsReporter.class);
        doNothing().when(dataCountsReporter).finishReporting();
        return new AutodetectCommunicator(
            createJobDetails(),
            autodetectProcess,
            stateStreamer,
            dataCountsReporter,
            autodetectResultProcessor,
            finishHandler,
            new NamedXContentRegistry(Collections.emptyList()),
            executorService
        );
    }

    @SuppressWarnings("unchecked")
    private AutodetectCommunicator createAutodetectCommunicator(
        AutodetectProcess autodetectProcess,
        AutodetectResultProcessor autodetectResultProcessor
    ) throws IOException {
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocationOnMock -> {
            Callable<?> runnable = (Callable<?>) invocationOnMock.getArguments()[0];
            runnable.call();
            return mock(Future.class);
        }).when(executorService).submit((Callable<Future<?>>) any());
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        return createAutodetectCommunicator(executorService, autodetectProcess, autodetectResultProcessor, (e, b) -> {});
    }

}
