/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.process.IndexingStateProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.AutodetectControlMsgWriter;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.process.ProcessResultsParser;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessageHandler;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NativeAutodetectProcessTests extends ESTestCase {

    private static final int NUMBER_FIELDS = 5;

    private ExecutorService executorService;
    private CppLogMessageHandler cppLogHandler;
    private ByteArrayOutputStream inputStream;
    private InputStream outputStream;
    private OutputStream restoreStream;
    private InputStream persistStream;
    private ProcessPipes processPipes;

    @Before
    @SuppressWarnings("unchecked")
    public void initialize() {
        executorService = mock(ExecutorService.class);
        when(executorService.submit(any(Runnable.class))).thenReturn(mock(Future.class));
        cppLogHandler = mock(CppLogMessageHandler.class);
        when(cppLogHandler.getErrors()).thenReturn("");
        inputStream = new ByteArrayOutputStream(AutodetectControlMsgWriter.FLUSH_SPACES_LENGTH + 1024);
        outputStream = new ByteArrayInputStream("some string of data".getBytes(StandardCharsets.UTF_8));
        restoreStream = mock(OutputStream.class);
        persistStream = mock(InputStream.class);
        processPipes = mock(ProcessPipes.class);
        when(processPipes.getLogStreamHandler()).thenReturn(cppLogHandler);
        when(processPipes.getProcessInStream()).thenReturn(Optional.of(inputStream));
        when(processPipes.getProcessOutStream()).thenReturn(Optional.of(outputStream));
        when(processPipes.getRestoreStream()).thenReturn(Optional.of(restoreStream));
        when(processPipes.getPersistStream()).thenReturn(Optional.of(persistStream));
    }

    @SuppressWarnings("unchecked")
    public void testProcessStartTime() throws Exception {
        try (NativeAutodetectProcess process = new NativeAutodetectProcess("foo", mock(NativeController.class),
                processPipes, NUMBER_FIELDS, null,
                new ProcessResultsParser<>(AutodetectResult.PARSER, NamedXContentRegistry.EMPTY), mock(Consumer.class), Duration.ZERO)) {
            process.start(executorService, mock(IndexingStateProcessor.class));

            ZonedDateTime startTime = process.getProcessStartTime();
            Thread.sleep(500);
            ZonedDateTime now = ZonedDateTime.now();
            assertTrue(now.isAfter(startTime));

            ZonedDateTime startPlus3 = startTime.plus(3, ChronoUnit.SECONDS);
            assertTrue(now.isBefore(startPlus3));
        }
    }

    @SuppressWarnings("unchecked")
    public void testWriteRecord() throws IOException {
        String[] record = {"r1", "r2", "r3", "r4", "r5"};
        try (NativeAutodetectProcess process = new NativeAutodetectProcess("foo", mock(NativeController.class),
                processPipes, NUMBER_FIELDS, Collections.emptyList(),
                new ProcessResultsParser<>(AutodetectResult.PARSER, NamedXContentRegistry.EMPTY), mock(Consumer.class), Duration.ZERO)) {
            process.start(executorService, mock(IndexingStateProcessor.class));

            process.writeRecord(record);
            process.flushStream();

            ByteBuffer bb = ByteBuffer.wrap(inputStream.toByteArray());

            // read header
            int numFields = bb.getInt();
            Assert.assertEquals(record.length, numFields);
            for (int i = 0; i < numFields; i++) {
                int recordSize = bb.getInt();
                assertEquals(2, recordSize);

                byte[] charBuff = new byte[recordSize];
                for (int j = 0; j < recordSize; j++) {
                    charBuff[j] = bb.get();
                }

                String value = new String(charBuff, StandardCharsets.UTF_8);
                Assert.assertEquals(record[i], value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testFlush() throws IOException {
        try (NativeAutodetectProcess process = new NativeAutodetectProcess("foo", mock(NativeController.class),
                processPipes, NUMBER_FIELDS, Collections.emptyList(),
                new ProcessResultsParser<>(AutodetectResult.PARSER, NamedXContentRegistry.EMPTY), mock(Consumer.class), Duration.ZERO)) {
            process.start(executorService, mock(IndexingStateProcessor.class));

            FlushJobParams params = FlushJobParams.builder().build();
            process.flushJob(params);

            ByteBuffer bb = ByteBuffer.wrap(inputStream.toByteArray());
            assertThat(bb.remaining(), is(greaterThan(AutodetectControlMsgWriter.FLUSH_SPACES_LENGTH)));
        }
    }

    public void testWriteResetBucketsControlMessage() throws IOException {
        DataLoadParams params = new DataLoadParams(TimeRange.builder().startTime("1").endTime("86400").build(), Optional.empty());
        testWriteMessage(p -> p.writeResetBucketsControlMessage(params), AutodetectControlMsgWriter.RESET_BUCKETS_MESSAGE_CODE);
    }

    public void testWriteUpdateConfigMessage() throws IOException {
        testWriteMessage(p -> p.writeUpdateModelPlotMessage(new ModelPlotConfig()), AutodetectControlMsgWriter.UPDATE_MESSAGE_CODE);
    }

    public void testPersistJob() throws IOException {
        testWriteMessage(NativeAutodetectProcess::persistState, AutodetectControlMsgWriter.BACKGROUND_PERSIST_MESSAGE_CODE);
    }

    @SuppressWarnings("unchecked")
    public void testConsumeAndCloseOutputStream() throws IOException {

        try (NativeAutodetectProcess process = new NativeAutodetectProcess("foo", mock(NativeController.class),
            processPipes, NUMBER_FIELDS, Collections.emptyList(),
            new ProcessResultsParser<>(AutodetectResult.PARSER, NamedXContentRegistry.EMPTY), mock(Consumer.class),
            Duration.ZERO)) {

            process.start(executorService);
            process.consumeAndCloseOutputStream();
            assertThat(outputStream.available(), equalTo(0));
        }
    }

    @SuppressWarnings("unchecked")
    public void testPipeConnectTimeout() throws IOException {

        int timeoutSeconds = randomIntBetween(5, 100);

        try (NativeAutodetectProcess process = new NativeAutodetectProcess("foo", mock(NativeController.class),
            processPipes, NUMBER_FIELDS, Collections.emptyList(),
            new ProcessResultsParser<>(AutodetectResult.PARSER, NamedXContentRegistry.EMPTY), mock(Consumer.class),
            Duration.ofSeconds(timeoutSeconds))) {

            process.start(executorService);
        }

        verify(processPipes, times(1)).connectLogStream(eq(Duration.ofSeconds(timeoutSeconds)));
        verify(processPipes, times(1)).connectOtherStreams(eq(Duration.ofSeconds(timeoutSeconds)));
    }

    @SuppressWarnings("unchecked")
    private void testWriteMessage(CheckedConsumer<NativeAutodetectProcess> writeFunction, String expectedMessageCode) throws IOException {
        try (NativeAutodetectProcess process = new NativeAutodetectProcess("foo", mock(NativeController.class),
                processPipes, NUMBER_FIELDS, Collections.emptyList(),
                new ProcessResultsParser<>(AutodetectResult.PARSER, NamedXContentRegistry.EMPTY), mock(Consumer.class), Duration.ZERO)) {
            process.start(executorService, mock(IndexingStateProcessor.class));

            writeFunction.accept(process);
            process.writeUpdateModelPlotMessage(new ModelPlotConfig());
            process.flushStream();

            String message = new String(inputStream.toByteArray(), StandardCharsets.UTF_8);
            assertTrue(message.contains(expectedMessageCode));
        }
    }

    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T t) throws IOException;
    }
}
