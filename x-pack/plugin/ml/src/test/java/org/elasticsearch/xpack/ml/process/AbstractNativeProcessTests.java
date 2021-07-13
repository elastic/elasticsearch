/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessageHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AbstractNativeProcessTests extends ESTestCase {

    private NativeController nativeController;
    private CppLogMessageHandler cppLogHandler;
    private OutputStream inputStream;
    private InputStream outputStream;
    private OutputStream restoreStream;
    private ProcessPipes processPipes;
    private Consumer<String> onProcessCrash;
    private ExecutorService executorService;
    // This must be counted down at the point where a real native process would terminate, thus
    // causing an end-of-file on the stream tailing its logs.  This will be:
    // 1) After close() for jobs that stop gracefully
    // 2) After kill() for jobs that are forcefully terminated
    // 3) After a simulated crash when we test simulated crash
    private CountDownLatch mockNativeProcessLoggingStreamEnds;

    @Before
    @SuppressWarnings("unchecked")
    public void initialize() throws IOException {
        nativeController = mock(NativeController.class);
        cppLogHandler = mock(CppLogMessageHandler.class);
        mockNativeProcessLoggingStreamEnds = new CountDownLatch(1);
        // This answer blocks the thread on the executor service.
        // In order to unblock it, the test needs to call mockNativeProcessLoggingStreamEnds.countDown().
        doAnswer(
            invocationOnMock -> {
                mockNativeProcessLoggingStreamEnds.await();
                return null;
            }).when(cppLogHandler).tailStream();
        when(cppLogHandler.getErrors()).thenReturn("");
        inputStream = mock(OutputStream.class);
        outputStream = mock(InputStream.class);
        when(outputStream.read(new byte[512])).thenReturn(-1);
        restoreStream = mock(OutputStream.class);
        processPipes = mock(ProcessPipes.class);
        when(processPipes.getLogStreamHandler()).thenReturn(cppLogHandler);
        when(processPipes.getProcessInStream()).thenReturn(Optional.of(inputStream));
        when(processPipes.getProcessOutStream()).thenReturn(Optional.of(outputStream));
        when(processPipes.getRestoreStream()).thenReturn(Optional.of(restoreStream));
        onProcessCrash = mock(Consumer.class);
        executorService = EsExecutors.newFixed("test", 1, 1, EsExecutors.daemonThreadFactory("test"), new ThreadContext(Settings.EMPTY),
            false);
    }

    @After
    public void terminateExecutorService() {
        ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);
        verifyNoMoreInteractions(onProcessCrash);
    }

    public void testStart_DoNotDetectCrashWhenNoInputPipeProvided() throws Exception {
        when(processPipes.getProcessInStream()).thenReturn(Optional.empty());
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
        }
        // Not detecting a crash during the close sequence is confirmed in terminateExecutorService()
    }

    public void testStart_DoNotDetectCrashWhenProcessIsBeingClosed() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
        }
        // Not detecting a crash during the close sequence is confirmed in terminateExecutorService()
    }

    public void testStart_DoNotDetectCrashWhenProcessIsBeingKilled() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            process.kill(randomBoolean());
            // This ends the logging stream immediately after the kill() instead of part
            // way through the close sequence.  It is critical that this is done, otherwise
            // we would not be accurately simulating what happens with the order streams
            // receive end-of-file after a kill() of a real process.  The latch is counted
            // down again during the close() call, but that is harmless.
            mockNativeProcessLoggingStreamEnds.countDown();
        }
        // Not detecting a crash during the close sequence is confirmed in terminateExecutorService()
    }

    public void testStart_DetectCrashBeforeFirstLogMessage() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            // Even though we are simulating no log messages (by not mocking a PID via cppLogHandler.tryGetPid())
            // we need to simulate disconnection of the logging stream.
            mockNativeProcessLoggingStreamEnds.countDown();
            ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);

            verify(onProcessCrash).accept("[foo] test process stopped unexpectedly before logging started: ");
        }
    }

    public void testCrashReporting() throws Exception {
        when(cppLogHandler.tryGetPid()).thenReturn(42L);
        when(cppLogHandler.getErrors()).thenReturn("Failed to find the answer");
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            // Losing the logging stream before the input stream is closed is how we
            // detect crashes.
            mockNativeProcessLoggingStreamEnds.countDown();
            ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);

            verify(onProcessCrash).accept("[foo] test/42 process stopped unexpectedly: Failed to find the answer");
        }
    }

    public void testWriteRecord() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            process.writeRecord(new String[]{"a", "b", "c"});
            process.flushStream();
            verify(inputStream).write(any(), anyInt(), anyInt());
        }
    }

    public void testWriteRecord_FailWhenNoInputPipeProvided() throws Exception {
        when(processPipes.getProcessInStream()).thenReturn(Optional.empty());
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            expectThrows(NullPointerException.class, () -> process.writeRecord(new String[]{"a", "b", "c"}));
        }
    }

    public void testFlush() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            process.flushStream();
            verify(inputStream).flush();
        }
    }

    public void testFlush_FailWhenNoInputPipeProvided() throws Exception {
        when(processPipes.getProcessInStream()).thenReturn(Optional.empty());
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            expectThrows(NullPointerException.class, process::flushStream);
        }
    }

    public void testIsReady() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            assertThat(process.isReady(), is(false));
            process.setReady();
            assertThat(process.isReady(), is(true));
        }
    }

    public void testConsumeAndCloseOutputStream_GivenNoOutputStream() throws Exception {
        when(processPipes.getProcessOutStream()).thenReturn(Optional.empty());
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.consumeAndCloseOutputStream();
        }
    }

    /**
     * Mock-based implementation of {@link AbstractNativeProcess}.
     */
    private class TestNativeProcess extends AbstractNativeProcess {

        TestNativeProcess() {
            super("foo", nativeController, processPipes, 0, null, onProcessCrash);
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        public void persistState() {
        }

        @Override
        public void persistState(long snapshotTimestamp, String snapshotId, String snapshotDescription) {
        }

        @Override
        protected void afterProcessInStreamClose() {
            // This simulates the process's log stream disconnecting shortly after its input stream is closed
            mockNativeProcessLoggingStreamEnds.countDown();
        }
    }
}
