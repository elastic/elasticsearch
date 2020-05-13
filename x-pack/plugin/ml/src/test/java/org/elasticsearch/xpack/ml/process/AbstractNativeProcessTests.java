/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import java.time.Duration;
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
    private CountDownLatch wait = new CountDownLatch(1);

    @Before
    @SuppressWarnings("unchecked")
    public void initialize() throws IOException {
        nativeController = mock(NativeController.class);
        cppLogHandler = mock(CppLogMessageHandler.class);
        // This answer blocks the thread on the executor service.
        // In order to unblock it, the test needs to call wait.countDown().
        doAnswer(
            invocationOnMock -> {
                wait.await();
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
            wait.countDown();
        }
    }

    public void testStart_DoNotDetectCrashWhenProcessIsBeingClosed() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            wait.countDown();
        }
    }

    public void testStart_DoNotDetectCrashWhenProcessIsBeingKilled() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            process.kill();
            wait.countDown();
        }
    }

    public void testStart_DetectCrashWhenInputPipeExists() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            wait.countDown();
            ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);

            verify(onProcessCrash).accept("[foo] test process stopped unexpectedly: ");
        }
    }

    public void testWriteRecord() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            process.writeRecord(new String[] {"a", "b", "c"});
            process.flushStream();

            verify(inputStream).write(any(), anyInt(), anyInt());

            wait.countDown();
        }
    }

    public void testWriteRecord_FailWhenNoInputPipeProvided() throws Exception {
        when(processPipes.getProcessInStream()).thenReturn(Optional.empty());
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            expectThrows(NullPointerException.class, () -> process.writeRecord(new String[] {"a", "b", "c"}));
            wait.countDown();
        }
    }

    public void testFlush() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            process.flushStream();

            verify(inputStream).flush();

            wait.countDown();
        }
    }

    public void testFlush_FailWhenNoInputPipeProvided() throws Exception {
        when(processPipes.getProcessInStream()).thenReturn(Optional.empty());
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            expectThrows(NullPointerException.class, process::flushStream);
            wait.countDown();
        }
    }

    public void testIsReady() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess()) {
            process.start(executorService);
            assertThat(process.isReady(), is(false));
            process.setReady();
            assertThat(process.isReady(), is(true));

            wait.countDown();
        }
    }

    /**
     * Mock-based implementation of {@link AbstractNativeProcess}.
     */
    private class TestNativeProcess extends AbstractNativeProcess {

        TestNativeProcess() {
            super("foo", nativeController, processPipes, 0, null, onProcessCrash, Duration.ZERO);
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        public void persistState() {
        }
    }
}
