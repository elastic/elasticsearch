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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AbstractNativeProcessTests extends ESTestCase {

    private NativeController nativeController;
    private InputStream logStream;
    private OutputStream inputStream;
    private InputStream outputStream;
    private OutputStream restoreStream;
    private Consumer<String> onProcessCrash;
    private ExecutorService executorService;
    private CountDownLatch wait = new CountDownLatch(1);

    @Before
    @SuppressWarnings("unchecked")
    public void initialize() throws IOException {
        nativeController = mock(NativeController.class);
        logStream = mock(InputStream.class);
        // This answer blocks the thread on the executor service.
        // In order to unblock it, the test needs to call wait.countDown().
        when(logStream.read(new byte[1024])).thenAnswer(
            invocationOnMock -> {
                wait.await();
                return -1;
            });
        inputStream = mock(OutputStream.class);
        outputStream = mock(InputStream.class);
        when(outputStream.read(new byte[512])).thenReturn(-1);
        restoreStream =  mock(OutputStream.class);
        onProcessCrash = mock(Consumer.class);
        executorService = EsExecutors.newFixed("test", 1, 1, EsExecutors.daemonThreadFactory("test"), new ThreadContext(Settings.EMPTY));
    }

    @After
    public void terminateExecutorService() {
        ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);
        verifyNoMoreInteractions(onProcessCrash);
    }

    public void testStart_DoNotDetectCrashWhenNoInputPipeProvided() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(null)) {
            process.start(executorService);
            wait.countDown();
        }
    }

    public void testStart_DoNotDetectCrashWhenProcessIsBeingClosed() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(inputStream)) {
            process.start(executorService);
            process.close();
            wait.countDown();
        }
    }

    public void testStart_DoNotDetectCrashWhenProcessIsBeingKilled() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(inputStream)) {
            process.start(executorService);
            process.kill();
            wait.countDown();
        }
    }

    public void testStart_DetectCrashWhenInputPipeExists() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(inputStream)) {
            process.start(executorService);
            wait.countDown();
            ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);

            verify(onProcessCrash).accept("[foo] test process stopped unexpectedly: ");
        }
    }

    public void testWriteRecord() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(inputStream)) {
            process.writeRecord(new String[] {"a", "b", "c"});
            process.flushStream();

            verify(inputStream).write(any(), anyInt(), anyInt());
        }
    }

    public void testWriteRecord_FailWhenNoInputPipeProvided() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(null)) {
            expectThrows(NullPointerException.class, () -> process.writeRecord(new String[] {"a", "b", "c"}));
        }
    }

    public void testFlush() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(inputStream)) {
            process.flushStream();

            verify(inputStream).flush();
        }
    }

    public void testFlush_FailWhenNoInputPipeProvided() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(null)) {
            expectThrows(NullPointerException.class, () -> process.flushStream());
        }
    }

    public void testIsReady() throws Exception {
        try (AbstractNativeProcess process = new TestNativeProcess(null)) {
            assertThat(process.isReady(), is(false));
            process.setReady();
            assertThat(process.isReady(), is(true));
        }
    }

    /**
     * Mock-based implementation of {@link AbstractNativeProcess}.
     */
    private class TestNativeProcess extends AbstractNativeProcess {

        TestNativeProcess(OutputStream inputStream) {
            super("foo", nativeController, logStream, inputStream, outputStream, restoreStream, 0, null, onProcessCrash);
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        public void persistState() throws IOException {
        }
    }
}
