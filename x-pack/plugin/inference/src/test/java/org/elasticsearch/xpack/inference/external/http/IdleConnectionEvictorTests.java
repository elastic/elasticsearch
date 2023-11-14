/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IdleConnectionEvictorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testStart_CallsExecutorSubmit() throws IOReactorException {
        var mockThreadPool = mock(ThreadPool.class);

        when(mockThreadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.Cancellable.class));

        try (
            var evictor = new IdleConnectionEvictor(
                mockThreadPool,
                createConnectionManager(),
                new TimeValue(1, TimeUnit.NANOSECONDS),
                new TimeValue(1, TimeUnit.NANOSECONDS)
            )
        ) {
            evictor.start();

            verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), any(), any());
        }
    }

    public void testStart_OnlyCallsSubmitOnce() throws IOReactorException {
        var mockThreadPool = mock(ThreadPool.class);

        when(mockThreadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.Cancellable.class));

        try (
            var evictor = new IdleConnectionEvictor(
                mockThreadPool,
                createConnectionManager(),
                new TimeValue(1, TimeUnit.NANOSECONDS),
                new TimeValue(1, TimeUnit.NANOSECONDS)
            )
        ) {
            evictor.start();
            evictor.start();

            verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), any(), any());
        }
    }

    public void testCloseExpiredConnections_IsCalled() throws InterruptedException {
        var manager = mock(PoolingNHttpClientConnectionManager.class);

        var evictor = new IdleConnectionEvictor(
            threadPool,
            manager,
            new TimeValue(1, TimeUnit.NANOSECONDS),
            new TimeValue(1, TimeUnit.NANOSECONDS)
        );

        CountDownLatch runLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            evictor.close();
            runLatch.countDown();
            return Void.TYPE;
        }).when(manager).closeExpiredConnections();

        evictor.start();
        runLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        verify(manager, times(1)).closeExpiredConnections();
    }

    public void testCloseIdleConnections_IsCalled() throws InterruptedException {
        var manager = mock(PoolingNHttpClientConnectionManager.class);

        var evictor = new IdleConnectionEvictor(
            threadPool,
            manager,
            new TimeValue(1, TimeUnit.NANOSECONDS),
            new TimeValue(1, TimeUnit.NANOSECONDS)
        );

        CountDownLatch runLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            evictor.close();
            runLatch.countDown();
            return Void.TYPE;
        }).when(manager).closeIdleConnections(anyLong(), any());

        evictor.start();
        runLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        verify(manager, times(1)).closeIdleConnections(anyLong(), any());
    }

    public void testIsRunning_ReturnsTrue() throws IOReactorException {
        var evictor = new IdleConnectionEvictor(
            threadPool,
            createConnectionManager(),
            new TimeValue(1, TimeUnit.SECONDS),
            new TimeValue(1, TimeUnit.SECONDS)
        );

        evictor.start();
        assertTrue(evictor.isRunning());
        evictor.close();
    }

    public void testIsRunning_ReturnsFalse() throws IOReactorException {
        var evictor = new IdleConnectionEvictor(
            threadPool,
            createConnectionManager(),
            new TimeValue(1, TimeUnit.SECONDS),
            new TimeValue(1, TimeUnit.SECONDS)
        );

        evictor.start();
        assertTrue(evictor.isRunning());

        evictor.close();
        assertFalse(evictor.isRunning());
    }

    private static PoolingNHttpClientConnectionManager createConnectionManager() throws IOReactorException {
        return new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
    }
}
