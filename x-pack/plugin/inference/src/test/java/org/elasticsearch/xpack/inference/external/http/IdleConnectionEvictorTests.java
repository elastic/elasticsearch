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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IdleConnectionEvictorTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = new TestThreadPool(
            getTestName(),
            new ScalingExecutorBuilder(
                UTILITY_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.inference.utility_thread_pool"
            )
        );
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testStart_CallsExecutorSubmit() throws IOReactorException {
        var mockThreadPool = mock(ThreadPool.class);
        var executor = mock(ExecutorService.class);

        when(executor.submit(any(Runnable.class))).thenReturn(mock(Future.class));
        when(mockThreadPool.executor(anyString())).thenReturn(executor);

        var evictor = new IdleConnectionEvictor(
            mockThreadPool,
            createConnectionManager(),
            new TimeValue(1, TimeUnit.NANOSECONDS),
            new TimeValue(1, TimeUnit.NANOSECONDS)
        );

        evictor.start();

        verify(mockThreadPool.executor(UTILITY_THREAD_POOL_NAME), times(1)).submit(any(Runnable.class));
    }

    @SuppressWarnings("unchecked")
    public void testStart_OnlyCallsSubmitOnce() throws IOReactorException {
        var mockThreadPool = mock(ThreadPool.class);
        var executor = mock(ExecutorService.class);

        when(executor.submit(any(Runnable.class))).thenReturn(mock(Future.class));
        when(mockThreadPool.executor(anyString())).thenReturn(executor);

        var evictor = new IdleConnectionEvictor(
            mockThreadPool,
            createConnectionManager(),
            new TimeValue(1, TimeUnit.NANOSECONDS),
            new TimeValue(1, TimeUnit.NANOSECONDS)
        );

        evictor.start();
        evictor.start();

        verify(mockThreadPool.executor(UTILITY_THREAD_POOL_NAME), times(1)).submit(any(Runnable.class));
    }

    public void testCloseExpiredConnections_IsCalled() throws TimeoutException {
        var manager = mock(PoolingNHttpClientConnectionManager.class);
        doThrow(new ElasticsearchException("failure")).when(manager).closeExpiredConnections();

        var evictor = new IdleConnectionEvictor(
            threadPool,
            manager,
            new TimeValue(1, TimeUnit.NANOSECONDS),
            new TimeValue(1, TimeUnit.NANOSECONDS)
        );

        evictor.start();
        evictor.awaitTermination(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        verify(manager, times(1)).closeExpiredConnections();
    }

    public void testCloseIdleConnections_IsCalled() throws TimeoutException {
        var manager = mock(PoolingNHttpClientConnectionManager.class);
        doThrow(new ElasticsearchException("failure")).when(manager).closeIdleConnections(anyLong(), any());

        var evictor = new IdleConnectionEvictor(
            threadPool,
            manager,
            new TimeValue(1, TimeUnit.NANOSECONDS),
            new TimeValue(1, TimeUnit.NANOSECONDS)
        );

        evictor.start();
        evictor.awaitTermination(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        verify(manager, times(1)).closeIdleConnections(anyLong(), any());
    }

    public void testIsRunning_ReturnsFalse_AfterAnExceptionIsThrown() throws TimeoutException {
        var manager = mock(PoolingNHttpClientConnectionManager.class);
        doThrow(new ElasticsearchException("failure")).when(manager).closeIdleConnections(anyLong(), any());

        var evictor = new IdleConnectionEvictor(
            threadPool,
            manager,
            new TimeValue(1, TimeUnit.NANOSECONDS),
            new TimeValue(1, TimeUnit.NANOSECONDS)
        );

        evictor.start();
        evictor.awaitTermination(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        assertFalse(evictor.isRunning());
    }

    private static PoolingNHttpClientConnectionManager createConnectionManager() throws IOReactorException {
        return new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
    }
}
