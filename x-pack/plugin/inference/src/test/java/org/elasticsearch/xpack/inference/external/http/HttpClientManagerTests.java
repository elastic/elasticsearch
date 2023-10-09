/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpClientManagerTests extends ESTestCase {
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

    public void testStartsANewEvictor_WithNewEvictionInterval() {
        var threadPool = mock(ThreadPool.class);
        var manager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty());

        var evictionInterval = TimeValue.timeValueSeconds(1);
        manager.setEvictionInterval(evictionInterval);
        verify(threadPool).scheduleWithFixedDelay(any(Runnable.class), eq(evictionInterval), any());
    }

    public void testStartsANewEvictor_WithNewEvictionMaxIdle() throws InterruptedException {
        var mockConnectionManager = mock(PoolingNHttpClientConnectionManager.class);

        Settings settings = Settings.builder()
            .put(HttpClientManager.CONNECTION_EVICTION_THREAD_INTERVAL_SETTING.getKey(), TimeValue.timeValueNanos(1))
            .build();
        var manager = new HttpClientManager(settings, mockConnectionManager, threadPool, mockClusterService(settings));

        CountDownLatch runLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            manager.close();
            runLatch.countDown();
            return Void.TYPE;
        }).when(mockConnectionManager).closeIdleConnections(anyLong(), any());

        var evictionMaxIdle = TimeValue.timeValueSeconds(1);
        manager.setEvictionMaxIdle(evictionMaxIdle);
        runLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        verify(mockConnectionManager, times(1)).closeIdleConnections(eq(evictionMaxIdle.millis()), eq(TimeUnit.MILLISECONDS));
    }

    private static ClusterService mockClusterServiceEmpty() {
        return mockClusterService(Settings.EMPTY);
    }

    private static ClusterService mockClusterService(Settings settings) {
        var clusterService = mock(ClusterService.class);

        var registeredSettings = Stream.concat(HttpClientManager.getSettings().stream(), HttpSettings.getSettings().stream())
            .collect(Collectors.toSet());

        var cSettings = new ClusterSettings(settings, registeredSettings);
        when(clusterService.getClusterSettings()).thenReturn(cSettings);

        return clusterService;
    }
}
