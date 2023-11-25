/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.external.http.Utils.mockClusterServiceEmpty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ThrottlerManagerTests extends ESTestCase {
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testWarn_LogsOnlyOnce() {
        var logger = mock(Logger.class);

        try (var throttler = new ThrottlerManager(Settings.EMPTY, threadPool, mockClusterServiceEmpty())) {
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));

            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verifyNoMoreInteractions(logger);
        }
    }

    public void testWarn_AllowsDifferentMessagesToBeLogged() {
        var logger = mock(Logger.class);

        try (var throttler = new ThrottlerManager(Settings.EMPTY, threadPool, mockClusterServiceEmpty())) {
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            throttler.warn(logger, "a different message", new IllegalArgumentException("failed"));
            verify(logger, times(1)).warn(eq("a different message"), any(Throwable.class));
        }
    }

    public void testStartsNewThrottler_WhenResetIntervalIsChanged() {
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.Cancellable.class));

        try (var manager = new ThrottlerManager(Settings.EMPTY, mockThreadPool, mockClusterServiceEmpty())) {
            var resetInterval = TimeValue.timeValueSeconds(1);
            var currentThrottler = manager.getThrottler();
            manager.setResetInterval(resetInterval);
            // once for when the throttler is created initially
            verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(TimeValue.timeValueDays(1)), any());
            verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(resetInterval), any());
            assertNotSame(currentThrottler, manager.getThrottler());
        }
    }

    public void testDoesNotStartNewThrottler_WhenWaitDurationIsChanged() {
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.Cancellable.class));

        try (var manager = new ThrottlerManager(Settings.EMPTY, mockThreadPool, mockClusterServiceEmpty())) {
            var currentThrottler = manager.getThrottler();

            var waitDuration = TimeValue.timeValueSeconds(1);
            manager.setWaitDuration(waitDuration);
            // should only call when initializing the throttler
            verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(TimeValue.timeValueDays(1)), any());
            assertSame(currentThrottler, manager.getThrottler());
        }
    }

    public static ThrottlerManager mockThrottlerManager() {
        var mockManager = mock(ThrottlerManager.class);
        when(mockManager.getThrottler()).thenReturn(mock(Throttler.class));

        return mockManager;
    }
}
