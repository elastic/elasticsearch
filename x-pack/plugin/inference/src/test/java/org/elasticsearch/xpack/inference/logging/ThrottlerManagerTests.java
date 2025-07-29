/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.logging.ThrottlerTests.mockLogger;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThrottlerManagerTests extends ESTestCase {
    private ThreadPool threadPool;
    private DeterministicTaskQueue taskQueue;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityPool());
        taskQueue = new DeterministicTaskQueue();
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testExecute_LogsOnlyOnce() {
        var mockedLogger = mockLogger();

        try (var throttler = new ThrottlerManager(Settings.EMPTY, taskQueue.getThreadPool())) {
            throttler.init(mockClusterServiceEmpty());

            throttler.warn(mockedLogger.logger(), "test", new IllegalArgumentException("failed"));
            mockedLogger.verify(1, "test");
            mockedLogger.verifyThrowable(1);

            mockedLogger.clearInvocations();

            throttler.warn(mockedLogger.logger(), "test", new IllegalArgumentException("failed"));
            mockedLogger.verifyNever();
            mockedLogger.verifyNoMoreInteractions();
        }
    }

    public void testExecute_AllowsDifferentMessagesToBeLogged() {
        var mockedLogger = mockLogger();

        try (var throttler = new ThrottlerManager(Settings.EMPTY, threadPool)) {
            throttler.init(mockClusterServiceEmpty());

            throttler.warn(mockedLogger.logger(), "test", new IllegalArgumentException("failed"));
            mockedLogger.verify(1, "test");
            mockedLogger.verifyThrowable(1);

            mockedLogger.clearInvocations();

            throttler.warn(mockedLogger.logger(), "a different message", new IllegalArgumentException("failed"));
            mockedLogger.verify(1, "a different message");
            mockedLogger.verifyThrowable(1);
            mockedLogger.verifyNoMoreInteractions();
        }
    }

    public void testStartsNewThrottler_WhenLoggingIntervalIsChanged() {
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.Cancellable.class));

        try (var manager = new ThrottlerManager(Settings.EMPTY, mockThreadPool)) {
            manager.init(mockClusterServiceEmpty());
            verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(TimeValue.timeValueHours(1)), any());

            clearInvocations(mockThreadPool);

            var loggingInterval = TimeValue.timeValueSeconds(1);
            var currentThrottler = manager.getThrottler();
            manager.setLogInterval(loggingInterval);
            verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(TimeValue.timeValueSeconds(1)), any());
            assertNotSame(currentThrottler, manager.getThrottler());
        }
    }

    public void testStartsNewThrottler_WhenLoggingIntervalIsChanged_ThreadEmitsPreviousObjectsMessages() {
        var mockedLogger = mockLogger();

        try (var manager = new ThrottlerManager(Settings.EMPTY, taskQueue.getThreadPool())) {
            manager.init(mockClusterServiceEmpty());

            // first log message should be automatically emitted
            manager.warn(mockedLogger.logger(), "test", new IllegalArgumentException("failed"));
            mockedLogger.verify(1, "test");
            mockedLogger.verifyThrowable(1);

            mockedLogger.clearInvocations();

            // This should not be emitted but should increment the counter to 1
            manager.warn(mockedLogger.logger(), "test", new IllegalArgumentException("failed"));
            mockedLogger.verifyNever();

            var loggingInterval = TimeValue.timeValueSeconds(1);
            var currentThrottler = manager.getThrottler();
            manager.setLogInterval(loggingInterval);
            assertNotSame(currentThrottler, manager.getThrottler());

            mockedLogger.clearInvocations();

            // This should not be emitted but should increment the counter to 2
            manager.warn(mockedLogger.logger(), "test", new IllegalArgumentException("failed"));
            mockedLogger.verifyNever();

            mockedLogger.clearInvocations();

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            mockedLogger.verifyContains(1, "test, repeated 2 times");
        }
    }

    public static ThrottlerManager mockThrottlerManager() {
        var mockManager = mock(ThrottlerManager.class);
        when(mockManager.getThrottler()).thenReturn(mock(Throttler.class));

        return mockManager;
    }
}
