/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.LogBuilder;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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

            throttler.warn(mockedLogger.logger, "test", new IllegalArgumentException("failed"));
            verify(mockedLogger.logger, times(1)).atWarn();
            verify(mockedLogger.logBuilder, times(1)).withThrowable(any(IllegalArgumentException.class));
            verify(mockedLogger.logBuilder, times(1)).log(eq("test"));

            mockedLogger.clearInvocations();

            throttler.warn(mockedLogger.logger, "test", new IllegalArgumentException("failed"));
            verify(mockedLogger.logger, times(1)).atWarn();
            verify(mockedLogger.logBuilder, times(1)).withThrowable(any(IllegalArgumentException.class));
            verify(mockedLogger.logBuilder, never()).log(any(String.class));
            verifyNoMoreInteractions(mockedLogger.logger);
            verifyNoMoreInteractions(mockedLogger.logBuilder);
        }
    }

    public void testExecute_AllowsDifferentMessagesToBeLogged() {
        var mockedLogger = mockLogger();

        try (var throttler = new ThrottlerManager(Settings.EMPTY, threadPool)) {
            throttler.init(mockClusterServiceEmpty());

            throttler.warn(mockedLogger.logger, "test", new IllegalArgumentException("failed"));
            verify(mockedLogger.logger, times(1)).atWarn();
            verify(mockedLogger.logBuilder, times(1)).withThrowable(any(IllegalArgumentException.class));
            verify(mockedLogger.logBuilder, times(1)).log(eq("test"));

            mockedLogger.clearInvocations();

            throttler.warn(mockedLogger.logger, "a different message", new IllegalArgumentException("failed"));
            verify(mockedLogger.logger, times(1)).atWarn();
            verify(mockedLogger.logBuilder, times(1)).withThrowable(any(IllegalArgumentException.class));
            verify(mockedLogger.logBuilder, times(1)).log(eq("a different message"));
            verifyNoMoreInteractions(mockedLogger.logger);
            verifyNoMoreInteractions(mockedLogger.logBuilder);
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
            manager.warn(mockedLogger.logger, "test", new IllegalArgumentException("failed"));
            verify(mockedLogger.logger, times(1)).atWarn();
            verify(mockedLogger.logBuilder, times(1)).withThrowable(any(IllegalArgumentException.class));
            verify(mockedLogger.logBuilder, times(1)).log(eq("test"));

            mockedLogger.clearInvocations();

            // This should not be emitted but should increment the counter to 1
            manager.warn(mockedLogger.logger, "test", new IllegalArgumentException("failed"));
            verify(mockedLogger.logger, times(1)).atWarn();
            verify(mockedLogger.logBuilder, times(1)).withThrowable(any(IllegalArgumentException.class));
            verify(mockedLogger.logBuilder, never()).log(any(String.class));

            var loggingInterval = TimeValue.timeValueSeconds(1);
            var currentThrottler = manager.getThrottler();
            manager.setLogInterval(loggingInterval);
            assertNotSame(currentThrottler, manager.getThrottler());

            mockedLogger.clearInvocations();

            // This should not be emitted but should increment the counter to 2
            manager.warn(mockedLogger.logger, "test", new IllegalArgumentException("failed"));
            verify(mockedLogger.logger, times(1)).atWarn();
            verify(mockedLogger.logBuilder, times(1)).withThrowable(any(IllegalArgumentException.class));
            verify(mockedLogger.logBuilder, never()).log(any(String.class));

            mockedLogger.clearInvocations();

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            verify(mockedLogger.logBuilder, times(1)).log(contains("test, repeated 2 times"));
        }
    }

    private record MockLogger(Logger logger, LogBuilder logBuilder) {
        void clearInvocations() {
            Mockito.clearInvocations(logger);
            Mockito.clearInvocations(logBuilder);
        }
    }

    private static MockLogger mockLogger() {
        var builder = mock(LogBuilder.class);
        when(builder.withThrowable(any(Throwable.class))).thenReturn(builder);
        var logger = mock(Logger.class);
        when(logger.atWarn()).thenReturn(builder);

        return new MockLogger(logger, builder);
    }

    public static ThrottlerManager mockThrottlerManager() {
        var mockManager = mock(ThrottlerManager.class);
        when(mockManager.getThrottler()).thenReturn(mock(Throttler.class));

        return mockManager;
    }
}
