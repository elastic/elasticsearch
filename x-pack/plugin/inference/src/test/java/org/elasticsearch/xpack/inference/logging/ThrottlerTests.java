/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogBuilder;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class ThrottlerTests extends ESTestCase {
    private DeterministicTaskQueue taskQueue;

    @Before
    public void init() {
        taskQueue = new DeterministicTaskQueue();
    }

    public void testExecute_LogsOnlyOnce() {
        var mockedLogger = mockLogger();

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                taskQueue.getThreadPool(),
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.init();
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verify(1, "test");

            mockedLogger.clearInvocations();

            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verifyNever();

            mockedLogger.verifyNoMoreInteractions();
        }
    }

    public void testExecute_LogsOnce_ThenOnceWhenEmittingThreadRuns() {
        var mockedLogger = mockLogger();

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                taskQueue.getThreadPool(),
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.init();

            // The first call is always logged
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verify(1, "test");

            mockedLogger.clearInvocations();

            // This should increment the skipped log count but not actually log anything
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verifyNever();

            mockedLogger.clearInvocations();

            // This should log a message with the skip count as 1
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            mockedLogger.verifyContains(1, "test, repeated 1 time");

            mockedLogger.verifyNoMoreInteractions();
        }
    }

    public void testExecute_LogsOnce_ThenOnceWhenEmittingThreadRuns_WithException() {
        var mockedLogger = mockLogger();

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                taskQueue.getThreadPool(),
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.init();

            // The first call is always logged
            throttler.execute(mockedLogger.logger, Level.WARN, "test", new IllegalArgumentException("failed"));
            mockedLogger.verify(1, "test");
            mockedLogger.verifyThrowable(1);

            mockedLogger.clearInvocations();

            // This should increment the skipped log count but not actually log anything
            throttler.execute(mockedLogger.logger, Level.WARN, "test", new IllegalArgumentException("failed"));
            mockedLogger.verifyNever();

            mockedLogger.clearInvocations();

            // This should log a message with the skip count as 1
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            mockedLogger.verifyContains(1, "test, repeated 1 time");
            mockedLogger.verifyThrowable(1);

            mockedLogger.verifyNoMoreInteractions();
        }
    }

    public void testExecute_LogsOnce_ThenOnceWhenEmittingThreadRuns_ThenOnceForFirstLog() {
        var mockedLogger = mockLogger();

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                taskQueue.getThreadPool(),
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.init();

            // The first call is always logged
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verify(1, "test");

            mockedLogger.clearInvocations();

            // This should increment the skipped log count but not actually log anything
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verifyNever();

            mockedLogger.clearInvocations();

            // This should log a message with the skip count as 1
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            mockedLogger.verifyContains(1, "test, repeated 1 time");

            mockedLogger.clearInvocations();

            // Since the thread ran in the code above it will have reset the state so this will be treated as a first message
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verify(1, "test");

            mockedLogger.verifyNoMoreInteractions();
        }
    }

    public void testExecute_AllowsDifferentMessagesToBeLogged() {
        var mockedLogger = mockLogger();

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                taskQueue.getThreadPool(),
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.init();
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verify(1, "test");

            mockedLogger.clearInvocations();

            throttler.execute(mockedLogger.logger, Level.WARN, "a different message");
            mockedLogger.verify(1, "a different message");
        }
    }

    public void testExecute_LogsRepeated2Times() {
        var mockedLogger = mockLogger();

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                taskQueue.getThreadPool(),
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.init();

            // The first call is always logged
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verify(1, "test");

            // This should increment the skipped log count but not actually log anything
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verifyNoMoreInteractions();

            // This should increment the skipped log count but not actually log anything
            throttler.execute(mockedLogger.logger, Level.WARN, "test");
            mockedLogger.verifyNoMoreInteractions();

            mockedLogger.clearInvocations();

            // This should log a message with the skip count as 2
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            mockedLogger.verifyContains(1, "test, repeated 2 time");

            mockedLogger.verifyNoMoreInteractions();
        }
    }

    public void testClose_DoesNotAllowLoggingAnyMore() {
        var mockedLogger = mockLogger();

        var clock = mock(Clock.class);

        var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>());

        throttler.close();
        throttler.execute(mockedLogger.logger, Level.WARN, "test");
        mockedLogger.verifyNoMoreInteractions();
    }

    record MockLogger(Logger logger, LogBuilder logBuilder) {
        MockLogger clearInvocations() {
            Mockito.clearInvocations(logger);
            Mockito.clearInvocations(logBuilder);

            return this;
        }

        MockLogger verifyNoMoreInteractions() {
            Mockito.verifyNoMoreInteractions(logger);
            Mockito.verifyNoMoreInteractions(logBuilder);

            return this;
        }

        MockLogger verify(int times, String message) {
            Mockito.verify(logger, times(times)).atLevel(eq(Level.WARN));
            Mockito.verify(logBuilder, times(times)).log(eq(message));

            return this;
        }

        MockLogger verifyContains(int times, String message) {
            Mockito.verify(logger, times(times)).atLevel(eq(Level.WARN));
            Mockito.verify(logBuilder, times(times)).log(contains(message));

            return this;
        }

        MockLogger verifyNever() {
            Mockito.verify(logger, never()).atLevel(eq(Level.WARN));
            Mockito.verify(logBuilder, never()).log(any(String.class));
            Mockito.verify(logBuilder, never()).log(any(Throwable.class));

            return this;
        }

        MockLogger verifyThrowable(int times) {
            Mockito.verify(logBuilder, times(times)).withThrowable(any(Throwable.class));

            return this;
        }
    }

    static MockLogger mockLogger() {
        var builder = mock(LogBuilder.class);
        when(builder.withThrowable(any(Throwable.class))).thenReturn(builder);
        var logger = mock(Logger.class);
        when(logger.atLevel(any(Level.class))).thenReturn(builder);

        return new MockLogger(logger, builder);
    }
}
