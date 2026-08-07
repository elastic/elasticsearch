/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

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

    static class MockLogger {
        final Logger logger;
        private final List<String> messages = new ArrayList<>();
        private final List<Throwable> throwables = new ArrayList<>();

        MockLogger(Logger logger) {
            this.logger = logger;
        }

        Logger logger() {
            return logger;
        }

        MockLogger clearInvocations() {
            messages.clear();
            throwables.clear();
            Mockito.clearInvocations(logger);

            return this;
        }

        MockLogger verifyNoMoreInteractions() {
            Mockito.verifyNoMoreInteractions(logger);

            return this;
        }

        MockLogger verify(int times, String message) {
            assertThat("Expected " + times + " log messages", messages, hasSize(times));
            for (String msg : messages) {
                assertEquals(message, msg);
            }

            return this;
        }

        MockLogger verifyContains(int times, String substring) {
            assertThat("Expected " + times + " log messages", messages, hasSize(times));
            for (String msg : messages) {
                assertThat(msg, containsString(substring));
            }

            return this;
        }

        MockLogger verifyNever() {
            Mockito.verify(logger, never()).log(eq(Level.WARN), any(String.class));
            Mockito.verify(logger, never()).log(eq(Level.WARN), ArgumentMatchers.<Supplier<String>>any(), any(Throwable.class));

            return this;
        }

        MockLogger verifyThrowable(int times) {
            assertThat("Expected " + times + " throwables", throwables, hasSize(times));

            return this;
        }
    }

    static MockLogger mockLogger() {
        var logger = mock(Logger.class);
        var mockLogger = new MockLogger(logger);

        doAnswer(invocation -> {
            mockLogger.messages.add((String) invocation.getArgument(1));
            return null;
        }).when(logger).log(eq(Level.WARN), any(String.class));

        doAnswer(invocation -> {
            Supplier<String> supplier = invocation.getArgument(1);
            mockLogger.messages.add(supplier.get());
            Throwable t = invocation.getArgument(2);
            mockLogger.throwables.add(t);
            return null;
        }).when(logger).log(eq(Level.WARN), ArgumentMatchers.<Supplier<String>>any(), any(Throwable.class));

        return mockLogger;
    }
}
