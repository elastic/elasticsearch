/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.LogBuilder;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ThrottlerTests extends ESTestCase {
    private DeterministicTaskQueue taskQueue;

    @Before
    public void init() {
        taskQueue = new DeterministicTaskQueue();
    }

    public void testExecute_LogsOnlyOnce() {
        var logger = mock(LogBuilder.class);

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                taskQueue.getThreadPool(),
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.init();
            throttler.execute(logger, "test");

            verify(logger, times(1)).log(eq("test"));

            throttler.execute(logger, "test");
            verifyNoMoreInteractions(logger);
        }
    }

    public void testExecute_LogsOnce_ThenOnceWhenEmittingThreadRuns() {
        var logger = mock(LogBuilder.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>())) {
            throttler.init();
            when(clock.instant()).thenReturn(now);

            // The first call is always logged
            throttler.execute(logger, "test");
            verify(logger, times(1)).log(eq("test"));

            // This should increment the skipped log count but not actually log anything
            throttler.execute(logger, "test");
            verifyNoMoreInteractions(logger);

            clearInvocations(logger);

            // This should log a message with the skip count as 1
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            verify(logger, times(1)).log(contains("test, repeated 1 time"));

            verifyNoMoreInteractions(logger);
        }
    }

    public void testExecute_LogsOnce_ThenOnceWhenEmittingThreadRuns_ThenOnceForFirstLog() {
        var logger = mock(LogBuilder.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>())) {
            throttler.init();
            when(clock.instant()).thenReturn(now);

            // The first call is always logged
            throttler.execute(logger, "test");
            verify(logger, times(1)).log(eq("test"));

            // This should increment the skipped log count but not actually log anything
            throttler.execute(logger, "test");
            verifyNoMoreInteractions(logger);

            clearInvocations(logger);

            // This should log a message with the skip count as 1
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            verify(logger, times(1)).log(contains("test, repeated 1 time"));

            clearInvocations(logger);

            // Since the thread ran in the code above it will have reset the state so this will be treated as a first message
            throttler.execute(logger, "test");
            verify(logger, times(1)).log(eq("test"));

            verifyNoMoreInteractions(logger);
        }
    }

    public void testExecute_AllowsDifferentMessagesToBeLogged() {
        var logger = mock(LogBuilder.class);

        var clock = mock(Clock.class);

        try (var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>())) {
            throttler.init();
            throttler.execute(logger, "test");
            verify(logger, times(1)).log(eq("test"));

            throttler.execute(logger, "a different message");
            verify(logger, times(1)).log(eq("a different message"));
        }
    }

    public void testExecute_LogsRepeated2Times() {
        var logger = mock(LogBuilder.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>())) {
            throttler.init();
            when(clock.instant()).thenReturn(now);

            // The first call is always logged
            throttler.execute(logger, "test");
            verify(logger, times(1)).log(eq("test"));

            // This should increment the skipped log count but not actually log anything
            throttler.execute(logger, "test");
            verifyNoMoreInteractions(logger);

            // This should increment the skipped log count but not actually log anything
            throttler.execute(logger, "test");
            verifyNoMoreInteractions(logger);

            clearInvocations(logger);

            // This should log a message with the skip count as 2
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            verify(logger, times(1)).log(contains("test, repeated 2 time"));

            verifyNoMoreInteractions(logger);
        }
    }

    public void testClose_DoesNotAllowLoggingAnyMore() {
        var logger = mock(LogBuilder.class);

        var clock = mock(Clock.class);

        var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>());

        throttler.close();
        throttler.execute(logger, "test");
        verifyNoMoreInteractions(logger);
    }
}
