/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

    public void testWarn_LogsOnlyOnce() {
        var logger = mock(Logger.class);

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                taskQueue.getThreadPool(),
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.init();
            throttler.warn(logger, "test");

            verify(logger, times(1)).warn(eq("test"));

            throttler.warn(logger, "test");
            verifyNoMoreInteractions(logger);
        }
    }

    public void testWarn_LogsOnce_ThenOnceWhenEmittingThreadRuns() {
        var logger = mock(Logger.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>())) {
            throttler.init();
            when(clock.instant()).thenReturn(now);

            // The first call is always logged
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            // This should increment the skipped log count but not actually log anything
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verifyNoMoreInteractions(logger);

            // This should log a message with the skip count as 1
            taskQueue.runAllRunnableTasks();
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            verifyNoMoreInteractions(logger);
        }
    }

    public void testWarn_LogsOnce_ThenOnceWhenEmittingThreadRuns_ThenOnceForFirstLog() {
        var logger = mock(Logger.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>())) {
            throttler.init();
            when(clock.instant()).thenReturn(now);

            // The first call is always logged
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            // This should increment the skipped log count but not actually log anything
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verifyNoMoreInteractions(logger);

            // This should log a message with the skip count as 1
            taskQueue.runAllRunnableTasks();
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            // Since the thread ran in the code above it will have reset the state so this will be treated as a first message
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            verifyNoMoreInteractions(logger);
        }
    }

    public void testWarn_AllowsDifferentMessagesToBeLogged() {
        var logger = mock(Logger.class);

        var clock = mock(Clock.class);

        try (var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>())) {
            throttler.init();
            throttler.warn(logger, "test");
            verify(logger, times(1)).warn(eq("test"));

            throttler.warn(logger, "a different message", new IllegalArgumentException("failed"));
            verify(logger, times(1)).warn(eq("a different message"), any(Throwable.class));
        }
    }

    public void testWarn_LogsRepeated2Times() {
        var logger = mock(Logger.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>())) {
            throttler.init();
            when(clock.instant()).thenReturn(now);

            // The first call is always logged
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            // This should increment the skipped log count but not actually log anything
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verifyNoMoreInteractions(logger);

            // This should increment the skipped log count but not actually log anything
            throttler.warn(logger, "test", new IllegalArgumentException("failed"));
            verifyNoMoreInteractions(logger);

            // This should log a message with the skip count as 2
            taskQueue.runAllRunnableTasks();
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            verifyNoMoreInteractions(logger);
        }
    }

    public void testClose_DoesNotAllowLoggingAnyMore() {
        var logger = mock(Logger.class);

        var clock = mock(Clock.class);

        var throttler = new Throttler(TimeValue.timeValueDays(1), clock, taskQueue.getThreadPool(), new ConcurrentHashMap<>());

        throttler.close();
        throttler.warn(logger, "test");
        verifyNoMoreInteractions(logger);
    }
}
