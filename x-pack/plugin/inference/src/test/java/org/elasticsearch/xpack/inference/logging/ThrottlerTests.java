/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ThrottlerTests extends ESTestCase {

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

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                TimeValue.timeValueSeconds(10),
                Clock.fixed(Instant.now(), ZoneId.systemDefault()),
                threadPool,
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.execute("test", logger::warn);

            verify(logger, times(1)).warn(eq("test"));

            throttler.execute("test", logger::warn);
            verifyNoMoreInteractions(logger);
        }
    }

    public void testWarn_LogsOnce_ThenOnceAfterDuration() {
        var logger = mock(Logger.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                TimeValue.timeValueSeconds(10),
                clock,
                threadPool,
                new ConcurrentHashMap<>()
            )
        ) {
            when(clock.instant()).thenReturn(now);

            // The first call is always logged
            throttler.execute("test", (message) -> logger.warn(message, new IllegalArgumentException("failed")));
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            when(clock.instant()).thenReturn(now.plus(Duration.ofMinutes(1)));
            // This call should be allowed because the clock thinks it's after the duration period
            throttler.execute("test", (message) -> logger.warn(message, new IllegalArgumentException("failed")));
            verify(logger, times(2)).warn(eq("test"), any(Throwable.class));

            when(clock.instant()).thenReturn(now);
            // This call should not be allowed because the clock doesn't think it's pasted the wait period
            throttler.execute("test", (message) -> logger.warn(message, new IllegalArgumentException("failed")));
            verifyNoMoreInteractions(logger);
        }
    }

    public void testWarn_AllowsDifferentMessagesToBeLogged() {
        var logger = mock(Logger.class);

        var clock = mock(Clock.class);

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                TimeValue.timeValueSeconds(10),
                clock,
                threadPool,
                new ConcurrentHashMap<>()
            )
        ) {
            throttler.execute("test", logger::warn);
            verify(logger, times(1)).warn(eq("test"));

            throttler.execute("a different message", (message) -> logger.warn(message, new IllegalArgumentException("failed")));
            verify(logger, times(1)).warn(eq("a different message"), any(Throwable.class));
        }
    }

    public void testWarn_LogsRepeated1Time() {
        var logger = mock(Logger.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                TimeValue.timeValueSeconds(10),
                clock,
                threadPool,
                new ConcurrentHashMap<>()
            )
        ) {
            when(clock.instant()).thenReturn(now);
            // first message is allowed
            throttler.execute("test", logger::warn);
            verify(logger, times(1)).warn(eq("test"));

            when(clock.instant()).thenReturn(now); // don't allow this message because duration hasn't expired
            throttler.execute("test", logger::warn);
            verify(logger, times(1)).warn(eq("test"));

            when(clock.instant()).thenReturn(now.plus(Duration.ofMinutes(1))); // allow this message by faking expired duration
            throttler.execute("test", logger::warn);
            verify(logger, times(1)).warn(eq("test, repeated 1 time"));
        }
    }

    public void testWarn_LogsRepeated2Times() {
        var logger = mock(Logger.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);

        try (
            var throttler = new Throttler(
                TimeValue.timeValueDays(1),
                TimeValue.timeValueSeconds(10),
                clock,
                threadPool,
                new ConcurrentHashMap<>()
            )
        ) {
            when(clock.instant()).thenReturn(now);
            // message allowed because it is the first one
            throttler.execute("test", (message) -> logger.warn(message, new IllegalArgumentException("failed")));
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            when(clock.instant()).thenReturn(now); // don't allow these messages because duration hasn't expired
            throttler.execute("test", (message) -> logger.warn(message, new IllegalArgumentException("failed")));
            throttler.execute("test", (message) -> logger.warn(message, new IllegalArgumentException("failed")));
            verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

            when(clock.instant()).thenReturn(now.plus(Duration.ofMinutes(1))); // allow this message by faking the duration completion
            throttler.execute("test", (message) -> logger.warn(message, new IllegalArgumentException("failed")));
            verify(logger, times(1)).warn(eq("test, repeated 2 times"), any(Throwable.class));
        }
    }

    public void testResetTask_ClearsInternalsAfterInterval() throws InterruptedException {
        var calledClearLatch = new CountDownLatch(1);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var concurrentMap = mock(ConcurrentHashMap.class);
        doAnswer(invocation -> {
            calledClearLatch.countDown();

            return Void.TYPE;
        }).when(concurrentMap).clear();

        try (@SuppressWarnings("unchecked")
        var ignored = new Throttler(TimeValue.timeValueNanos(1), TimeValue.timeValueSeconds(10), clock, threadPool, concurrentMap)) {
            calledClearLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        }
    }

    public void testClose_DoesNotAllowLoggingAnyMore() {
        var logger = mock(Logger.class);

        var clock = mock(Clock.class);

        var throttler = new Throttler(
            TimeValue.timeValueDays(1),
            TimeValue.timeValueSeconds(10),
            clock,
            threadPool,
            new ConcurrentHashMap<>()
        );

        throttler.close();
        throttler.execute("test", logger::warn);
        verifyNoMoreInteractions(logger);
    }
}
