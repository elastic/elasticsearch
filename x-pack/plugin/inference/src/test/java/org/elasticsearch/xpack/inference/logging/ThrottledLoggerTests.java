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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ThrottledLoggerTests extends ESTestCase {
    public void testWarn_LogsOnlyOnce() {
        var logger = mock(Logger.class);

        var throttled = new ThrottledLogger(logger, 1, TimeValue.timeValueSeconds(10), Clock.fixed(Instant.now(), ZoneId.systemDefault()));
        throttled.warn("test", new IllegalArgumentException("failed"));

        verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

        throttled.warn("test2", new IllegalArgumentException("failed"));
        verifyNoMoreInteractions(logger);
    }

    public void testWarn_LogsOnceAfterDuration() {
        var logger = mock(Logger.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now) // initial value for class
            .thenReturn(now.plus(Duration.ofMinutes(1))) // first log.warn to be after 10 second duration
            .thenReturn(now); // second log.warn will not be after 10 second duration

        var throttled = new ThrottledLogger(logger, 0, TimeValue.timeValueSeconds(10), clock);
        throttled.warn("test", new IllegalArgumentException("failed"));

        verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

        throttled.warn("test2", new IllegalArgumentException("failed"));
        verifyNoMoreInteractions(logger);
    }

    public void testWarn_LogsOnce_ThenAfterDuration() {
        var logger = mock(Logger.class);

        var now = Clock.systemUTC().instant();

        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now) // initial value for class
            .thenReturn(now) // for when the log.warn is allowed
            .thenReturn(now.plus(Duration.ofMinutes(1))) // first log.warn to be after 10 second duration
            .thenReturn(now); // second log.warn will not be after 10 second duration

        var throttled = new ThrottledLogger(logger, 1, TimeValue.timeValueSeconds(10), clock);
        throttled.warn("test", new IllegalArgumentException("failed"));

        verify(logger, times(1)).warn(eq("test"), any(Throwable.class));

        throttled.warn("test2", new IllegalArgumentException("failed"));
        verify(logger, times(1)).warn(eq("test2"), any(Throwable.class));

        throttled.warn("test2", new IllegalArgumentException("failed"));
        verifyNoMoreInteractions(logger);
    }
}
