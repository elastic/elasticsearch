/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RateLimiterTimeToReserveTests extends ESTestCase {
    public void testTimeToReserve_Returns_1Second() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, 1, TimeUnit.SECONDS, sleeper, clock);
        var timeToWait = limiter.timeToReserve(1);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(1)));
    }

    public void testTimeToReserve_Returns_1Second_WithoutReservingToken() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, 1, TimeUnit.SECONDS, sleeper, clock);
        var timeToWait = limiter.timeToReserve(1);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(1)));

        timeToWait = limiter.timeToReserve(1);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(1)));
    }

    public void testTimeToReserve_Returns_0Seconds_WhenTokenIsAlreadyAvailable() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.SECONDS, sleeper, clock);
        var timeToWait = limiter.timeToReserve(1);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(0)));
    }

    public void testTimeToReserve_Returns_0Seconds_WhenTokenIsAlreadyAvailable_WithoutReservingToken() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.SECONDS, sleeper, clock);
        var timeToWait = limiter.timeToReserve(1);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(0)));

        timeToWait = limiter.timeToReserve(1);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(0)));
    }

    public void testTimeToReserve_Returns_1Seconds_When1TokenIsAlreadyAvailable_ButRequires2Tokens() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.SECONDS, sleeper, clock);
        var timeToWait = limiter.timeToReserve(2);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(1)));
    }

    public void testTimeToReserve_Returns_1Seconds_When1TokenIsAlreadyAvailable_ButRequires2Tokens_WithoutReservingToken() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.SECONDS, sleeper, clock);
        var timeToWait = limiter.timeToReserve(2);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(1)));

        timeToWait = limiter.timeToReserve(2);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(1)));
    }

    public void testTimeToReserve_Returns_0Seconds_WhenTimeAdvancesToAccumulate2Tokens() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(2, 1, TimeUnit.SECONDS, sleeper, clock);
        // drain the accumulated tokens
        var drainedTokensTime = limiter.reserve(2);
        assertThat(drainedTokensTime, is(TimeValue.timeValueSeconds(0)));

        when(clock.instant()).thenReturn(now.plus(Duration.ofSeconds(2)));
        // 2 tokens should now be available
        var timeToWait = limiter.timeToReserve(2);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(0)));
    }

    public void testTimeToReserve_Returns_0Seconds_WhenTimeAdvancesToAccumulate2Tokens_MethodCallDoesNotReserveTokens() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(2, 1, TimeUnit.SECONDS, sleeper, clock);
        // drain the accumulated tokens
        var drainedTokensTime = limiter.reserve(2);
        assertThat(drainedTokensTime, is(TimeValue.timeValueSeconds(0)));

        when(clock.instant()).thenReturn(now.plus(Duration.ofSeconds(2)));
        // 2 tokens should now be available
        var timeToWait = limiter.timeToReserve(2);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(0)));

        // 2 tokens should still be available
        timeToWait = limiter.timeToReserve(2);
        assertThat(timeToWait, is(TimeValue.timeValueSeconds(0)));
    }
}
