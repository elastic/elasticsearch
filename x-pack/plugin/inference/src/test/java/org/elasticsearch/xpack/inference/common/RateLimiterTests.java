/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RateLimiterTests extends ESTestCase {
    public void testThrows_WhenAccumulatedTokensLimit_IsNegative() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RateLimiter(-1, 1, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC())
        );
        assertThat(exception.getMessage(), is("Accumulated tokens limit must be greater than or equal to 0"));
    }

    public void testThrows_WhenAccumulatedTokensLimit_IsInfinity() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RateLimiter(Double.POSITIVE_INFINITY, 1, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC())
        );
        assertThat(
            exception.getMessage(),
            is(Strings.format("Accumulated tokens limit must be less than or equal to %s", Double.MAX_VALUE))
        );
    }

    public void testThrows_WhenTokensPerTimeUnit_IsZero() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RateLimiter(0, 0, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC())
        );
        assertThat(exception.getMessage(), is("Tokens per time unit must be greater than 0"));
    }

    public void testThrows_WhenTokensPerTimeUnit_IsInfinity() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RateLimiter(0, Double.POSITIVE_INFINITY, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC())
        );
        assertThat(exception.getMessage(), is(Strings.format("Tokens per time unit must be less than or equal to %s", Double.MAX_VALUE)));
    }

    public void testThrows_WhenTokensPerTimeUnit_IsNegative() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RateLimiter(0, -1, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC())
        );
        assertThat(exception.getMessage(), is("Tokens per time unit must be greater than 0"));
    }

    public void testAcquire_Throws_WhenTokens_IsZero() {
        var limiter = new RateLimiter(0, 1, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC());
        var exception = expectThrows(IllegalArgumentException.class, () -> limiter.acquire(0));
        assertThat(exception.getMessage(), is("Requested tokens must be positive"));
    }

    public void testAcquire_Throws_WhenTokens_IsNegative() {
        var limiter = new RateLimiter(0, 1, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC());
        var exception = expectThrows(IllegalArgumentException.class, () -> limiter.acquire(-1));
        assertThat(exception.getMessage(), is("Requested tokens must be positive"));
    }

    public void testAcquire_First_CallDoesNotSleep() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.MINUTES, sleeper, clock);
        limiter.acquire(1);
        verify(sleeper, times(1)).sleep(0);
    }

    public void testAcquire_DoesNotSleep_WhenTokenRateIsHigh() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, Double.MAX_VALUE, TimeUnit.NANOSECONDS, sleeper, clock);
        limiter.acquire(1);
        verify(sleeper, times(1)).sleep(0);
    }

    public void testAcquire_AcceptsMaxIntValue_WhenTokenRateIsHigh() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, Double.MAX_VALUE, TimeUnit.NANOSECONDS, sleeper, clock);
        limiter.acquire(Integer.MAX_VALUE);
        verify(sleeper, times(1)).sleep(0);
    }

    public void testAcquire_AcceptsMaxIntValue_WhenTokenRateIsLow() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        double tokensPerDay = 1;
        var limiter = new RateLimiter(0, tokensPerDay, TimeUnit.DAYS, sleeper, clock);
        limiter.acquire(Integer.MAX_VALUE);

        double tokensPerNano = tokensPerDay / TimeUnit.DAYS.toNanos(1);
        verify(sleeper, times(1)).sleep((long) ((double) Integer.MAX_VALUE / tokensPerNano));
    }

    public void testAcquire_SleepsForOneMinute_WhenRequestingOneUnavailableToken() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.MINUTES, sleeper, clock);
        limiter.acquire(2);
        verify(sleeper, times(1)).sleep(TimeUnit.MINUTES.toNanos(1));
    }

    public void testAcquire_SleepsForOneMinute_WhenRequestingOneUnavailableToken_NoAccumulated() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, 1, TimeUnit.MINUTES, sleeper, clock);
        limiter.acquire(1);
        verify(sleeper, times(1)).sleep(TimeUnit.MINUTES.toNanos(1));
    }

    public void testAcquire_SleepsFor10Minute_WhenRequesting10UnavailableToken_NoAccumulated() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, 1, TimeUnit.MINUTES, sleeper, clock);
        limiter.acquire(10);
        verify(sleeper, times(1)).sleep(TimeUnit.MINUTES.toNanos(10));
    }

    public void testAcquire_SecondCallToAcquire_ShouldWait_WhenAccumulatedTokensAreDepleted() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.MINUTES, sleeper, clock);
        limiter.acquire(1);
        verify(sleeper, times(1)).sleep(0);
        limiter.acquire(1);
        verify(sleeper, times(1)).sleep(TimeUnit.MINUTES.toNanos(1));
    }

    public void testAcquire_SecondCallToAcquire_ShouldWaitForHalfDuration_WhenElapsedTimeIsHalfRequiredDuration()
        throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.MINUTES, sleeper, clock);
        limiter.acquire(1);
        verify(sleeper, times(1)).sleep(0);
        when(clock.instant()).thenReturn(now.plus(Duration.ofSeconds(30)));
        limiter.acquire(1);
        verify(sleeper, times(1)).sleep(TimeUnit.SECONDS.toNanos(30));
    }
}
