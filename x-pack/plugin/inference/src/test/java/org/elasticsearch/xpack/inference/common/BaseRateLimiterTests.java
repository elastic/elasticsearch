/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseRateLimiterTests extends ESTestCase {

    protected abstract TimeValue tokenMethod(RateLimiter limiter, int tokens) throws InterruptedException;

    protected abstract void sleepValidationMethod(
        TimeValue result,
        RateLimiter.Sleeper mockSleeper,
        int numberOfClassToExpect,
        long expectedMicrosecondsToSleep
    ) throws InterruptedException;

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

    public void testMethod_Throws_WhenTokens_IsZero() {
        var limiter = new RateLimiter(0, 1, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC());
        var exception = expectThrows(IllegalArgumentException.class, () -> limiter.acquire(0));
        assertThat(exception.getMessage(), is("Requested tokens must be positive"));
    }

    public void testMethod_Throws_WhenTokens_IsNegative() {
        var limiter = new RateLimiter(0, 1, TimeUnit.SECONDS, new RateLimiter.TimeUnitSleeper(), Clock.systemUTC());
        var exception = expectThrows(IllegalArgumentException.class, () -> limiter.acquire(-1));
        assertThat(exception.getMessage(), is("Requested tokens must be positive"));
    }

    public void testMethod_First_CallDoesNotSleep() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.MINUTES, sleeper, clock);
        var res = tokenMethod(limiter, 1);
        sleepValidationMethod(res, sleeper, 1, 0);
    }

    public void testMethod_DoesNotSleep_WhenTokenRateIsHigh() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, Double.MAX_VALUE, TimeUnit.MICROSECONDS, sleeper, clock);
        var res = tokenMethod(limiter, 1);
        sleepValidationMethod(res, sleeper, 1, 0);
    }

    public void testMethod_AcceptsMaxIntValue_WhenTokenRateIsHigh() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, Double.MAX_VALUE, TimeUnit.MICROSECONDS, sleeper, clock);
        var res = tokenMethod(limiter, Integer.MAX_VALUE);
        sleepValidationMethod(res, sleeper, 1, 0);
    }

    public void testMethod_AcceptsMaxIntValue_WhenTokenRateIsLow() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        double tokensPerDay = 1;
        var limiter = new RateLimiter(0, tokensPerDay, TimeUnit.DAYS, sleeper, clock);

        var res = tokenMethod(limiter, Integer.MAX_VALUE);
        double tokensPerMicro = tokensPerDay / TimeUnit.DAYS.toMicros(1);
        sleepValidationMethod(res, sleeper, 1, (long) ((double) Integer.MAX_VALUE / tokensPerMicro));
    }

    public void testMethod_SleepsForOneMinute_WhenRequestingOneUnavailableToken() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.MINUTES, sleeper, clock);
        var res = tokenMethod(limiter, 2);
        sleepValidationMethod(res, sleeper, 1, TimeUnit.MINUTES.toMicros(1));
    }

    public void testMethod_SleepsForOneMinute_WhenRequestingOneUnavailableToken_NoAccumulated() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, 1, TimeUnit.MINUTES, sleeper, clock);
        var res = tokenMethod(limiter, 1);
        sleepValidationMethod(res, sleeper, 1, TimeUnit.MINUTES.toMicros(1));
    }

    public void testMethod_SleepsFor10Minute_WhenRequesting10UnavailableToken_NoAccumulated() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, 1, TimeUnit.MINUTES, sleeper, clock);
        var res = tokenMethod(limiter, 10);
        sleepValidationMethod(res, sleeper, 1, TimeUnit.MINUTES.toMicros(10));
    }

    public void testMethod_IncrementsNextTokenAvailabilityInstant_ByOneMinute() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(0, 1, TimeUnit.MINUTES, sleeper, clock);
        var res = tokenMethod(limiter, 1);
        sleepValidationMethod(res, sleeper, 1, TimeUnit.MINUTES.toMicros(1));
        assertThat(limiter.getNextTokenAvailability(), is(now.plus(1, ChronoUnit.MINUTES)));
    }

    public void testMethod_SecondCallToAcquire_ShouldWait_WhenAccumulatedTokensAreDepleted() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.MINUTES, sleeper, clock);

        var res = tokenMethod(limiter, 1);
        sleepValidationMethod(res, sleeper, 1, 0);
        res = tokenMethod(limiter, 1);
        sleepValidationMethod(res, sleeper, 1, TimeUnit.MINUTES.toMicros(1));
    }

    public void testMethod_SecondCallToAcquire_ShouldWaitForHalfDuration_WhenElapsedTimeIsHalfRequiredDuration()
        throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(1, 1, TimeUnit.MINUTES, sleeper, clock);

        var res = tokenMethod(limiter, 1);
        sleepValidationMethod(res, sleeper, 1, 0);
        when(clock.instant()).thenReturn(now.plus(Duration.ofSeconds(30)));
        res = tokenMethod(limiter, 1);
        sleepValidationMethod(res, sleeper, 1, TimeUnit.SECONDS.toMicros(30));
    }

    public void testMethod_ShouldAccumulateTokens() throws InterruptedException {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var sleeper = mock(RateLimiter.Sleeper.class);

        var limiter = new RateLimiter(10, 10, TimeUnit.MINUTES, sleeper, clock);

        var res = tokenMethod(limiter, 5);
        sleepValidationMethod(res, sleeper, 1, 0);
        // it should accumulate 5 tokens
        when(clock.instant()).thenReturn(now.plus(Duration.ofSeconds(30)));
        res = tokenMethod(limiter, 10);
        sleepValidationMethod(res, sleeper, 2, 0);
    }
}
