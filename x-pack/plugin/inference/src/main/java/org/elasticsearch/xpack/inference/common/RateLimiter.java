/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.Strings;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Implements a throttler using the <a href="https://en.wikipedia.org/wiki/Token_bucket">token bucket algorithm</a>.
 *
 * The general approach is to define the rate limiter with size (accumulated tokens limit) which dictates how many
 * unused tokens can be saved up, and a rate at which the tokens are created. Then when a thread should be rate limited
 * it can attempt to acquire a certain number of tokens (typically one for each item of work it's going to do). If unused tokens
 * are available in the bucket already, those will be used. If the number of available tokens covers the desired amount
 * the thread will not sleep. If the bucket does not contain enough tokens, it will calculate how long the thread needs to sleep
 * to accumulate the requested amount of tokens.
 *
 * By setting the accumulated tokens limit to a value greater than zero, it effectively allows bursts of traffic. If the accumulated
 * tokens limit is set to zero, it will force the acquiring thread to wait on each call.
 */
public class RateLimiter {

    private double tokensPerNanos;
    private double accumulatedTokensLimit;
    private double accumulatedTokens;
    private Instant nextTokenAvailability;
    private final Sleeper sleeper;
    private final Clock clock;

    /**
     * @param accumulatedTokensLimit the limit for tokens stashed in the bucket
     * @param tokensPerTimeUnit the number of tokens to produce per the time unit passed in
     * @param unit the time unit frequency for generating tokens
     */
    public RateLimiter(double accumulatedTokensLimit, double tokensPerTimeUnit, TimeUnit unit) {
        this(accumulatedTokensLimit, tokensPerTimeUnit, unit, new TimeUnitSleeper(), Clock.systemUTC());
    }

    // default for testing
    RateLimiter(double accumulatedTokensLimit, double tokensPerTimeUnit, TimeUnit unit, Sleeper sleeper, Clock clock) {
        this.sleeper = Objects.requireNonNull(sleeper);
        this.clock = Objects.requireNonNull(clock);
        nextTokenAvailability = Instant.MIN;
        setRate(accumulatedTokensLimit, tokensPerTimeUnit, unit);
    }

    public final synchronized void setRate(double newAccumulatedTokensLimit, double newTokensPerTimeUnit, TimeUnit newUnit) {
        Objects.requireNonNull(newUnit);

        if (newAccumulatedTokensLimit < 0) {
            throw new IllegalArgumentException("Accumulated tokens limit must be greater than or equal to 0");
        }

        if (newAccumulatedTokensLimit == Double.POSITIVE_INFINITY) {
            throw new IllegalArgumentException(
                Strings.format("Accumulated tokens limit must be less than or equal to %s", Double.MAX_VALUE)
            );
        }

        if (newTokensPerTimeUnit <= 0) {
            throw new IllegalArgumentException("Tokens per time unit must be greater than 0");
        }

        if (newTokensPerTimeUnit == Double.POSITIVE_INFINITY) {
            throw new IllegalArgumentException(Strings.format("Tokens per time unit must be less than or equal to %s", Double.MAX_VALUE));
        }

        accumulatedTokens = Math.min(accumulatedTokens, newAccumulatedTokensLimit);

        accumulatedTokensLimit = newAccumulatedTokensLimit;

        var unitsInNanos = newUnit.toNanos(1);
        tokensPerNanos = newTokensPerTimeUnit / unitsInNanos;
        assert tokensPerNanos != Double.POSITIVE_INFINITY : "Tokens per nanosecond should not be infinity";

        accumulateTokens();
    }

    /**
     * Causes the thread to wait until the tokens are available
     * @param tokens the number of items of work that should be throttled, typically you'd pass a value of 1 here
     * @throws InterruptedException
     */
    public void acquire(int tokens) throws InterruptedException {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Requested tokens must be positive");
        }

        double nanosToWait;
        synchronized (this) {
            accumulateTokens();
            var accumulatedTokensToUse = Math.min(tokens, accumulatedTokens);
            var additionalTokensRequired = tokens - accumulatedTokensToUse;
            nanosToWait = additionalTokensRequired / tokensPerNanos;
            accumulatedTokens -= accumulatedTokensToUse;
            nextTokenAvailability = nextTokenAvailability.plus(Duration.ofNanos((long) nanosToWait));
        }

        sleeper.sleep((long) nanosToWait);
    }

    private void accumulateTokens() {
        var now = Instant.now(clock);
        if (now.isAfter(nextTokenAvailability)) {
            var elapsedTimeNanos = nanosBetweenExact(nextTokenAvailability, now);
            var newTokens = tokensPerNanos * elapsedTimeNanos;
            accumulatedTokens = Math.min(accumulatedTokensLimit, newTokens);
            nextTokenAvailability = now;
        }
    }

    private static double nanosBetweenExact(Instant start, Instant end) {
        try {
            return ChronoUnit.NANOS.between(start, end);
        } catch (ArithmeticException e) {
            if (end.isAfter(start)) {
                return Double.POSITIVE_INFINITY;
            }

            return Double.NEGATIVE_INFINITY;
        }
    }

    public interface Sleeper {
        void sleep(long nanosecondsToSleep) throws InterruptedException;
    }

    static final class TimeUnitSleeper implements Sleeper {
        public void sleep(long nanosecondsToSleep) throws InterruptedException {
            TimeUnit.NANOSECONDS.sleep(nanosecondsToSleep);
        }
    }
}
