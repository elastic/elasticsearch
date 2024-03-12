/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Implements a throttler using the <a href="https://en.wikipedia.org/wiki/Token_bucket">token bucket algorithm</a>.
 */
public class RateLimiter {

    private double tokensPerTimeUnit;
    private double accumulatedTokensLimit;
    private double accumulatedTokens;
    private Instant nextTokenAvailability;
    private TimeUnit unit;
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
        if (newAccumulatedTokensLimit < 0) {
            throw new IllegalArgumentException("Accumulated tokens limit must be greater than or equal to 0");
        }

        if (newTokensPerTimeUnit <= 0) {
            throw new IllegalArgumentException("Tokens per time unit must be greater than 0");
        }

        accumulatedTokens = Math.min(accumulatedTokens, newAccumulatedTokensLimit);

        accumulatedTokensLimit = newAccumulatedTokensLimit;
        tokensPerTimeUnit = newTokensPerTimeUnit;
        unit = Objects.requireNonNull(newUnit);
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
            var timeUnitsToWait = additionalTokensRequired / tokensPerTimeUnit;
            var unitsInNanos = unit.toNanos(1);
            nanosToWait = timeUnitsToWait * unitsInNanos;
            accumulatedTokens -= accumulatedTokensToUse;
            nextTokenAvailability = nextTokenAvailability.plus(Duration.ofNanos((long) nanosToWait));
        }

        sleeper.sleep((long) nanosToWait);
    }

    private void accumulateTokens() {
        var now = Instant.now(clock);
        if (now.isAfter(nextTokenAvailability)) {
            var elapsedTime = unit.toChronoUnit().between(nextTokenAvailability, now);
            var newTokens = tokensPerTimeUnit * elapsedTime;
            accumulatedTokens = Math.min(accumulatedTokensLimit, newTokens);
            nextTokenAvailability = now;
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
