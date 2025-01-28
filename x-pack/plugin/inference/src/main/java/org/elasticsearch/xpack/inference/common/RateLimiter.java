/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;

import java.time.Clock;
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
 *
 * Example:
 * Time unit: Second
 * Tokens to produce per time unit: 10
 * Limit for tokens in bucket: 100
 *
 * Tokens in bucket after n seconds (n second -> tokens in bucket):
 * 1 sec -> 10 tokens, 2 sec -> 20 tokens, ... , 10 sec -> 100 tokens (bucket full), ... 200 sec -> 100 tokens (no increase in tokens)
 */
public class RateLimiter {

    private double tokensPerMicros;
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

        if (Double.isInfinite(newAccumulatedTokensLimit)) {
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

        // If the new token limit is smaller than what we've accumulated already we need to drop tokens to meet the new token limit
        accumulatedTokens = Math.min(accumulatedTokens, newAccumulatedTokensLimit);

        accumulatedTokensLimit = newAccumulatedTokensLimit;

        var unitsInMicros = newUnit.toMicros(1);
        tokensPerMicros = newTokensPerTimeUnit / unitsInMicros;
        assert Double.isInfinite(tokensPerMicros) == false : "Tokens per microsecond should not be infinity";

        accumulateTokens();
    }

    /**
     * Causes the thread to wait until the tokens are available.
     * This reserves token in advance leading to a reduction of accumulated tokens.
     * @param tokens the number of items of work that should be throttled, typically you'd pass a value of 1 here
     * @throws InterruptedException _
     */
    public void acquire(int tokens) throws InterruptedException {
        sleeper.sleep(reserveInternal(tokens));
    }

    /**
     * Returns the amount of time to wait for the tokens to become available but does not reserve them in advance.
     * A caller will need to call {@link #reserve(int)} or {@link #acquire(int)} after this call.
     * @param tokens the number of items of work that should be throttled, typically you'd pass a value of 1 here. Must be greater than 0.
     * @return the amount of time to wait
     */
    public TimeValue timeToReserve(int tokens) {
        var timeToReserveRes = timeToReserveInternal(tokens);

        return new TimeValue((long) timeToReserveRes.microsToWait, TimeUnit.MICROSECONDS);
    }

    private TimeToReserve timeToReserveInternal(int tokens) {
        validateTokenRequest(tokens);

        double microsToWait;
        accumulateTokens();
        var accumulatedTokensToUse = Math.min(tokens, accumulatedTokens);
        var additionalTokensRequired = tokens - accumulatedTokensToUse;
        microsToWait = additionalTokensRequired / tokensPerMicros;

        return new TimeToReserve(microsToWait, accumulatedTokensToUse);
    }

    private record TimeToReserve(double microsToWait, double accumulatedTokensToUse) {}

    private static void validateTokenRequest(int tokens) {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Requested tokens must be positive");
        }
    }

    /**
     * Returns the amount of time to wait for the tokens to become available.
     * This reserves tokens in advance leading to a reduction of accumulated tokens.
     * @param tokens the number of items of work that should be throttled, typically you'd pass a value of 1 here. Must be greater than 0.
     * @return the amount of time to wait
     */
    public TimeValue reserve(int tokens) {
        return new TimeValue(reserveInternal(tokens), TimeUnit.MICROSECONDS);
    }

    private synchronized long reserveInternal(int tokens) {
        var timeToReserveRes = timeToReserveInternal(tokens);
        accumulatedTokens -= timeToReserveRes.accumulatedTokensToUse;
        nextTokenAvailability = nextTokenAvailability.plus((long) timeToReserveRes.microsToWait, ChronoUnit.MICROS);

        return (long) timeToReserveRes.microsToWait;
    }

    private synchronized void accumulateTokens() {
        var now = Instant.now(clock);
        if (now.isAfter(nextTokenAvailability)) {
            var elapsedTimeMicros = microsBetweenExact(nextTokenAvailability, now);
            var newTokens = tokensPerMicros * elapsedTimeMicros;
            accumulatedTokens = Math.min(accumulatedTokensLimit, accumulatedTokens + newTokens);
            nextTokenAvailability = now;
        }
    }

    private static long microsBetweenExact(Instant start, Instant end) {
        try {
            return ChronoUnit.MICROS.between(start, end);
        } catch (ArithmeticException e) {
            if (end.isAfter(start)) {
                return Long.MAX_VALUE;
            }

            return 0;
        }
    }

    // default for testing
    Instant getNextTokenAvailability() {
        return nextTokenAvailability;
    }

    public interface Sleeper {
        void sleep(long microsecondsToSleep) throws InterruptedException;
    }

    static final class TimeUnitSleeper implements Sleeper {
        public void sleep(long microsecondsToSleep) throws InterruptedException {
            TimeUnit.MICROSECONDS.sleep(microsecondsToSleep);
        }
    }
}
