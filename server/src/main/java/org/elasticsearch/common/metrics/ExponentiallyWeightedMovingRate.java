/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.metrics;

import static java.lang.Math.exp;
import static java.lang.Math.expm1;
import static java.lang.Math.log;

/**
 * Implements a version of an exponentially weighted moving rate (EWMR). This is a calculation over a finite time series of increments to
 * some sort of gauge or counter which gives a value for the rate at which the gauge is being incremented where the weight given to an
 * increment decreases exponentially with how long ago it happened.
 *
 * <p>Definitions: The <i>rate</i> of increase of the gauge or counter over an interval is the sum of the increments which occurred during
 * the interval, divided by the length of the interval. The <i>weighted</i> rate of increase is the sum of the increments with each
 * multiplied by a weight which is a function of how long ago it happened, divided by the integral of the weight function over the interval.
 * The <i>exponentially</i> weighted rate is the weighted rate when the weight function is given by {@code exp(-1.0 * lambda * time)} where
 * {@code lambda} is a constant and {@code time} specifies how long ago the increment happened. A <i>moving</i> rate is simply a rate
 * calculated for an every-growing series of increments (typically by updating the previous rate rather than recalculating from scratch).
 *
 * <p>The times involved in the API of this class can use the caller's choice of units and origin, e.g. they can be millis since the
 * epoch as returned by {@link System#currentTimeMillis}, nanos since an arbitrary origin as returned by {@link System#nanoTime}, or
 * anything else. The only requirement is that the same convention must be used consistently.
 *
 * <p>This class is thread-safe.
 */
public class ExponentiallyWeightedMovingRate {

    // The maths behind this is explained in section 2 of this document: https://github.com/user-attachments/files/19166625/ewma.pdf

    // This implementation uses synchronization to provide thread safety. The synchronized code is all non-blocking, and just performs a
    // fixed small number of floating point operations plus some memory reads and writes. If they take somewhere in the region of 10ns each,
    // we can do up to tens of millions of QPS before the lock risks becoming a bottleneck.

    private final double lambda;
    private final long startTime;
    private double rate;
    private long lastTime;
    private boolean waitingForFirstIncrement;

    /**
     * Constructor.
     *
     * @param lambda A parameter which dictates how quickly the average "forgets" older increments. The weight given to an increment which
     *     happened time {@code timeAgo} ago will be proportional to {@code exp(-1.0 * lambda * timeAgo)}. The half-life is related to this
     *     parameter by the equation {@code exp(-1.0 * lambda * halfLife)} = 0.5}, so {@code lambda = log(2.0) / halfLife)}. This may be
     *     zero, but must not be negative. The units of this value are the inverse of the units being used for time.
     * @param startTime The time to consider the start time for the rate calculation. This must be greater than zero. The units and origin
     *     of this value must match all other calls to this instance.
     */
    public ExponentiallyWeightedMovingRate(double lambda, long startTime) {
        if (lambda < 0.0) {
            throw new IllegalArgumentException("lambda must be non-negative but was " + lambda);
        }
        synchronized (this) {
            this.lambda = lambda;
            this.rate = Double.NaN; // should never be used
            this.startTime = startTime;
            this.lastTime = 0; // should never be used
            this.waitingForFirstIncrement = true;
        }
    }

    /**
     * Returns the EWMR at the given time. The units and origin of this time must match all other calls to this instance. The units
     * of the returned rate are the ratio of the increment units to the time units as used for {@link #addIncrement}.
     *
     * <p>If there have been no increments yet, this returns zero.
     *
     * <p>Otherwise, we require the time to be no earlier than the time of the previous increment, i.e. the value of {@code time}
     * for this call must not be less than the value of {@code time} for the last call to {@link #addIncrement}. If this is not the
     * case, the method behaves as if it had that minimum value.
     */
    public double getRate(long time) {
        synchronized (this) {
            if (waitingForFirstIncrement) {
                return 0.0;
            } else if (time <= lastTime) {
                return rate;
            } else {
                // This is the formula for R(t) given in subsection 2.6 of the document referenced above:
                return expHelper(lastTime - startTime) * exp(-1.0 * lambda * (time - lastTime)) * rate / expHelper(time - startTime);
            }
        }
    }

    /**
     * Given the EWMR {@code currentRate} at time {@code currentTime} and the EWMR {@code oldRate} at time {@code oldTime}, returns the EWMR
     * that would be calculated at {@code currentTime} if the start time was {@code oldTime} rather than the {@code startTime} passed to the
     * parameter. This rate incorporates all the increments that contributed to {@code currentRate} but not to {@code oldRate}. The
     * increments that contributed to {@code oldRate} are effectively 'forgotten'. The units and origin of the times and rates must match
     * all other calls to this instance.
     *
     * <p>Normally, {@code currentTime} should be after {@code oldTime}. If it is not, this method returns zero.
     *
     * <p>Note that this method does <i>not</i> depend on any of the increments made to this {@link ExponentiallyWeightedMovingRate}
     * instance. It is only non-static because it uses this instance's {@code lambda} and {@code startTime}.
     */
    public double calculateRateSince(long currentTime, double currentRate, long oldTime, double oldRate) {
        if (oldTime < startTime) {
            oldTime = startTime;
        }
        if (currentTime <= oldTime) {
            return 0.0;
        }
        // This is the formula for R'(t, T) given in subsection 2.7 of the document referenced above:
        return (expHelper(currentTime - startTime) * currentRate - expHelper(oldTime - startTime) * exp(
            -1.0 * lambda * (currentTime - oldTime)
        ) * oldRate) / expHelper(currentTime - oldTime);
    }

    /**
     * Updates the rate to reflect that the gauge has been incremented by an amount {@code increment} at a time {@code time}. The unit and
     * offset of the time must match all other calls to this instance. The units of the increment are arbitrary but must also be consistent.
     *
     * <p>If this is the first increment, we require it to occur after the start time for the rate calculation, i.e. the value of
     * {@code time} must be greater than {@code startTime} passed to the constructor. If this is not the case, the method behaves as if
     * {@code time} is {@code startTime + 1} to prevent a division by zero error.
     *
     * <p>If this is not the first increment, we require it not to occur before the previous increment, i.e. the value of {@code time} for
     * this call must be greater than or equal to the value for the previous call. If this is not the case, the method behaves as if this
     * call's {@code time} is the same as the previous call's.
     */
    public void addIncrement(double increment, long time) {
        synchronized (this) {
            if (waitingForFirstIncrement) {
                if (time <= startTime) {
                    time = startTime + 1;
                }
                // This is the formula for R(t_1) given in subsection 2.6 of the document referenced above:
                rate = increment / expHelper(time - startTime);
                waitingForFirstIncrement = false;
            } else {
                if (time < lastTime) {
                    time = lastTime;
                }
                // This is the formula for R(t_j+1) given in subsection 2.6 of the document referenced above:
                rate += (increment - expHelper(time - lastTime) * rate) / expHelper(time - startTime);
            }
            lastTime = time;
        }
    }

    /**
     * Returns something mathematically equivalent to {@code (1.0 - exp(-1.0 * lambda * time)) / lambda}, using an implementation which
     * should not be subject to numerical instability when {@code lambda * time} is small. Returns {@code time} when {@code lambda = 0},
     * which is the correct limit.
     */
    private double expHelper(double time) {
        // This is the function E(lambda, t) defined in subsection 2.6 of the document referenced above, and the calculation follows the
        // principles discussed there:
        assert time >= 0.0;
        double lambdaTime = lambda * time;
        if (lambdaTime >= 1.0e-2) {
            // The direct calculation should be fine here:
            return (1.0 - exp(-1.0 * lambdaTime)) / lambda;
        } else if (lambdaTime >= 1.0e-10) {
            // Avoid taking the small difference of two similar quantities by using expm1 here:
            return -1.0 * expm1(-1.0 * lambdaTime) / lambda;
        } else {
            // Approximate exp(-1.0 * lambdaTime) = 1.0 - lambdaTime + 0.5 * lambdaTime * lambdaTime here (also works for lambda = 0):
            return time * (1.0 - 0.5 * lambdaTime);
        }
    }

    /**
     * Returns the configured half-life of this instance. The units are the same as all other times in API calls, and the inverse of the
     * units used for the {@code lambda} constructor parameter. If {@code lambda} is {@code 0.0}, returns {@link Double#POSITIVE_INFINITY}.
     */
    public double getHalfLife() {
        return log(2.0) / lambda;
    }
}
