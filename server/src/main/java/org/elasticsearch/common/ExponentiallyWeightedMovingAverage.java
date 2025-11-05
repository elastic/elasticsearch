/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements exponentially weighted moving averages (commonly abbreviated EWMA) for a single value.
 * This class is safe to share between threads using lock-free atomic operations.
 *
 * <p>The exponentially weighted moving average is calculated using the formula:</p>
 * <pre>
 * newAvg = (alpha * newValue) + ((1 - alpha) * currentAvg)
 * </pre>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create EWMA with alpha=0.2 and initial average of 100
 * ExponentiallyWeightedMovingAverage ewma = new ExponentiallyWeightedMovingAverage(0.2, 100.0);
 *
 * // Add new values
 * ewma.addValue(110.0);
 * ewma.addValue(105.0);
 *
 * // Get current average
 * double average = ewma.getAverage();
 * }</pre>
 */
public class ExponentiallyWeightedMovingAverage {

    private final double alpha;
    private final AtomicLong averageBits;

    /**
     * Creates a new EWMA with the specified smoothing factor and initial average.
     * A smaller alpha gives less weight to new data points (slower response to changes),
     * while a higher alpha gives more weight to new data points (faster response to changes).
     *
     * @param alpha the smoothing factor, must be between 0 and 1 (inclusive)
     * @param initialAvg the initial average value
     * @throws IllegalArgumentException if alpha is not between 0 and 1
     */
    public ExponentiallyWeightedMovingAverage(double alpha, double initialAvg) {
        if (alpha < 0 || alpha > 1) {
            throw new IllegalArgumentException("alpha must be greater or equal to 0 and less than or equal to 1");
        }
        this.alpha = alpha;
        this.averageBits = new AtomicLong(Double.doubleToLongBits(initialAvg));
    }

    /**
     * Returns the current exponentially weighted moving average.
     *
     * @return the current average value
     */
    public double getAverage() {
        return Double.longBitsToDouble(this.averageBits.get());
    }

    /**
     * Adds a new value to the moving average calculation. This method updates the average
     * using a lock-free compare-and-set operation, making it thread-safe.
     *
     * @param newValue the new value to incorporate into the moving average
     */
    public void addValue(double newValue) {
        boolean successful = false;
        do {
            final long currentBits = this.averageBits.get();
            final double currentAvg = getAverage();
            final double newAvg = (alpha * newValue) + ((1 - alpha) * currentAvg);
            final long newBits = Double.doubleToLongBits(newAvg);
            successful = averageBits.compareAndSet(currentBits, newBits);
        } while (successful == false);
    }

    // Used for testing
    public double getAlpha() {
        return alpha;
    }
}
