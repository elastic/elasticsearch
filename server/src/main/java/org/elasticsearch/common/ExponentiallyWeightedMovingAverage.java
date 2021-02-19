/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;


import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements exponentially weighted moving averages (commonly abbreviated EWMA) for a single value.
 * This class is safe to share between threads.
 */
public class ExponentiallyWeightedMovingAverage {

    private final double alpha;
    private final AtomicLong averageBits;

    /**
     * Create a new EWMA with a given {@code alpha} and {@code initialAvg}. A smaller alpha means
     * that new data points will have less weight, where a high alpha means older data points will
     * have a lower influence.
     */
    public ExponentiallyWeightedMovingAverage(double alpha, double initialAvg) {
        if (alpha < 0 || alpha > 1) {
            throw new IllegalArgumentException("alpha must be greater or equal to 0 and less than or equal to 1");
        }
        this.alpha = alpha;
        this.averageBits = new AtomicLong(Double.doubleToLongBits(initialAvg));
    }

    public double getAverage() {
        return Double.longBitsToDouble(this.averageBits.get());
    }

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
}
