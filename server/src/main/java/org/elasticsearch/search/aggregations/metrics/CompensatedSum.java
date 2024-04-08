/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

/**
 * Used to calculate sums using the Kahan summation algorithm.
 *
 * <p>The Kahan summation algorithm (also known as compensated summation) reduces the numerical errors that
 * occur when adding a sequence of finite precision floating point numbers. Numerical errors arise due to
 * truncation and rounding. These errors can lead to numerical instability.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan Summation Algorithm</a>
 */
public class CompensatedSum {

    private static final double NO_CORRECTION = 0.0;

    private double value;
    private double delta;

    /**
     * Used to calculate sums using the Kahan summation algorithm.
     *
     * @param value the sum
     * @param delta correction term
     */
    public CompensatedSum(double value, double delta) {
        this.value = value;
        this.delta = delta;
    }

    public CompensatedSum() {
        this(0, 0);
    }

    /**
     * The value of the sum.
     */
    public double value() {
        return value;
    }

    /**
     * The correction term.
     */
    public double delta() {
        return delta;
    }

    /**
     * Increments the Kahan sum by adding a value without a correction term.
     */
    public CompensatedSum add(double value) {
        return add(value, NO_CORRECTION);
    }

    /**
     * Resets the internal state to use the new value and compensation delta
     */
    public void reset(double value, double delta) {
        this.value = value;
        this.delta = delta;
    }

    /**
     * Increments the Kahan sum by adding two sums, and updating the correction term for reducing numeric errors.
     */
    public CompensatedSum add(double value, double delta) {
        // If the value is Inf or NaN, just add it to the running tally to "convert" to
        // Inf/NaN. This keeps the behavior bwc from before kahan summing
        if (Double.isFinite(value) == false) {
            this.value = value + this.value;
        }

        if (Double.isFinite(this.value)) {
            double correctedSum = value + (this.delta + delta);
            double updatedValue = this.value + correctedSum;
            this.delta = correctedSum - (updatedValue - this.value);
            this.value = updatedValue;
        }

        return this;
    }

}
