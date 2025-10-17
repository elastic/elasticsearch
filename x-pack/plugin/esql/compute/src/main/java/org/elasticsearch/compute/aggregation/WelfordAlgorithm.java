/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

/**
 * Algorithm for calculating standard deviation, one value at a time.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm">
 *         Welford's_online_algorithm</a> and
 *         <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm">
 *         Parallel algorithm</a>
 */
public final class WelfordAlgorithm {
    private double mean;
    private double m2;
    private long count;
    // If true, compute standard deviation, otherwise compute variance
    private final boolean stdDev;

    public double mean() {
        return mean;
    }

    public double m2() {
        return m2;
    }

    public long count() {
        return count;
    }

    /**
     * Creates a new WelfordAlgorithm instance.
     * @param stdDev if true, compute standard deviation, otherwise compute variance
     */
    public WelfordAlgorithm(boolean stdDev) {
        this(0, 0, 0, stdDev);
    }

    /**
     * Creates a new WelfordAlgorithm instance with the given state.
     * @param mean the mean calculated so far
     * @param m2 the sum of squares of differences from the current mean
     * @param count the number of values added so far
     * @param stdDev if true, compute standard deviation, otherwise compute variance
     */
    public WelfordAlgorithm(double mean, double m2, long count, boolean stdDev) {
        this.mean = mean;
        this.m2 = m2;
        this.count = count;
        this.stdDev = stdDev;
    }

    public void add(int value) {
        add((double) value);
    }

    public void add(long value) {
        add((double) value);
    }

    public void add(double value) {
        final double delta = value - mean;
        count += 1;
        mean += delta / count;
        m2 += delta * (value - mean);
    }

    public void add(double meanValue, double m2Value, long countValue) {
        if (countValue == 0) {
            return;
        }
        if (count == 0) {
            mean = meanValue;
            m2 = m2Value;
            count = countValue;
            return;
        }
        double delta = mean - meanValue;
        m2 += m2Value + delta * delta * count * countValue / (count + countValue);
        mean = (mean * count + meanValue * countValue) / (count + countValue);
        count += countValue;
    }

    public double evaluate() {
        if (stdDev == false) {
            return count < 2 ? 0 : m2 / count;
        } else {
            return count < 2 ? 0 : Math.sqrt(m2 / count);
        }
    }
}
