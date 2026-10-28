/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

/**
 * Welford's online algorithm for computing mean and variance in an online fashion.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm">
 *         Welford's online algorithm</a>.
 */
public final class WelfordVariance {

    private double mean;
    private double m2;
    private long count;

    public WelfordVariance() {}

    WelfordVariance(double mean, double m2, long count) {
        this.mean = mean;
        this.m2 = m2;
        this.count = count;
    }

    public void add(int value) {
        add((double) value);
    }

    public void add(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
    }

    /**
     * Advances the observation counter without recording a value. Use when earlier
     * observations were counted but carried no measurement.
     */
    public void advanceToObservation(long observationIndex) {
        count = observationIndex;
    }

    /**
     * Records a value as the {@code observationIndex}-th observation when earlier
     * observations were counted but carried no value.
     */
    public void addAsObservation(double value, long observationIndex) {
        if (observationIndex <= 0) {
            return;
        }
        double delta = value - mean;
        mean += delta / observationIndex;
        m2 += delta * (value - mean);
        count = observationIndex;
    }

    public void merge(WelfordVariance other) {
        if (other.count == 0) {
            return;
        }
        if (count == 0) {
            mean = other.mean;
            m2 = other.m2;
            count = other.count;
            return;
        }
        double delta = mean - other.mean;
        m2 += other.m2 + delta * delta * count * other.count / (count + other.count);
        mean = (mean * count + other.mean * other.count) / (count + other.count);
        count += other.count;
    }

    public long count() {
        return count;
    }

    public double mean() {
        return mean;
    }

    public double m2() {
        return m2;
    }

    public double sampleVariance() {
        return count < 2 ? 0.0 : m2 / (count - 1);
    }

    public double populationVariance() {
        return count < 1 ? 0.0 : m2 / count;
    }

    public double sampleStdDev() {
        return Math.sqrt(sampleVariance());
    }

    public double populationStdDev() {
        return Math.sqrt(populationVariance());
    }
}
