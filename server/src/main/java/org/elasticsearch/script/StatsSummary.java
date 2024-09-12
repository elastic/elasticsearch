/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.Strings;

import java.util.Objects;
import java.util.function.DoubleConsumer;

/**
 * The {@link StatsSummary} class accumulates statistical data for a sequence of double values.
 *
 * <p>This class provides statistics such as count, sum, minimum, maximum, and arithmetic mean
 * of the recorded values.
 */
public class StatsSummary implements DoubleConsumer {

    private long count = 0;
    private double sum = 0d;
    private Double min;
    private Double max;

    public StatsSummary() {}

    StatsSummary(long count, double sum, double min, double max) {
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
    }

    @Override
    public void accept(double value) {
        count++;
        sum += value;
        min = min == null ? value : (value < min ? value : min);
        max = max == null ? value : (value > max ? value : max);
    }

    /**
     * Returns the min for recorded value.
     */
    public double getMin() {
        return min == null ? 0.0 : min;
    }

    /**
     * Returns the max for recorded values.
     */
    public double getMax() {
        return max == null ? 0.0 : max;
    }

    /**
     * Returns the arithmetic mean for recorded values.
     */
    public double getAverage() {
        return count == 0.0 ? 0.0 : sum / count;
    }

    /**
     * Returns the sum of all recorded values.
     */
    public double getSum() {
        return sum;
    }

    /**
     * Returns the number of recorded values.
     */
    public long getCount() {
        return count;
    }

    /**
     * Resets the accumulator, clearing all accumulated statistics.
     * After calling this method, the accumulator will be in its initial state.
     */
    public void reset() {
        count = 0;
        sum = 0d;
        min = null;
        max = null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, sum, min, max);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StatsSummary other = (StatsSummary) obj;

        return Objects.equals(count, other.count)
            && Objects.equals(sum, other.sum)
            && Objects.equals(min, other.min)
            && Objects.equals(max, other.max);
    }

    @Override
    public String toString() {
        return Strings.format(
            "%s{count=%d, sum=%f, min=%f, average=%f, max=%f}",
            this.getClass().getSimpleName(),
            getCount(),
            getSum(),
            getMin(),
            getAverage(),
            getMax()
        );
    }
}
