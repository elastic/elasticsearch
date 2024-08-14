/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Objects;
import java.util.function.DoubleConsumer;

public class StatsAccumulator implements DoubleConsumer {

    private long count = 0;
    private double sum = 0d;
    private Double min;
    private Double max;

    @Override
    public void accept(double value) {
        count++;
        sum += value;
        min = min == null ? value : (value < min ? value : min);
        max = max == null ? value : (value > max ? value : max);
    }

    public double getMin() {
        return min == null ? 0.0 : min;
    }

    public double getMax() {
        return max == null ? 0.0 : max;
    }

    public double getAverage() {
        return count == 0.0 ? 0.0 : sum / count;
    }

    public double getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

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

        StatsAccumulator other = (StatsAccumulator) obj;

        return Objects.equals(count, other.count)
            && Objects.equals(sum, other.sum)
            && Objects.equals(min, other.min)
            && Objects.equals(max, other.max);
    }
}
