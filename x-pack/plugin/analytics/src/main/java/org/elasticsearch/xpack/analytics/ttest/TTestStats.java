/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Collects basic stats that are needed to perform t-test
 */
public class TTestStats implements Writeable {
    public final long count;
    public final double sum;
    public final double sumOfSqrs;

    public TTestStats(long count, double sum, double sumOfSqrs) {
        this.count = count;
        this.sum = sum;
        this.sumOfSqrs = sumOfSqrs;
    }

    public TTestStats(StreamInput in) throws IOException {
        count = in.readVLong();
        sum = in.readDouble();
        sumOfSqrs = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeDouble(sum);
        out.writeDouble(sumOfSqrs);
    }

    public double variance() {
        double v = (sumOfSqrs - ((sum * sum) / count)) / (count - 1);
        return v < 0 ? 0 : v;
    }

    public double average() {
        return sum / count;
    }

    public static class Reducer implements Consumer<TTestStats> {
        private long count = 0;
        final CompensatedSum compSum = new CompensatedSum(0, 0);
        final CompensatedSum compSumOfSqrs = new CompensatedSum(0, 0);

        @Override
        public void accept(TTestStats stat) {
            count += stat.count;
            compSum.add(stat.sum);
            compSumOfSqrs.add(stat.sumOfSqrs);
        }

        public TTestStats result() {
            return new TTestStats(count, compSum.value(), compSumOfSqrs.value());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TTestStats that = (TTestStats) o;
        return count == that.count && Double.compare(that.sum, sum) == 0 && Double.compare(that.sumOfSqrs, sumOfSqrs) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, sum, sumOfSqrs);
    }
}
