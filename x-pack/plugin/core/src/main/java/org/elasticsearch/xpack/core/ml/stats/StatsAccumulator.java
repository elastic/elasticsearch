/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.metrics.Stats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class to collect min, max, avg and total statistics for a quantity
 */
public class StatsAccumulator implements Writeable {

    public static class Fields {
        public static final String MIN = "min";
        public static final String MAX = "max";
        public static final String AVG = "avg";
        public static final String TOTAL = "total";
    }

    private long count;
    private double total;
    private Double min;
    private Double max;

    public StatsAccumulator() {
    }

    public StatsAccumulator(StreamInput in) throws IOException {
        count = in.readLong();
        total = in.readDouble();
        min = in.readOptionalDouble();
        max = in.readOptionalDouble();
    }

    private StatsAccumulator(long count, double total, double min, double max) {
        this.count = count;
        this.total = total;
        this.min = min;
        this.max = max;
    }

    public void add(double value) {
        count++;
        total += value;
        min = min == null ? value : (value < min ? value : min);
        max = max == null ? value : (value > max ? value : max);
    }

    public double getMin() {
        return min == null ? 0.0 : min;
    }

    public double getMax() {
        return max == null ? 0.0 : max;
    }

    public double getAvg() {
        return count == 0.0 ? 0.0 : total/count;
    }

    public double getTotal() {
        return total;
    }

    public void merge(StatsAccumulator other) {
        count += other.count;
        total += other.total;

        // note: not using Math.min/max as some internal prefetch optimization causes an NPE
        min = min == null ? other.min : (other.min == null ? min : other.min < min ? other.min : min);
        max = max == null ? other.max : (other.max == null ? max : other.max > max ? other.max : max);
    }

    public Map<String, Double> asMap() {
        Map<String, Double> map = new HashMap<>();
        map.put(Fields.MIN, getMin());
        map.put(Fields.MAX, getMax());
        map.put(Fields.AVG, getAvg());
        map.put(Fields.TOTAL, getTotal());
        return map;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(count);
        out.writeDouble(total);
        out.writeOptionalDouble(min);
        out.writeOptionalDouble(max);
    }

    public static StatsAccumulator fromStatsAggregation(Stats statsAggregation) {
        return new StatsAccumulator(statsAggregation.getCount(), statsAggregation.getSum(), statsAggregation.getMin(),
                statsAggregation.getMax());
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, total, min, max);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        StatsAccumulator other = (StatsAccumulator) obj;
        return Objects.equals(count, other.count) && Objects.equals(total, other.total) && Objects.equals(min, other.min)
                && Objects.equals(max, other.max);
    }
}

