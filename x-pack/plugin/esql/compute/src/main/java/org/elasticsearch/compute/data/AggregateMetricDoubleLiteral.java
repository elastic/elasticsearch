/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Java Object representation of AggregateMetricDouble in ES|QL.
 */
public class AggregateMetricDoubleLiteral implements GenericNamedWriteable {
    private static final TransportVersion ESQL_AGGREGATE_METRIC_DOUBLE_LITERAL = TransportVersion.fromName(
        "esql_aggregate_metric_double_literal"
    );

    private final double min;
    private final double max;
    private final double sum;
    private final int count;
    private final boolean minAvailable;
    private final boolean maxAvailable;
    private final boolean sumAvailable;
    private final boolean countAvailable;

    public AggregateMetricDoubleLiteral(
        double min,
        double max,
        double sum,
        int count,
        boolean minAvailable,
        boolean maxAvailable,
        boolean sumAvailable,
        boolean countAvailable
    ) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.minAvailable = minAvailable;
        this.maxAvailable = maxAvailable;
        this.sumAvailable = sumAvailable;
        this.countAvailable = countAvailable;
    }

    public AggregateMetricDoubleLiteral(double min, double max, double sum, int count) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.minAvailable = true;
        this.maxAvailable = true;
        this.sumAvailable = true;
        this.countAvailable = true;
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        GenericNamedWriteable.class,
        "AggregateMetricDoubleLiteral",
        AggregateMetricDoubleLiteral::new
    );

    @Override
    public String getWriteableName() {
        return "AggregateMetricDoubleLiteral";
    }

    public AggregateMetricDoubleLiteral(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            this.min = input.readDouble();
            this.minAvailable = true;
        } else {
            this.min = 0.0;
            this.minAvailable = false;
        }
        if (input.readBoolean()) {
            this.max = input.readDouble();
            this.maxAvailable = true;
        } else {
            this.max = 0.0;
            this.maxAvailable = false;
        }
        if (input.readBoolean()) {
            this.sum = input.readDouble();
            this.sumAvailable = true;
        } else {
            this.sum = 0.0;
            this.sumAvailable = false;
        }
        if (input.readBoolean()) {
            this.count = input.readInt();
            this.countAvailable = true;
        } else {
            this.count = 0;
            this.countAvailable = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (minAvailable) {
            out.writeBoolean(true);
            out.writeDouble(min);
        } else {
            out.writeBoolean(false);
        }
        if (maxAvailable) {
            out.writeBoolean(true);
            out.writeDouble(max);
        } else {
            out.writeBoolean(false);
        }
        if (sumAvailable) {
            out.writeBoolean(true);
            out.writeDouble(sum);
        } else {
            out.writeBoolean(false);
        }
        if (countAvailable) {
            out.writeBoolean(true);
            out.writeInt(count);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(ESQL_AGGREGATE_METRIC_DOUBLE_LITERAL);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "must not be called when overriding supportsVersion";
        throw new UnsupportedOperationException("must not be called when overriding supportsVersion");
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public boolean isMinAvailable() {
        return minAvailable;
    }

    public boolean isMaxAvailable() {
        return maxAvailable;
    }

    public boolean isSumAvailable() {
        return sumAvailable;
    }

    public boolean isCountAvailable() {
        return countAvailable;
    }

    @Override
    public String toString() {
        return "AggregateMetricDoubleLiteral("
            + (minAvailable ? "min: " + min + ", " : "")
            + (maxAvailable ? "max: " + max + ", " : "")
            + (sumAvailable ? "sum: " + sum + ", " : "")
            + (countAvailable ? "count: " + count : "")
            + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AggregateMetricDoubleLiteral == false) {
            return false;
        }
        var that = (AggregateMetricDoubleLiteral) o;
        return Double.compare(min, that.min) == 0
            && Double.compare(max, that.max) == 0
            && Double.compare(sum, that.sum) == 0
            && count == that.count
            && minAvailable == that.minAvailable
            && maxAvailable == that.maxAvailable
            && sumAvailable == that.sumAvailable
            && countAvailable == that.countAvailable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, sum, count, minAvailable, maxAvailable, sumAvailable, countAvailable);
    }
}
