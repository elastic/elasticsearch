/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.InstantiatingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represent hard_bounds and extended_bounds in histogram aggregations.
 *
 * This class is similar to {@link LongBounds} used in date histograms, but is using longs to store data. LongBounds and DoubleBounds are
 * not used interchangeably and therefore don't share any common interfaces except for serialization.
 */

public class DoubleBounds implements ToXContentFragment, Writeable {
    static final ParseField MIN_FIELD = new ParseField("min");
    static final ParseField MAX_FIELD = new ParseField("max");
    static final InstantiatingObjectParser<DoubleBounds, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<DoubleBounds, Void> parser = InstantiatingObjectParser.builder(
            "double_bounds",
            false,
            DoubleBounds.class
        );
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.doubleValue(),
            MIN_FIELD,
            ObjectParser.ValueType.DOUBLE_OR_NULL
        );
        parser.declareField(
            optionalConstructorArg(),
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.doubleValue(),
            MAX_FIELD,
            ObjectParser.ValueType.DOUBLE_OR_NULL
        );
        PARSER = parser.build();
    }

    /**
     * Min value
     */
    private final Double min;

    /**
     * Max value
     */
    private final Double max;

    /**
     * Construct with bounds.
     */
    public DoubleBounds(Double min, Double max) {
        if (min != null && Double.isFinite(min) == false) {
            throw new IllegalArgumentException("min bound must be finite, got: " + min);
        }
        if (max != null && Double.isFinite(max) == false) {
            throw new IllegalArgumentException("max bound must be finite, got: " + max);
        }
        if (max != null && min != null && max < min) {
            throw new IllegalArgumentException("max bound [" + max + "] must be greater than min bound [" + min + "]");
        }
        this.min = min;
        this.max = max;
    }

    /**
     * Read from a stream.
     */
    public DoubleBounds(StreamInput in) throws IOException {
        min = in.readOptionalDouble();
        max = in.readOptionalDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalDouble(min);
        out.writeOptionalDouble(max);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (min != null) {
            builder.field(MIN_FIELD.getPreferredName(), min);
        }
        if (max != null) {
            builder.field(MAX_FIELD.getPreferredName(), max);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DoubleBounds other = (DoubleBounds) obj;
        return Objects.equals(min, other.min) && Objects.equals(max, other.max);
    }

    public Double getMin() {
        return min;
    }

    public Double getMax() {
        return max;
    }

    /**
     * returns bounds min if it is defined or POSITIVE_INFINITY otherwise
     */
    public static double getEffectiveMin(DoubleBounds bounds) {
        return bounds == null || bounds.min == null ? Double.POSITIVE_INFINITY : bounds.min;
    }

    /**
     * returns bounds max if it is defined or NEGATIVE_INFINITY otherwise
     */
    public static Double getEffectiveMax(DoubleBounds bounds) {
        return bounds == null || bounds.max == null ? Double.NEGATIVE_INFINITY : bounds.max;
    }

    public boolean contain(double value) {
        if (max != null && value > max) {
            return false;
        }
        if (min != null && value < min) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (min != null) {
            b.append(min);
        }
        b.append("--");
        if (max != null) {
            b.append(max);
        }
        return b.toString();
    }
}
