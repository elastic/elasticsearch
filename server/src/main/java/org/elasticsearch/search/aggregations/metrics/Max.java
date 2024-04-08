/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class Max extends InternalNumericMetricsAggregation.SingleValue {
    private final double max;
    private final boolean nonEmpty;

    public Max(String name, double max, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.max = max;
        this.nonEmpty = true;
    }

    public static Max createEmptyMax(String name, DocValueFormat formatter, Map<String, Object> metadata) {
        return new Max(name, formatter, metadata);
    }

    private Max(String name, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.max = Double.NEGATIVE_INFINITY;
        this.nonEmpty = false;
    }

    /**
     * Read from a stream.
     */
    public Max(StreamInput in) throws IOException {
        super(in);
        max = in.readDouble();
        this.nonEmpty = max != Double.NEGATIVE_INFINITY || format != DocValueFormat.RAW;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(max);
    }

    @Override
    public String getWriteableName() {
        return MaxAggregationBuilder.NAME;
    }

    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    public double value() {
        return max;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            double max = Double.NEGATIVE_INFINITY;

            @Override
            public void accept(InternalAggregation aggregation) {
                max = Math.max(max, ((Max) aggregation).max);
            }

            @Override
            public InternalAggregation get() {
                return new Max(name, max, format, getMetadata());
            }
        };
    }

    @Override
    public boolean canLeadReduction() {
        return nonEmpty;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isInfinite(max) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? max : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(max).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), max);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        Max other = (Max) obj;
        return Objects.equals(max, other.max);
    }
}
