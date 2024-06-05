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

public class Min extends InternalNumericMetricsAggregation.SingleValue {
    private final double min;

    private final boolean nonEmpty;

    public Min(String name, double min, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.min = min;
        this.nonEmpty = true;
    }

    public static Min createEmptyMin(String name, DocValueFormat formatter, Map<String, Object> metadata) {
        return new Min(name, formatter, metadata);
    }

    private Min(String name, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.min = Double.POSITIVE_INFINITY;
        this.nonEmpty = false;
    }

    /**
     * Read from a stream.
     */
    public Min(StreamInput in) throws IOException {
        super(in);
        min = in.readDouble();
        this.nonEmpty = min != Double.POSITIVE_INFINITY || format != DocValueFormat.RAW;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(min);
    }

    @Override
    public String getWriteableName() {
        return MinAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return min;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            double min = Double.POSITIVE_INFINITY;

            @Override
            public void accept(InternalAggregation aggregation) {
                min = Math.min(min, ((Min) aggregation).min);
            }

            @Override
            public InternalAggregation get() {
                return new Min(getName(), min, format, getMetadata());
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    public boolean canLeadReduction() {
        return nonEmpty;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isInfinite(min) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? min : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(min).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), min);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        Min other = (Min) obj;
        return Objects.equals(min, other.min);
    }

}
