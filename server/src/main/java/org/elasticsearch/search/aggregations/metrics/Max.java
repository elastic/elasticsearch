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
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Max extends InternalNumericMetricsAggregation.SingleValue {
    private final double max;

    public Max(String name, double max, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.max = max;
    }

    /**
     * Read from a stream.
     */
    public Max(StreamInput in) throws IOException {
        super(in);
        max = in.readDouble();
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
    public Max reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        double max = Double.NEGATIVE_INFINITY;
        for (InternalAggregation aggregation : aggregations) {
            max = Math.max(max, ((Max) aggregation).max);
        }
        return new Max(name, max, format, getMetadata());
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
