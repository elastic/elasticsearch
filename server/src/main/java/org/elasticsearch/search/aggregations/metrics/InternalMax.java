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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalMax extends InternalNumericMetricsAggregation.SingleValue implements Max {
    private final double max;

    public InternalMax(String name, double max, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, metadata);
        this.format = formatter;
        this.max = max;
    }

    /**
     * Read from a stream.
     */
    public InternalMax(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
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

    @Override
    public double value() {
        return max;
    }

    @Override
    public double getValue() {
        return max;
    }

    @Override
    public InternalMax reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double max = Double.NEGATIVE_INFINITY;
        for (InternalAggregation aggregation : aggregations) {
            max = Math.max(max, ((InternalMax) aggregation).max);
        }
        return new InternalMax(name, max, format, getMetadata());
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
        InternalMax other = (InternalMax) obj;
        return Objects.equals(max, other.max);
    }
}
