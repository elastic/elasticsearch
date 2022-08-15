/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalSimpleValue extends InternalNumericMetricsAggregation.SingleValue implements SimpleValue {
    public static final String NAME = "simple_value";
    protected final double value;

    public InternalSimpleValue(String name, double value, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public InternalSimpleValue(StreamInput in) throws IOException {
        super(in);
        value = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(value);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public double value() {
        return value;
    }

    public double getValue() {
        return value;
    }

    DocValueFormat formatter() {
        return format;
    }

    @Override
    public InternalSimpleValue reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = (Double.isInfinite(value) || Double.isNaN(value)) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(value).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalSimpleValue other = (InternalSimpleValue) obj;
        return Objects.equals(value, other.value);
    }
}
