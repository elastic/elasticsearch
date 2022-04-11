/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.cumulativecardinality;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.SimpleValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalSimpleLongValue extends InternalNumericMetricsAggregation.SingleValue implements SimpleValue {
    public static final String NAME = "simple_long_value";
    protected final long value;

    public InternalSimpleLongValue(String name, long value, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public InternalSimpleLongValue(StreamInput in) throws IOException {
        super(in);
        value = in.readZLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeZLong(value);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public double value() {
        return value;
    }

    public long getValue() {
        return value;
    }

    DocValueFormat formatter() {
        return format;
    }

    @Override
    public InternalSimpleLongValue reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
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
        InternalSimpleLongValue other = (InternalSimpleLongValue) obj;
        return Objects.equals(value, other.value);
    }
}
