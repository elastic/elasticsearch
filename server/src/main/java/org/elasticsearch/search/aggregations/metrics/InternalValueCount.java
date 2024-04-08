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
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * An internal implementation of {@link ValueCount}.
 */
public class InternalValueCount extends InternalNumericMetricsAggregation.SingleValue implements ValueCount {
    private final long value;

    public InternalValueCount(String name, long value, Map<String, Object> metadata) {
        super(name, null, metadata);
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public InternalValueCount(StreamInput in) throws IOException {
        super(in, false);
        value = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(value);
    }

    @Override
    public String getWriteableName() {
        return ValueCountAggregationBuilder.NAME;
    }

    public static InternalValueCount empty(String name, Map<String, Object> metadata) {
        return new InternalValueCount(name, 0L, metadata);
    }

    @Override
    public long getValue() {
        return value;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            long valueCount = 0;

            @Override
            public void accept(InternalAggregation aggregation) {
                valueCount += ((InternalValueCount) aggregation).value;
            }

            @Override
            public InternalAggregation get() {
                return new InternalValueCount(name, valueCount, getMetadata());
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalValueCount(name, samplingContext.scaleUp(value), getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), value);
        return builder;
    }

    @Override
    public String toString() {
        return "count[" + value + "]";
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

        InternalValueCount that = (InternalValueCount) obj;
        return Objects.equals(this.value, that.value);
    }
}
