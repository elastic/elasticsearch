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

public class Sum extends InternalNumericMetricsAggregation.SingleValue {
    private final double sum;

    public Sum(String name, double sum, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.sum = sum;
    }

    /**
     * Read from a stream.
     */
    public Sum(StreamInput in) throws IOException {
        super(in);
        sum = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
    }

    @Override
    public String getWriteableName() {
        return SumAggregationBuilder.NAME;
    }

    public static Sum empty(String name, DocValueFormat format, Map<String, Object> metadata) {
        return new Sum(name, 0.0, format, metadata);
    }

    @Override
    public double value() {
        return sum;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            final CompensatedSum kahanSummation = new CompensatedSum(0, 0);

            @Override
            public void accept(InternalAggregation aggregation) {
                kahanSummation.add(((Sum) aggregation).sum);
            }

            @Override
            public InternalAggregation get() {
                return new Sum(name, kahanSummation.value(), format, getMetadata());
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new Sum(name, samplingContext.scaleUp(sum), format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), sum);
        if (format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(sum).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sum);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        Sum that = (Sum) obj;
        return Objects.equals(sum, that.sum);
    }
}
