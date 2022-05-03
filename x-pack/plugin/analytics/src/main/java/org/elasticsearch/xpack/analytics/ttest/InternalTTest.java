/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalTTest extends InternalNumericMetricsAggregation.SingleValue implements TTest {

    protected final TTestState state;

    InternalTTest(String name, TTestState state, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.state = state;
    }

    /**
     * Read from a stream.
     */
    public InternalTTest(StreamInput in) throws IOException {
        super(in);
        state = in.readNamedWriteable(TTestState.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeNamedWriteable(state);
    }

    @Override
    public String getWriteableName() {
        return TTestAggregationBuilder.NAME;
    }

    // for testing only
    DocValueFormat format() {
        return format;
    }

    @Override
    public InternalTTest reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        TTestState reduced = state.reduce(aggregations.stream().map(a -> ((InternalTTest) a).state));
        return new InternalTTest(name, reduced, format, getMetadata());
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        double value = state.getValue();
        boolean hasValue = Double.isNaN(value) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(value).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), state);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalTTest that = (InternalTTest) obj;
        return Objects.equals(state, that.state);
    }

    @Override
    public double value() {
        return state.getValue();
    }

    @Override
    public double getValue() {
        return state.getValue();
    }

}
