/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalRate extends InternalNumericMetricsAggregation.SingleValue implements Rate {
    final double sum;
    final double divisor;

    public InternalRate(String name, double sum, double divisor, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.sum = sum;
        this.divisor = divisor;
    }

    /**
     * Read from a stream.
     */
    public InternalRate(StreamInput in) throws IOException {
        super(in);
        sum = in.readDouble();
        divisor = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
        out.writeDouble(divisor);
    }

    @Override
    public String getWriteableName() {
        return RateAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return sum / divisor;
    }

    @Override
    public double getValue() {
        return sum / divisor;
    }

    // for testing only
    DocValueFormat format() {
        return format;
    }

    @Override
    public InternalRate reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        Double firstDivisor = null;
        for (InternalAggregation aggregation : aggregations) {
            double value = ((InternalRate) aggregation).sum;
            kahanSummation.add(value);
            if (firstDivisor == null) {
                firstDivisor = ((InternalRate) aggregation).divisor;
            }
        }
        return new InternalRate(name, kahanSummation.value(), firstDivisor, format, getMetadata());
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalRate(name, samplingContext.scaleUp(sum), divisor, format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), value());
        if (format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(value()).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sum, divisor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalRate that = (InternalRate) obj;
        return Objects.equals(sum, that.sum) && Objects.equals(divisor, that.divisor);
    }
}
