/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalRate extends InternalNumericMetricsAggregation.SingleValue implements Rate {
    final double sum;
    final double divider;

    public InternalRate(String name, double sum, double divider, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, metadata);
        this.sum = sum;
        this.divider = divider;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    public InternalRate(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        sum = in.readDouble();
        divider = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
        out.writeDouble(divider);
    }

    @Override
    public String getWriteableName() {
        return RateAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return sum / divider;
    }

    @Override
    public double getValue() {
        return sum / divider;
    }

    // for testing only
    DocValueFormat format() {
        return format;
    }

    @Override
    public InternalRate reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        Double divider = null;
        for (InternalAggregation aggregation : aggregations) {
            double value = ((InternalRate) aggregation).sum;
            kahanSummation.add(value);
            if (divider == null) {
                divider = ((InternalRate) aggregation).divider;
            }
        }
        return new InternalRate(name, kahanSummation.value(), divider, format, getMetadata());
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
        return Objects.hash(super.hashCode(), sum, divider);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalRate that = (InternalRate) obj;
        return Objects.equals(sum, that.sum) && Objects.equals(divider, that.divider);
    }
}
