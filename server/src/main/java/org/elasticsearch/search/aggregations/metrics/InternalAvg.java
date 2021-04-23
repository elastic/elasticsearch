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

public class InternalAvg extends InternalNumericMetricsAggregation.SingleValue implements Avg {
    private final double sum;
    private final long count;

    public InternalAvg(String name, double sum, long count, DocValueFormat format, Map<String, Object> metadata) {
        super(name, metadata);
        this.sum = sum;
        this.count = count;
        this.format = format;
    }

    /**
     * Read from a stream.
     */
    public InternalAvg(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        sum = in.readDouble();
        count = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
        out.writeVLong(count);
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public double getValue() {
        return sum / count;
    }

    double getSum() {
        return sum;
    }

    long getCount() {
        return count;
    }

    DocValueFormat getFormatter() {
        return format;
    }

    @Override
    public String getWriteableName() {
        return AvgAggregationBuilder.NAME;
    }

    @Override
    public InternalAvg reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        long count = 0;
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        for (InternalAggregation aggregation : aggregations) {
            InternalAvg avg = (InternalAvg) aggregation;
            count += avg.count;
            kahanSummation.add(avg.sum);
        }
        return new InternalAvg(getName(), kahanSummation.value(), count, format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), count != 0 ? getValue() : null);
        if (count != 0 && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(getValue()).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sum, count, format.getWriteableName());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalAvg other = (InternalAvg) obj;
        return Objects.equals(sum, other.sum) &&
                Objects.equals(count, other.count) &&
                Objects.equals(format.getWriteableName(), other.format.getWriteableName());
    }
}
