/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.internal;

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

public class TimeSeriesLast extends InternalNumericMetricsAggregation.SingleValue {
    public static final String NAME = "time_series_last";

    private final double last;
    private final long timestamp;

    public TimeSeriesLast(String name, double last, long timestamp, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.last = last;
        this.timestamp = timestamp;
    }

    /**
     * Read from a stream.
     */
    public TimeSeriesLast(StreamInput in) throws IOException {
        super(in);
        last = in.readDouble();
        timestamp = in.readLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(last);
        out.writeLong(timestamp);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    public double value() {
        return last;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public TimeSeriesLast reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        double last = Double.NEGATIVE_INFINITY;
        long timestamp = Long.MIN_VALUE;
        for (InternalAggregation aggregation : aggregations) {
            if (((TimeSeriesLast) aggregation).timestamp > timestamp) {
                last = ((TimeSeriesLast) aggregation).last;
                timestamp = ((TimeSeriesLast) aggregation).timestamp;
            }
        }
        return new TimeSeriesLast(name, last, timestamp, format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isInfinite(last) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? last : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(last).toString());
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (false == super.equals(o)) {
            return false;
        }
        TimeSeriesLast last1 = (TimeSeriesLast) o;
        return Double.compare(last1.last, last) == 0 && timestamp == last1.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), last, timestamp);
    }
}
