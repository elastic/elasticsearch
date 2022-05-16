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
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.IRateFunction;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TimeSeriesIRate extends InternalNumericMetricsAggregation.SingleValue implements Comparable<TimeSeriesIRate> {
    public static final String NAME = "time_series_irate";

    private final boolean isRate;
    private TimePoint lastSample;
    private TimePoint previousSample;
    private long count;
    private double resultValue = -1d;

    public TimeSeriesIRate(
        String name,
        boolean isRate,
        TimePoint lastSample,
        TimePoint previousSample,
        long count,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, formatter, metadata);
        this.isRate = isRate;
        this.lastSample = lastSample;
        this.previousSample = previousSample;
        this.count = count;
    }

    /**
     * Read from a stream.
     */
    public TimeSeriesIRate(StreamInput in) throws IOException {
        super(in);
        isRate = in.readBoolean();
        lastSample = in.readOptionalWriteable(TimePoint::new);
        previousSample = in.readOptionalWriteable(TimePoint::new);
        count = in.readLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(isRate);
        out.writeOptionalWriteable(lastSample);
        out.writeOptionalWriteable(previousSample);
        out.writeLong(count);
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
        if (resultValue < 0) {
            resultValue = IRateFunction.instantValue(isRate, lastSample, previousSample, count);
        }
        return resultValue;
    }

    @Override
    public TimeSeriesIRate reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        if (aggregations.size() == 1) {
            return (TimeSeriesIRate) aggregations.get(0);
        }

        List<TimeSeriesIRate> timeSeriesIRates = aggregations.stream().map(c -> (TimeSeriesIRate) c).sorted().collect(Collectors.toList());

        TimeSeriesIRate reduced = timeSeriesIRates.get(0);
        for (int i = 1; i < timeSeriesIRates.size(); i++) {
            TimeSeriesIRate timeSeriesIRate = timeSeriesIRates.get(i);
            if (timeSeriesIRate.count == 0) {
                continue;
            }
            reduced.count += timeSeriesIRate.count;
            if (timeSeriesIRate.count == 1) {
                reduced.previousSample = reduced.lastSample;
            } else {
                reduced.previousSample = timeSeriesIRate.previousSample;
            }
            reduced.lastSample = timeSeriesIRate.lastSample;
        }
        return reduced;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isInfinite(value()) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value() : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(value()).toString());
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
        if (!super.equals(o)) {
            return false;
        }
        TimeSeriesIRate that = (TimeSeriesIRate) o;
        return isRate == that.isRate
            && count == that.count
            && Double.compare(that.resultValue, resultValue) == 0
            && Objects.equals(lastSample, that.lastSample)
            && Objects.equals(previousSample, that.previousSample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isRate, lastSample, previousSample, count, resultValue);
    }

    @Override
    public int compareTo(TimeSeriesIRate o) {
        return lastSample.compareTo(o.lastSample);
    }
}
