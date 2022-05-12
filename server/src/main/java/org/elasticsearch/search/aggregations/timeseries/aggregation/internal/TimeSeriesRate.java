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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TimeSeriesRate extends InternalNumericMetricsAggregation.SingleValue implements Comparable<TimeSeriesRate> {
    public static final String NAME = "rate";

    private final long range;
    private final long timestamp;
    private final boolean isCounter;
    private final boolean isRate;
    private TimePoint lastSample;
    private TimePoint firstSample;
    private long count;
    private double totalRevertValue;
    private double resultValue = -1d;

    public TimeSeriesRate(
        String name,
        long range,
        long timestamp,
        boolean isCounter,
        boolean isRate,
        TimePoint lastSample,
        TimePoint firstSample,
        long count,
        double totalRevertValue,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, formatter, metadata);
        this.range = range;
        this.timestamp = timestamp;
        this.isCounter = isCounter;
        this.isRate = isRate;
        this.lastSample = lastSample;
        this.firstSample = firstSample;
        this.count = count;
        this.totalRevertValue = totalRevertValue;
    }

    /**
     * Read from a stream.
     */
    public TimeSeriesRate(StreamInput in) throws IOException {
        super(in);
        range = in.readLong();
        timestamp = in.readLong();
        isCounter = in.readBoolean();
        isRate = in.readBoolean();
        lastSample = new TimePoint(in);
        firstSample = new TimePoint(in);
        count = in.readLong();
        totalRevertValue = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeLong(range);
        out.writeLong(timestamp);
        out.writeBoolean(isCounter);
        out.writeBoolean(isRate);
        lastSample.writeTo(out);
        firstSample.writeTo(out);
        out.writeLong(count);
        out.writeDouble(totalRevertValue);
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
            resultValue = calc();
        }
        return resultValue;
    }

    public double calc() {
        long rangeStart = timestamp - range;
        long rangeEnd = timestamp;

        if (count < 2) {
            return 0d;
        }

        double resultValue = lastSample.getValue() - firstSample.getValue();
        if (isCounter) {
            resultValue += totalRevertValue;
        }

        double durationToStart = (firstSample.getTimestamp() - rangeStart) / 1000;
        double durationToEnd = (rangeEnd - lastSample.getTimestamp()) / 1000;

        double sampledInterval = (lastSample.getTimestamp() - firstSample.getTimestamp()) / 1000;
        double averageDurationBetweenSamples = sampledInterval / (count - 1);

        if (isCounter && resultValue > 0 && firstSample.getValue() >= 0) {
            double durationToZero = sampledInterval * (firstSample.getValue() / resultValue);
            if (durationToZero < durationToStart) {
                durationToStart = durationToZero;
            }
        }

        double extrapolationThreshold = averageDurationBetweenSamples * 1.1;
        double extrapolateToInterval = sampledInterval;

        if (durationToStart < extrapolationThreshold) {
            extrapolateToInterval += durationToStart;
        } else {
            extrapolateToInterval += averageDurationBetweenSamples / 2;
        }
        if (durationToEnd < extrapolationThreshold) {
            extrapolateToInterval += durationToEnd;
        } else {
            extrapolateToInterval += averageDurationBetweenSamples / 2;
        }
        resultValue = resultValue * (extrapolateToInterval / sampledInterval);
        if (isRate) {
            resultValue = resultValue / range;
        }

        return resultValue;
    }

    @Override
    public TimeSeriesRate reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        if (aggregations.size() == 1) {
            return (TimeSeriesRate)aggregations.get(0);
        }

        List<TimeSeriesRate> timeSeriesRates = aggregations.stream().map(c->(TimeSeriesRate)c).sorted().collect(Collectors.toList());

        TimeSeriesRate reduced = timeSeriesRates.get(0);
        for (int i = 1; i < timeSeriesRates.size(); i++) {
            TimeSeriesRate timeSeriesRate = timeSeriesRates.get(i);
            reduced.count += timeSeriesRate.count;
            reduced.lastSample = timeSeriesRate.lastSample;
            reduced.totalRevertValue += timeSeriesRate.totalRevertValue;
            if (timeSeriesRate.firstSample.getValue() < reduced.lastSample.getValue()) {
                reduced.totalRevertValue += reduced.lastSample.getValue() - timeSeriesRate.firstSample.getValue();
            }
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
        TimeSeriesRate rate = (TimeSeriesRate) o;
        return range == rate.range
            && timestamp == rate.timestamp
            && isCounter == rate.isCounter
            && isRate == rate.isRate
            && count == rate.count
            && Double.compare(rate.totalRevertValue, totalRevertValue) == 0
            && Objects.equals(lastSample, rate.lastSample)
            && Objects.equals(firstSample, rate.firstSample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), range, timestamp, isCounter, isRate, lastSample, firstSample, count, totalRevertValue);
    }

    @Override
    public int compareTo(TimeSeriesRate o) {
        return firstSample.compareTo(o.firstSample);
    }
}
