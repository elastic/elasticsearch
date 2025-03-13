/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalResetTrackingRate extends InternalNumericMetricsAggregation.SingleValue implements Rate {

    public static final String NAME = "rate_with_resets";
    private static final int MILLIS_IN_SECOND = 1_000;

    private final double startValue;
    private final double endValue;
    private final long startTime;
    private final long endTime;
    private final double resetCompensation;

    private final Rounding.DateTimeUnit rateUnit;

    protected InternalResetTrackingRate(
        String name,
        DocValueFormat format,
        Map<String, Object> metadata,
        double startValue,
        double endValue,
        long startTime,
        long endTime,
        double resetCompensation,
        Rounding.DateTimeUnit rateUnit
    ) {
        super(name, format, metadata);
        this.startValue = startValue;
        this.endValue = endValue;
        this.startTime = startTime;
        this.endTime = endTime;
        this.resetCompensation = resetCompensation;
        this.rateUnit = Objects.requireNonNull(rateUnit);
    }

    public InternalResetTrackingRate(StreamInput in) throws IOException {
        super(in, false);
        this.startValue = in.readDouble();
        this.endValue = in.readDouble();
        this.startTime = in.readLong();
        this.endTime = in.readLong();
        this.resetCompensation = in.readDouble();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            this.rateUnit = Rounding.DateTimeUnit.resolve(in.readByte());
        } else {
            this.rateUnit = Rounding.DateTimeUnit.SECOND_OF_MINUTE;
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(startValue);
        out.writeDouble(endValue);
        out.writeLong(startTime);
        out.writeLong(endTime);
        out.writeDouble(resetCompensation);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X) && rateUnit != null) {
            out.writeByte(rateUnit.getId());
        } else {
            out.writeByte(Rounding.DateTimeUnit.SECOND_OF_MINUTE.getId());
        }
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        final List<InternalResetTrackingRate> aggregations = new ArrayList<>(size);
        return new AggregatorReducer() {
            @Override
            public void accept(InternalAggregation aggregation) {
                aggregations.add((InternalResetTrackingRate) aggregation);
            }

            @Override
            public InternalAggregation get() {
                List<InternalResetTrackingRate> toReduce = aggregations.stream()
                    .sorted(Comparator.comparingLong(o -> o.startTime))
                    .toList();
                double resetComp = toReduce.get(0).resetCompensation;
                double startValue = toReduce.get(0).startValue;
                double endValue = toReduce.get(0).endValue;
                final int endIndex = toReduce.size() - 1;
                for (int i = 1; i < endIndex + 1; i++) {
                    InternalResetTrackingRate rate = toReduce.get(i);
                    assert rate.startTime >= toReduce.get(i - 1).endTime;
                    resetComp += rate.resetCompensation;
                    if (endValue > rate.startValue) {
                        resetComp += endValue;
                    }
                    endValue = rate.endValue;
                }
                return new InternalResetTrackingRate(
                    name,
                    format,
                    metadata,
                    startValue,
                    endValue,
                    toReduce.get(0).startTime,
                    toReduce.get(endIndex).endTime,
                    resetComp,
                    toReduce.get(0).rateUnit
                );
            }
        };
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CommonFields.VALUE.getPreferredName(), value());
    }

    @Override
    public double value() {
        long rateUnitSeconds = rateUnit.getField().getBaseUnit().getDuration().toSeconds();
        return (endValue - startValue + resetCompensation) / (endTime - startTime) * MILLIS_IN_SECOND * rateUnitSeconds;
    }

    @Override
    public double getValue() {
        return value();
    }

    boolean includes(InternalResetTrackingRate other) {
        return this.startTime < other.startTime && this.endTime > other.endTime;
    }
}
