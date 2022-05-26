/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesRate;

import java.util.Map;

public class RateFunction implements AggregatorFunction<TimePoint, Double> {

    private final long range;
    private final long timestamp;
    private final boolean isCounter;
    private final boolean isRate;
    private TimePoint lastSample;
    private TimePoint firstSample;
    private TimePoint currentSample;
    private long count;
    private double totalRevertValue = 0;

    public RateFunction(long range, long timestamp, boolean isCounter, boolean isRate) {
        this.timestamp = timestamp;
        this.range = range;
        this.isCounter = isCounter;
        this.isRate = isRate;
    }

    @Override
    public void collect(TimePoint value) {
        count += 1;
        if (firstSample == null) {
            firstSample = value;
            lastSample = value;
            currentSample = value;
            return;
        }

        if (value.compareTo(lastSample) > 0) {
            lastSample = value;
        }
        if (value.compareTo(firstSample) < 0) {
            firstSample = value;
        }

        if (currentSample.compareTo(value) > 0 && currentSample.getValue() < value.getValue()) {
            totalRevertValue += value.getValue();
        }

        currentSample = value;
    }

    @Override
    public Double get() {
        return extrapolatedRate(range, timestamp, isCounter, isRate, lastSample, firstSample, count, totalRevertValue);
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new TimeSeriesRate(
            TimeSeriesRate.NAME,
            range,
            timestamp,
            isCounter,
            isRate,
            lastSample,
            firstSample,
            count,
            totalRevertValue,
            formatter,
            metadata
        );
    }

    public static double extrapolatedRate(
        long range,
        long timestamp,
        boolean isCounter,
        boolean isRate,
        TimePoint lastSample,
        TimePoint firstSample,
        long count,
        double totalRevertValue
    ) {
        long rangeStart = timestamp - range;
        long rangeEnd = timestamp;

        if (count < 2) {
            return Double.NaN;
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
            resultValue = resultValue / (range / 1000);
        }

        return resultValue;
    }
}
