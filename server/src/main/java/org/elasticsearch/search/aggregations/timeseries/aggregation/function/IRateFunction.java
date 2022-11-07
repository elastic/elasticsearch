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
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesIRate;

import java.util.Map;

public class IRateFunction implements AggregatorFunction<TimePoint, Double> {

    private final boolean isRate;
    private TimePoint lastSample;
    private TimePoint previousSample;
    private long count;

    public IRateFunction(boolean isRate) {
        this.isRate = isRate;
    }

    @Override
    public void collect(TimePoint value) {
        count += 1;
        if (lastSample == null) {
            lastSample = value;
            return;
        }

        if (previousSample == null) {
            previousSample = value;
            return;
        }
    }

    @Override
    public Double get() {
        return instantValue(isRate, lastSample, previousSample, count);
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new TimeSeriesIRate(TimeSeriesIRate.NAME, isRate, lastSample, previousSample, count, formatter, metadata);
    }

    public static double instantValue(boolean isRate, TimePoint lastSample, TimePoint previousSample, long count) {
        if (count < 2) {
            return Double.NaN;
        }

        double resultValue;
        if (isRate && lastSample.getValue() < previousSample.getValue()) {
            resultValue = lastSample.getValue();
        } else {
            resultValue = lastSample.getValue() - previousSample.getValue();
        }

        long sampledInterval = lastSample.getTimestamp() - previousSample.getTimestamp();
        if (sampledInterval == 0) {
            return Double.NaN;
        }

        if (isRate) {
            resultValue /= (double) sampledInterval / 1000;
        }

        return resultValue;
    }
}
