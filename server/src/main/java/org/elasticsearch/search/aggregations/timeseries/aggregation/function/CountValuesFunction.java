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
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesCountValues;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CountValuesFunction implements AggregatorFunction<TimePoint, Map<Long, AtomicInteger>> {

    private Map<Long, AtomicInteger> valueCount;

    public CountValuesFunction() {
        valueCount = new HashMap<>();
    }

    @Override
    public void collect(TimePoint value) {
        long val = java.lang.Double.doubleToLongBits(value.getValue());
        AtomicInteger count = valueCount.get(val);
        if (count == null) {
            count = new AtomicInteger(0);
            valueCount.put(val, count);
        }
        count.incrementAndGet();
    }

    @Override
    public Map<Long, AtomicInteger> get() {
        return valueCount;
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new TimeSeriesCountValues(TimeSeriesCountValues.NAME, valueCount, formatter, metadata);
    }
}
