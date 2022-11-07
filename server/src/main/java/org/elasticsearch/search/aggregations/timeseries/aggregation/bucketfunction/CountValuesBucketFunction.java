/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesCountValues;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CountValuesBucketFunction implements AggregatorBucketFunction<Double> {

    private ObjectArray<Map<Long, AtomicInteger>> values;
    private BigArrays bigArrays;

    public CountValuesBucketFunction(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        this.values = bigArrays.newObjectArray(1);
    }

    @Override
    public String name() {
        return Aggregator.count_values.name();
    }

    @Override
    public void collect(Double number, long bucket) {
        values = bigArrays.grow(values, bucket + 1);
        long value = java.lang.Double.doubleToLongBits(number);
        Map<Long, AtomicInteger> valueCount = values.get(bucket);
        if (valueCount == null) {
            valueCount = new HashMap<>();
            values.set(bucket, valueCount);
        }

        AtomicInteger count = valueCount.get(value);
        if (count == null) {
            count = new AtomicInteger(0);
            valueCount.put(value, count);
        }
        count.incrementAndGet();
    }

    @Override
    public InternalAggregation getAggregation(
        long bucket,
        Map<String, Object> aggregatorParams,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        return new TimeSeriesCountValues(name(), values.get(bucket), formatter, metadata);
    }

    @Override
    public void close() {
        Releasables.close(values);
    }
}
