/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator;

import java.util.Map;

public class MinBucketFunction implements AggregatorBucketFunction<Double> {
    private final BigArrays bigArrays;
    private DoubleArray value;

    public MinBucketFunction(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        value = bigArrays.newDoubleArray(1, true);
        value.fill(0, value.size(), Double.POSITIVE_INFINITY);
    }

    @Override
    public String name() {
        return Aggregator.min.name();
    }

    @Override
    public void collect(Double number, long bucket) {
        if (bucket >= value.size()) {
            long from = value.size();
            value = bigArrays.grow(value, bucket + 1);
            value.fill(from, value.size(), Double.POSITIVE_INFINITY);
        }

        double current = value.get(bucket);
        value.set(bucket, Math.min(current, number));
    }

    @Override
    public InternalAggregation getAggregation(long bucket, Map<String, Object> aggregatorParams, DocValueFormat formatter, Map<String, Object> metadata) {
        return new org.elasticsearch.search.aggregations.metrics.Min(name(), value.get(bucket), formatter, metadata);
    }

    @Override
    public void close() {
        Releasables.close(value);
    }
}
