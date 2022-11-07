/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator;

import java.util.Map;

public class ValueCountBucketFunction implements AggregatorBucketFunction<Double> {
    private final BigArrays bigArrays;
    private LongArray counts;

    public ValueCountBucketFunction(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        counts = bigArrays.newLongArray(1, true);
        counts.fill(0, counts.size(), 0);
    }

    @Override
    public String name() {
        return Aggregator.count.name();
    }

    @Override
    public void collect(Double number, long bucket) {
        if (bucket >= counts.size()) {
            long from = counts.size();
            counts = bigArrays.grow(counts, bucket + 1);
            counts.fill(from, counts.size(), 0);
        }

        counts.increment(bucket, 1);
    }

    @Override
    public InternalAggregation getAggregation(long bucket, Map<String, Object> aggregatorParams, DocValueFormat formatter, Map<String, Object> metadata) {
        return new InternalValueCount(name(), counts.get(bucket), metadata);
    }

    @Override
    public void close() {
        Releasables.close(counts);
    }
}
