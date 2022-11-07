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
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator;

import java.util.Map;

public class AvgBucketFunction implements AggregatorBucketFunction<Double> {
    private final BigArrays bigArrays;
    private DoubleArray sums;
    private LongArray counts;

    public AvgBucketFunction(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        sums = bigArrays.newDoubleArray(1, true);
        sums.fill(0, sums.size(), 0);
        counts = bigArrays.newLongArray(1, true);
        counts.fill(0, counts.size(), 0);
    }

    @Override
    public String name() {
        return Aggregator.avg.name();
    }

    @Override
    public void collect(Double number, long bucket) {
        if (bucket >= sums.size()) {
            long from = sums.size();
            sums = bigArrays.grow(sums, bucket + 1);
            sums.fill(from, sums.size(), 0);
            counts = bigArrays.grow(counts, bucket + 1);
            counts.fill(from, counts.size(), 0);
        }

        double current = sums.get(bucket);
        sums.set(bucket, current + number);
        counts.increment(bucket, 1);
    }

    @Override
    public InternalAggregation getAggregation(long bucket, Map<String, Object> aggregatorParams, DocValueFormat formatter, Map<String, Object> metadata) {
        return new org.elasticsearch.search.aggregations.metrics.InternalAvg(
            name(),
            sums.get(bucket),
            counts.get(bucket),
            formatter,
            metadata
        );
    }

    @Override
    public void close() {
        Releasables.close(sums, counts);
    }
}
