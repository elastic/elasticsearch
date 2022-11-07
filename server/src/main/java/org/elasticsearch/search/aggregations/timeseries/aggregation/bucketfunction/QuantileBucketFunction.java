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
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator;

import java.util.Map;

public class QuantileBucketFunction implements AggregatorBucketFunction<Double> {

    private final BigArrays bigArrays;
    private ObjectArray<TDigestState> states;
    private double[] keys = new double[1];
    private final double compression = 100.0;

    public QuantileBucketFunction(BigArrays bigArrays, double quantile) {
        this.bigArrays = bigArrays;
        states = bigArrays.newObjectArray(1);
        keys[0] = quantile;
    }

    @Override
    public String name() {
        return Aggregator.quantile.name();
    }

    @Override
    public void collect(Double number, long bucket) {
        states = bigArrays.grow(states, bucket + 1);
        TDigestState state = states.get(bucket);
        if (state == null) {
            state = new TDigestState(compression);
            states.set(bucket, state);
        }
        state.add(number);
    }

    @Override
    public InternalAggregation getAggregation(
        long bucket,
        Map<String, Object> aggregatorParams,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        return new InternalTDigestPercentiles(InternalTDigestPercentiles.NAME, keys, states.get(bucket), false, formatter, metadata);
    }

    @Override
    public void close() {
        Releasables.close(states);
    }
}
