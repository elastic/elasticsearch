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
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;

import java.util.Map;

public class QuantileFunction implements AggregatorFunction<TimePoint, Double> {

    private TDigestState state;
    private double[] keys = new double[1];
    private final double compression = 100.0;

    public QuantileFunction(double quantile) {
        state = new TDigestState(compression);
        keys[0] = quantile;
    }

    @Override
    public void collect(TimePoint value) {
        state.add(value.getValue());
    }

    @Override
    public Double get() {
        return state.quantile(keys[0]);
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new InternalTDigestPercentiles(InternalTDigestPercentiles.NAME, keys, state, false, formatter, metadata);
    }
}
