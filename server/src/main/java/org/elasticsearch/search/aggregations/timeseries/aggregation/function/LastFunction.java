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
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.Last;

import java.util.Map;

public class LastFunction implements AggregatorFunction<Double, Double> {
    private double last = Double.NEGATIVE_INFINITY;
    private long timestamp = Long.MIN_VALUE;

    @Override
    public void collect(Double value) {
        if (last != Double.NEGATIVE_INFINITY) {
            last = value;
        }
    }

    public void collectExact(double number, long timestamp) {
        if (timestamp > this.timestamp) {
            last = number;
        }
    }

    @Override
    public Double get() {
        return last;
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new Last("last", last, timestamp, formatter, metadata);
    }
}
