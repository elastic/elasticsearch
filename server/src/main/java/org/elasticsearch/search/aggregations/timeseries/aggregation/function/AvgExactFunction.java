/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;

import java.util.Map;

public class AvgExactFunction implements AggregatorFunction<Tuple<Double, Long>, Double> {
    private double sum = 0;
    private long count = 0;

    @Override
    public void collect(Tuple<Double, Long> value) {
        this.sum += value.v1();
        this.count += value.v2();
    }

    @Override
    public Double get() {
        return sum / count;
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new org.elasticsearch.search.aggregations.metrics.InternalAvg(AvgAggregationBuilder.NAME, sum, count, formatter, metadata);
    }
}
