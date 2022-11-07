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
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;

import java.util.Map;

public class SumFunction implements AggregatorFunction<TimePoint, Double> {
    private double sum = 0;

    @Override
    public void collect(TimePoint value) {
        this.sum += value.getValue();
    }

    @Override
    public Double get() {
        return sum;
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new org.elasticsearch.search.aggregations.metrics.Sum(SumAggregationBuilder.NAME, sum, formatter, metadata);
    }
}
