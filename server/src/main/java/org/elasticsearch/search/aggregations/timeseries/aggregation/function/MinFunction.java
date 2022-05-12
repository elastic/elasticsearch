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
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;

import java.util.Map;

public class MinFunction implements AggregatorFunction<TimePoint, Double> {
    private Double min = Double.POSITIVE_INFINITY;

    @Override
    public void collect(TimePoint value) {
        this.min = Math.min(value.getValue(), min);
    }

    @Override
    public Double get() {
        return min;
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new org.elasticsearch.search.aggregations.metrics.Min(MinAggregationBuilder.NAME, min, formatter, metadata);
    }
}
