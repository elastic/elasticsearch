/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesLast;

import java.util.Map;

public class NoAggregatorBucketFunction implements AggregatorBucketFunction<Double> {
    private double value;

    @Override
    public String name() {
        return "no_aggregator";
    }

    @Override
    public void collect(Double number, long bucket) {
        value = number;
    }

    @Override
    public InternalAggregation getAggregation(long bucket, Map<String, Object> aggregatorParams, DocValueFormat formatter, Map<String, Object> metadata) {
        return new TimeSeriesLast(name(), value, 0, formatter, metadata);
    }

    @Override
    public void close() {

    }
}
