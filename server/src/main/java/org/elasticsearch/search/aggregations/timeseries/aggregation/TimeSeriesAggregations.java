/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.AggregatorBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.AvgBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.MaxBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.MinBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.NoAggregatorBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.SumBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AvgFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.SumFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;

public class TimeSeriesAggregations {
    public static AggregatorFunction getAggregatorFunction(TimeSeriesAggregation.Function function) {
        switch (function) {
            case avg:
                return new AvgFunction();
            case max:
                return new MaxFunction();
            case min:
                return new MinFunction();
            case sum:
                return new SumFunction();
            case count:
                return new ValueCountFunction();
        }
        return new AvgFunction();
    }

    public static AggregatorBucketFunction getAggregatorBucketFunction(TimeSeriesAggregation.Aggregator aggregator, BigArrays bigArrays) {
        if (aggregator == null) {
            return new NoAggregatorBucketFunction();
        }

        switch (aggregator) {
            case max:
                return new MaxBucketFunction(bigArrays);
            case min:
                return new MinBucketFunction(bigArrays);
            case sum:
                return new SumBucketFunction(bigArrays);
            case avg:
                return new AvgBucketFunction(bigArrays);
        }
        return new AvgBucketFunction(bigArrays);
    }
}
