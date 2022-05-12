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
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.SumBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AvgFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.LastFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.RateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.SumFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;

public enum Function {
    count {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(TimeSeriesAggregationAggregator aggregator) {
            return new ValueCountFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            throw new UnsupportedOperationException("count aggregator bucket function not support");
        }
    },
    sum {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(TimeSeriesAggregationAggregator aggregator) {
            return new SumFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new SumBucketFunction(bigArrays);
        }
    },
    min {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(TimeSeriesAggregationAggregator aggregator) {
            return new MinFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new MinBucketFunction(bigArrays);
        }
    },
    max {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(TimeSeriesAggregationAggregator aggregator) {
            return new MaxFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new MaxBucketFunction(bigArrays);
        }
    },
    avg {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(TimeSeriesAggregationAggregator aggregator) {
            return new AvgFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new AvgBucketFunction(bigArrays);
        }
    },
    last {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            throw new UnsupportedOperationException("last aggregator bucket function not support");
        }
    },
    rate {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(TimeSeriesAggregationAggregator aggregator) {
            return new RateFunction(0, 0, true, true);
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            throw new UnsupportedOperationException("last aggregator bucket function not support");
        }
    };

    public static Function resolve(String name) {
        return Function.valueOf(name);
    }

    /**
     * get the aggregator function
     */
    public abstract AggregatorFunction<?, ?> getAggregatorFunction(TimeSeriesAggregationAggregator aggregator);

    /**
     * get the aggregator bucket function
     */
    public abstract AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays);
}
