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
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.ValueCountBucketFunction;
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
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new ValueCountFunction();
        }

        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction() {
            return new ValueCountFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new ValueCountBucketFunction(bigArrays);
        }
    },
    sum {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new SumFunction();
        }

        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction() {
            return new SumFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new SumBucketFunction(bigArrays);
        }
    },
    min {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new MinFunction();
        }

        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction() {
            return new MinFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new MinBucketFunction(bigArrays);
        }
    },
    max {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new MaxFunction();
        }

        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction() {
            return new MaxFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new MaxBucketFunction(bigArrays);
        }
    },
    avg {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new AvgFunction();
        }

        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction() {
            return new AvgFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new AvgBucketFunction(bigArrays);
        }
    },
    last {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction();
        }
    },
    rate {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new RateFunction(aggregator.downsampleRange, aggregator.preRounding, true, true);
        }
    };

    public static Function resolve(String name) {
        return Function.valueOf(name);
    }

    /**
     * get the function
     */
    public abstract AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator);

    /**
     * get the aggregator function
     */
    public AggregatorFunction<?, ?> getAggregatorFunction() {
        throw new UnsupportedOperationException(name() + " aggregator function not support");
    };

    /**
     * get the aggregator bucket function
     */
    public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
        throw new UnsupportedOperationException(name() + " aggregator bucket function not support");
    };
}
