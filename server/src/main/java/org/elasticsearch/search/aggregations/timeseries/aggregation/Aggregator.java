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
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.SumFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;

public enum Aggregator {
    count {
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
        public AggregatorFunction<?, ?> getAggregatorFunction() {
            return new AvgFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays) {
            return new AvgBucketFunction(bigArrays);
        }
    };

    public static Aggregator resolve(String name) {
        try {
            return Aggregator.valueOf(name);
        } catch (Exception e){
            throw new IllegalArgumentException("aggregator [" + name + "] not support");
        }
    }

    /**
     * get the aggregator function
     */
    public abstract AggregatorFunction<?, ?> getAggregatorFunction();

    /**
     * get the aggregator bucket function
     */
    public abstract AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays);
}
