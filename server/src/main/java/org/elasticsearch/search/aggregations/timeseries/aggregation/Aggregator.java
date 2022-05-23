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
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.CountValuesBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.MaxBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.MinBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.SumBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.TopkBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.ValueCountBucketFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AvgFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.CountValuesFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.SumFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.TopkFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;

import java.util.Map;

public enum Aggregator {
    count {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams) {
            return new ValueCountFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays, Map<String, Object> aggregatorParams) {
            return new ValueCountBucketFunction(bigArrays);
        }
    },
    sum {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams) {
            return new SumFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays, Map<String, Object> aggregatorParams) {
            return new SumBucketFunction(bigArrays);
        }
    },
    min {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams) {
            return new MinFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays, Map<String, Object> aggregatorParams) {
            return new MinBucketFunction(bigArrays);
        }
    },
    max {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams) {
            return new MaxFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays, Map<String, Object> aggregatorParams) {
            return new MaxBucketFunction(bigArrays);
        }
    },
    avg {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams) {
            return new AvgFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays, Map<String, Object> aggregatorParams) {
            return new AvgBucketFunction(bigArrays);
        }
    },
    topk {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams) {
            int size = 0;
            if (aggregatorParams != null && aggregatorParams.containsKey("size")) {
                size = (int) aggregatorParams.get("size");
            }
            return new TopkFunction(size, true);
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays, Map<String, Object> aggregatorParams) {
            int size = 0;
            if (aggregatorParams != null && aggregatorParams.containsKey("size")) {
                size = (int) aggregatorParams.get("size");
            }
            return new TopkBucketFunction(size, true);
        }
    },
    bottomk {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams) {
            int size = 0;
            if (aggregatorParams != null && aggregatorParams.containsKey("size")) {
                size = (int) aggregatorParams.get("size");
            }
            return new TopkFunction(size, false);
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays,
            Map<String, Object> aggregatorParams) {
            int size = 0;
            if (aggregatorParams != null && aggregatorParams.containsKey("size")) {
                size = (int) aggregatorParams.get("size");
            }
            return new TopkBucketFunction(size, false);
        }
    },
    count_values {
        @Override
        public AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams) {
            return new CountValuesFunction();
        }

        @Override
        public AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays,
            Map<String, Object> aggregatorParams) {
            return new CountValuesBucketFunction(bigArrays);
        }
    };

    public static Aggregator resolve(String name) {
        try {
            return Aggregator.valueOf(name);
        } catch (Exception e) {
            throw new IllegalArgumentException("aggregator [" + name + "] not support");
        }
    }

    /**
     * get the aggregator function
     */
    public abstract AggregatorFunction<?, ?> getAggregatorFunction(Map<String, Object> aggregatorParams);

    /**
     * get the aggregator bucket function
     */
    public abstract AggregatorBucketFunction<?> getAggregatorBucketFunction(BigArrays bigArrays, Map<String, Object> aggregatorParams);
}
