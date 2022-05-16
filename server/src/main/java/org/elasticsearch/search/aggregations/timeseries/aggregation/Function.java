/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AvgFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.IRateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.LastFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.RateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.SumFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;

public enum Function {
    count_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new ValueCountFunction();
        }
    },
    sum_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new SumFunction();
        }
    },
    min_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new MinFunction();
        }
    },
    max_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new MaxFunction();
        }
    },
    avg_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new AvgFunction();
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
    },
    delta {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new RateFunction(aggregator.downsampleRange, aggregator.preRounding, false, false);
        }
    },
    increase {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new RateFunction(aggregator.downsampleRange, aggregator.preRounding, true, false);
        }
    },
    irate {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new IRateFunction(true);
        }
    },
    idelta {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new IRateFunction(true);
        }
    };

    public static Function resolve(String name) {
        try {
            return Function.valueOf(name);
        } catch (Exception e) {
            throw new IllegalArgumentException("function [" + name + "] not support");
        }
    }

    /**
     * get the function
     */
    public abstract AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator);
}
