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
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ClampFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ClampMaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ClampMinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.DateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.IRateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.LastFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.QuantileFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.RateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.SumFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.TimestampFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;

import java.time.DayOfWeek;
import java.time.ZonedDateTime;

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
            return new LastFunction(value -> value);
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
    },
    abs {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::abs);
        }
    },
    ceil {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::ceil);
        }
    },
    floor {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::floor);
        }
    },
    exp {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::exp);
        }
    },
    sqrt {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::sqrt);
        }
    },
    ln {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::log);
        }
    },
    log10 {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::log10);
        }
    },
    sin {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::sin);
        }
    },
    cos {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::cos);
        }
    },
    tan {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::tan);
        }
    },
    asin {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::asin);
        }
    },
    acos {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::acos);
        }
    },
    atan {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::atan);
        }
    },
    sinh {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::sinh);
        }
    },
    cosh {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::cosh);
        }
    },
    tanh {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(Math::tanh);
        }
    },
    rad {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(value -> value * Math.PI / 180);
        }
    },
    deg {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(value -> value * 180 / Math.PI);
        }
    },
    pi {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(value -> Math.PI);
        }
    },
    sgn {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(v -> {
                if (v < 0) {
                    return -1d;
                } else if (v > 0) {
                    return 1d;
                }
                return v;
            });
        }
    },
    timestamp {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new TimestampFunction();
        }
    },
    day_of_month {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new DateFunction(ZonedDateTime::getDayOfMonth);
        }
    },
    day_of_week {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new DateFunction(zonedDateTime -> {
                DayOfWeek dayOfWeek = zonedDateTime.getDayOfWeek();
                return dayOfWeek.getValue() % 7;
            });
        }
    },
    hour {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new DateFunction(ZonedDateTime::getHour);
        }
    },
    minute {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new DateFunction(ZonedDateTime::getMinute);
        }
    },
    month {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new DateFunction(ZonedDateTime::getMonthValue);
        }
    },
    year {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new DateFunction(ZonedDateTime::getMonthValue);
        }
    },
    clamp {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            double max = Double.MAX_VALUE;
            if (aggregator.downsampleParams != null && aggregator.downsampleParams.containsKey("max")) {
                max = Double.valueOf(String.valueOf(aggregator.downsampleParams.get("max")));
            }
            double min = Double.MIN_VALUE;
            if (aggregator.downsampleParams != null && aggregator.downsampleParams.containsKey("min")) {
                min = Double.valueOf(String.valueOf(aggregator.downsampleParams.get("min")));
            }
            return new ClampFunction(max, min);
        }
    },
    clamp_max {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            double max = Double.MAX_VALUE;
            if (aggregator.downsampleParams != null && aggregator.downsampleParams.containsKey("max")) {
                max = Double.valueOf(String.valueOf(aggregator.downsampleParams.get("max")));
            }
            return new ClampMaxFunction(max);
        }
    },
    clamp_min {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            double min = Double.MIN_VALUE;
            if (aggregator.downsampleParams != null && aggregator.downsampleParams.containsKey("min")) {
                min = Double.valueOf(String.valueOf(aggregator.downsampleParams.get("min")));
            }
            return new ClampMinFunction(min);
        }
    },
    quantile_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            double quantile = 1f;
            if (aggregator.downsampleParams != null && aggregator.downsampleParams.containsKey("quantile")) {
                quantile = (double) aggregator.downsampleParams.get("quantile");
            }
            return new QuantileFunction(quantile);
        }
    },
    last_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(TimeSeriesAggregationAggregator aggregator) {
            return new LastFunction(value -> value);
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
