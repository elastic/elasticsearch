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
import java.util.Map;

public enum Function {
    count_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new ValueCountFunction();
        }
    },
    sum_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new SumFunction();
        }
    },
    min_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new MinFunction();
        }
    },
    max_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new MaxFunction();
        }
    },
    avg_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new AvgFunction();
        }
    },
    last {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> value);
        }
    },
    rate {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new RateFunction((long)params.get(RANGE_FIELD), (long)params.get(ROUNDING_FIELD), true, true);
        }
    },
    delta {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new RateFunction((long)params.get(RANGE_FIELD), (long)params.get(ROUNDING_FIELD), false, false);
        }
    },
    increase {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new RateFunction((long)params.get(RANGE_FIELD), (long)params.get(ROUNDING_FIELD), true, false);
        }
    },
    irate {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new IRateFunction(true);
        }
    },
    idelta {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new IRateFunction(true);
        }
    },
    abs {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::abs);
        }
    },
    ceil {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::ceil);
        }
    },
    floor {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::floor);
        }
    },
    exp {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::exp);
        }
    },
    sqrt {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::sqrt);
        }
    },
    ln {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::log);
        }
    },
    log10 {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::log10);
        }
    },
    sin {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::sin);
        }
    },
    cos {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::cos);
        }
    },
    tan {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::tan);
        }
    },
    asin {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::asin);
        }
    },
    acos {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::acos);
        }
    },
    atan {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::atan);
        }
    },
    sinh {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::sinh);
        }
    },
    cosh {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::cosh);
        }
    },
    tanh {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::tanh);
        }
    },
    rad {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> value * Math.PI / 180);
        }
    },
    deg {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> value * 180 / Math.PI);
        }
    },
    pi {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> Math.PI);
        }
    },
    sgn {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
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
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new TimestampFunction();
        }
    },
    day_of_month {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getDayOfMonth);
        }
    },
    day_of_week {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(zonedDateTime -> {
                DayOfWeek dayOfWeek = zonedDateTime.getDayOfWeek();
                return dayOfWeek.getValue() % 7;
            });
        }
    },
    hour {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getHour);
        }
    },
    minute {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getMinute);
        }
    },
    month {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getMonthValue);
        }
    },
    year {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getMonthValue);
        }
    },
    clamp {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            double max = Double.MAX_VALUE;
            if (params != null && params.containsKey(MAX_FIELD)) {
                max = Double.valueOf(String.valueOf(params.get(MAX_FIELD)));
            }
            double min = Double.MIN_VALUE;
            if (params != null && params.containsKey(MIN_FIELD)) {
                min = Double.valueOf(String.valueOf(params.get(MIN_FIELD)));
            }
            return new ClampFunction(max, min);
        }
    },
    clamp_max {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            double max = Double.MAX_VALUE;
            if (params != null && params.containsKey(MAX_FIELD)) {
                max = Double.valueOf(String.valueOf(params.get(MAX_FIELD)));
            }
            return new ClampMaxFunction(max);
        }
    },
    clamp_min {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            double min = Double.MIN_VALUE;
            if (params != null && params.containsKey(MIN_FIELD)) {
                min = Double.valueOf(String.valueOf(params.get(MIN_FIELD)));
            }
            return new ClampMinFunction(min);
        }
    },
    quantile_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            double quantile = 1f;
            if (params != null && params.containsKey(QUANTILE_FIELD)) {
                quantile = (double) params.get(QUANTILE_FIELD);
            }
            return new QuantileFunction(quantile);
        }
    },
    last_over_time {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> value);
        }
    };

    public static final String RANGE_FIELD = "range";
    public static final String ROUNDING_FIELD = "rounding";
    public static final String MAX_FIELD = "max";
    public static final String MIN_FIELD = "min";
    public static final String QUANTILE_FIELD = "quantile";

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
    public abstract AggregatorFunction<?, ?> getFunction(Map<String, Object> params);
}
