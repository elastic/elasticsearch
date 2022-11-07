/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AggregatorFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AvgExactFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.AvgFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ClampFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ClampMaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ClampMinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.DateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.IRateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.LastFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MaxFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.MinFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.OriginValuesFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.QuantileFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.RateFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.SumFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.TimestampFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountExactFunction;
import org.elasticsearch.search.aggregations.timeseries.aggregation.function.ValueCountFunction;

import java.time.DayOfWeek;
import java.time.ZonedDateTime;
import java.util.Map;

public enum Function {
    count_over_time(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new ValueCountFunction();
        }
    },
    count_exact_over_time(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new ValueCountExactFunction();
        }
    },
    sum_over_time(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new SumFunction();
        }
    },
    min_over_time(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new MinFunction();
        }
    },
    max_over_time(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new MaxFunction();
        }
    },
    avg_over_time(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new AvgFunction();
        }
    },
    avg_exact_over_time(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new AvgExactFunction();
        }
    },
    last(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> value);
        }
    },
    origin_value(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new OriginValuesFunction();
        }
    },
    rate(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new RateFunction((long) params.get(RANGE_FIELD), (long) params.get(ROUNDING_FIELD), true, true);
        }
    },
    delta(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new RateFunction((long) params.get(RANGE_FIELD), (long) params.get(ROUNDING_FIELD), false, false);
        }
    },
    increase(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new RateFunction((long) params.get(RANGE_FIELD), (long) params.get(ROUNDING_FIELD), true, false);
        }
    },
    irate(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new IRateFunction(true);
        }
    },
    idelta(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new IRateFunction(true);
        }
    },
    abs(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::abs);
        }
    },
    ceil(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::ceil);
        }
    },
    floor(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::floor);
        }
    },
    exp(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::exp);
        }
    },
    sqrt(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::sqrt);
        }
    },
    ln(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::log);
        }
    },
    log10(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::log10);
        }
    },
    sin(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::sin);
        }
    },
    cos(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::cos);
        }
    },
    tan(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::tan);
        }
    },
    asin(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::asin);
        }
    },
    acos(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::acos);
        }
    },
    atan(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::atan);
        }
    },
    sinh(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::sinh);
        }
    },
    cosh(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::cosh);
        }
    },
    tanh(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(Math::tanh);
        }
    },
    rad(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> value * Math.PI / 180);
        }
    },
    deg(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> value * 180 / Math.PI);
        }
    },
    pi(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new LastFunction(value -> Math.PI);
        }
    },
    sgn(ValueType.vector) {
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
    timestamp(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new TimestampFunction();
        }
    },
    day_of_month(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getDayOfMonth);
        }
    },
    day_of_week(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(zonedDateTime -> {
                DayOfWeek dayOfWeek = zonedDateTime.getDayOfWeek();
                return dayOfWeek.getValue() % 7;
            });
        }
    },
    hour(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getHour);
        }
    },
    minute(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getMinute);
        }
    },
    month(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getMonthValue);
        }
    },
    year(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            return new DateFunction(ZonedDateTime::getMonthValue);
        }
    },
    clamp(ValueType.vector) {
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
    clamp_max(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            double max = Double.MAX_VALUE;
            if (params != null && params.containsKey(MAX_FIELD)) {
                max = Double.valueOf(String.valueOf(params.get(MAX_FIELD)));
            }
            return new ClampMaxFunction(max);
        }
    },
    clamp_min(ValueType.vector) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            double min = Double.MIN_VALUE;
            if (params != null && params.containsKey(MIN_FIELD)) {
                min = Double.valueOf(String.valueOf(params.get(MIN_FIELD)));
            }
            return new ClampMinFunction(min);
        }
    },
    quantile_over_time(ValueType.matrix) {
        @Override
        public AggregatorFunction<?, ?> getFunction(Map<String, Object> params) {
            double quantile = 1f;
            if (params != null && params.containsKey(QUANTILE_FIELD)) {
                quantile = (double) params.get(QUANTILE_FIELD);
            }
            return new QuantileFunction(quantile);
        }
    },
    last_over_time(ValueType.matrix) {
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

    private ValueType valueType;

    Function(ValueType valueType) {
        this.valueType = valueType;
    }

    public ValueType getValueType() {
        return valueType;
    }

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

    public enum ValueType {
        vector,
        matrix
    }
}
