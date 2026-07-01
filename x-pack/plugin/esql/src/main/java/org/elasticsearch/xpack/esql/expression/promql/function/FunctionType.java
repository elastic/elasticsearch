/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;

/**
 * Classifies PromQL functions by their input vector type and aggregation behavior.
 * <p>
 * This classification is independent of how the function is transformed to ESQL.
 */
public enum FunctionType {
    /**
     * Aggregates samples within each time series over a time window.
     * <p>
     * Examples: rate(), irate(), increase(), delta(), avg_over_time(), sum_over_time(),
     * first_over_time(), last_over_time(), present_over_time(), absent_over_time()
     */
    WITHIN_SERIES_AGGREGATION(PromqlDataType.RANGE_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Aggregates multiple time series at a single evaluation timestamp.
     * <p>
     * Examples: sum(), avg(), max(), min(), count(), stddev(), stdvar(), quantile(),
     * group(), count_values()
     */
    ACROSS_SERIES_AGGREGATION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Ranks multiple time series at a single evaluation timestamp and keeps a subset of them.
     * <p>
     * Unlike aggregations, ranking functions preserve the full label identity of selected series.
     * Examples: topk(), bottomk()
     */
    ACROSS_SERIES_REDUCTION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Transforms each sample independently without changing vector cardinality.
     * <p>
     * Examples: abs(), ceil(), floor(), round(), sqrt(), exp(), ln(), log2(), log10(),
     * sin(), cos(), clamp(), clamp_max(), clamp_min(), sgn()
     */
    VALUE_TRANSFORMATION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Manipulates, queries, or filters series based on their labels.
     * <p>
     * Examples: label_replace(), label_join(), absent()
     */
    METADATA_MANIPULATION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Extracts or computes time-based values from timestamps.
     * <p>
     * Examples: day_of_month(), day_of_week(), days_in_month(), hour(), minute(),
     * month(), year(), timestamp()
     */
    TIME_EXTRACTION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Operates on native histogram samples.
     * <p>
     * Examples: histogram_quantile(), histogram_avg(), histogram_count(), histogram_sum()
     */
    HISTOGRAM(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Converts a scalar to an instant vector.
     * <p>
     * Example: {@code vector(42)}
     */
    VECTOR_CONVERSION(PromqlDataType.SCALAR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Converts a single-element instant vector to a scalar.
     * <p>
     * If the vector does not contain exactly one element, {@code NaN} is returned.
     * Example: {@code scalar(vector(42))}
     */
    SCALAR_CONVERSION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.SCALAR),

    /**
     * Produces a scalar without consuming an input argument.
     * <p>
     * Examples: {@code pi()}, {@code time()}
     */
    SCALAR(null, PromqlDataType.SCALAR);

    public final PromqlDataType inputType;
    public final PromqlDataType outputType;

    FunctionType(PromqlDataType inputType, PromqlDataType outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }
}
