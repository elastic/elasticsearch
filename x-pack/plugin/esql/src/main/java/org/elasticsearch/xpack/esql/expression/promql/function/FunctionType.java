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
     * Input: Range vector, with multiple samples per series over the requested window.
     * <br>
     * Output: Instant vector, with one aggregated value per series.
     * <br>
     * Grouping: Implicit by time series.
     * <p>
     * Examples:
     * <ul>
     * <li>Rate functions: rate(), irate(), increase(), delta(), idelta()</li>
     * <li>Aggregations: avg_over_time(), sum_over_time(), max_over_time(), min_over_time(), count_over_time()</li>
     * <li>Selection: first_over_time(), last_over_time()</li>
     * <li>Presence: present_over_time(), absent_over_time()</li>
     * </ul>
     */
    WITHIN_SERIES_AGGREGATION(PromqlDataType.RANGE_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Aggregates multiple time series for each evaluation interval.
     * <p>
     * Input: Instant vector, with one sample per series for the interval.
     * <br>
     * Output: Instant vector, aggregated across series.
     * <br>
     * Grouping: Explicit by labels ({@code by}/{@code without}) or ungrouped.
     * <p>
     * Examples:
     * <ul>
     * <li>Basic: sum(), avg(), max(), min(), count()</li>
     * <li>Statistical: stddev(), stdvar(), quantile()</li>
     * <li>Grouping: group(), count_values()</li>
     * </ul>
     */
    ACROSS_SERIES_AGGREGATION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Ranks multiple time series for each evaluation interval and keeps a subset of them.
     * <p>
     * Input: Instant vector, with one sample per series for the interval.
     * <br>
     * Output: Instant vector, with the selected input series.
     * <br>
     * Grouping: Explicit {@code by} partitions the ranking; selected series keep their full label identity.
     * <p>
     * Unlike aggregations, ranking functions preserve the full label identity of selected series.
     * <p>
     * Examples:
     * <ul>
     * <li>Top-k: topk()</li>
     * <li>Bottom-k: bottomk()</li>
     * </ul>
     */
    ACROSS_SERIES_REDUCTION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Transforms each sample independently without changing vector cardinality.
     * <p>
     * Input: Instant vector.
     * <br>
     * Output: Instant vector with the same label sets.
     * <br>
     * Grouping: none; each sample is transformed independently.
     * <p>
     * Examples:
     * <ul>
     * <li>Math: abs(), ceil(), floor(), round(), sqrt(), exp(), ln(), log2(), log10()</li>
     * <li>Trigonometry: sin(), cos()</li>
     * <li>Clamping/sign: clamp(), clamp_max(), clamp_min(), sgn()</li>
     * </ul>
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
