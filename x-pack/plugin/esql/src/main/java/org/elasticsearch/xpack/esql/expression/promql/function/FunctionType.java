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
     * Aggregates data within each time series over a time window.
     * <p>
     * Input: Range vector (multiple samples per series over time range)
     * <br>
     * Output: Instant vector (one aggregated value per series)
     * <br>
     * Grouping: Implicit by time series (_tsid)
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
     * Aggregates data across multiple time series at a single point in time.
     * <p>
     * Input: Instant vector (one sample per series at evaluation time)
     * <br>
     * Output: Instant vector (aggregated across series)
     * <br>
     * Grouping: Explicit by labels (by/without)
     * <p>
     * Examples:
     * <ul>
     * <li>Basic: sum(), avg(), max(), min(), count()</li>
     * <li>Statistical: stddev(), stdvar(), quantile()</li>
     * <li>Top-k: topk(), bottomk()</li>
     * <li>Grouping: group(), count_values()</li>
     * </ul>
     */
    ACROSS_SERIES_AGGREGATION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Transforms each sample in a vector independently (element-wise operations).
     * <p>
     * Input: Instant vector
     * <br>
     * Output: Instant vector (same cardinality, transformed values)
     * <p>
     * Examples:
     * <ul>
     * <li>Math: abs(), ceil(), floor(), round(), sqrt(), exp(), ln(), log2(), log10()</li>
     * <li>Trigonometric: sin(), cos(), tan(), asin(), acos(), atan(), sinh(), cosh(), tanh()</li>
     * <li>Clamping: clamp(), clamp_max(), clamp_min()</li>
     * <li>Sign: sgn()</li>
     * </ul>
     */
    VALUE_TRANSFORMATION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Manipulates or queries the label set of time series.
     * <p>
     * Input: Instant vector
     * <br>
     * Output: Instant vector (modified labels or label-based filtering)
     * <p>
     * Examples:
     * <ul>
     * <li>Manipulation: label_replace(), label_join()</li>
     * <li>Querying: absent()</li>
     * </ul>
     */
    METADATA_MANIPULATION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Extracts or computes time-based values from timestamps.
     * <p>
     * Input: Instant vector
     * <br>
     * Output: Instant vector (timestamp replaced with time component)
     * <p>
     * Examples: day_of_month(), day_of_week(), hour(), minute(), month(), year(), timestamp()
     */
    TIME_EXTRACTION(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Operates on native histogram data types.
     * <p>
     * Input: Instant vector (histogram samples)
     * <br>
     * Output: Instant vector or scalar
     * <p>
     * Examples: histogram_quantile(), histogram_avg(), histogram_count(), histogram_sum()
     */
    HISTOGRAM(PromqlDataType.INSTANT_VECTOR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Converts a scalar to a vector.
     * <p>
     * Input: Scalar
     * <br>
     * Output: Instant vector (single sample with no labels)
     */
    VECTOR(PromqlDataType.SCALAR, PromqlDataType.INSTANT_VECTOR),

    /**
     * Special functions that don't fit standard patterns.
     * <p>
     * Examples:
     * <ul>
     * <li>scalar() - converts single-element vector to scalar</li>
     * <li>time() - current timestamp as scalar</li>
     * <li>pi() - mathematical constant</li>
     * </ul>
     */
    // TODO refine input/output types for these special cases
    SPECIAL(null, null);

    private final PromqlDataType inputType;
    private final PromqlDataType outputType;

    FunctionType(PromqlDataType inputType, PromqlDataType outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    /**
     * Returns whether this function operates on range vectors.
     */
    public boolean isRangeVector() {
        return this == WITHIN_SERIES_AGGREGATION;
    }

    /**
     * Returns whether this function operates on instant vectors.
     */
    public boolean isInstantVector() {
        return this != WITHIN_SERIES_AGGREGATION;
    }

    /**
     * Returns whether this function performs aggregation.
     */
    public boolean isAggregation() {
        return this == WITHIN_SERIES_AGGREGATION || this == ACROSS_SERIES_AGGREGATION;
    }

    /**
     * Returns whether this function transforms values element-wise.
     */
    public boolean isElementWise() {
        return this == VALUE_TRANSFORMATION || this == TIME_EXTRACTION || this == METADATA_MANIPULATION;
    }

    public PromqlDataType outputType() {
        return outputType;
    }

    public PromqlDataType inputType() {
        return inputType;
    }

}
