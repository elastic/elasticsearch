/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

/**
 * Classifies PromQL functions by their input vector type and aggregation behavior.
 * This classification is independent of how the function is transformed to ESQL.
 */
public enum FunctionType {
    /**
     * Aggregates data within each time series over a time window.
     *
     * Input: Range vector (multiple samples per series over time range)
     * Output: Instant vector (one aggregated value per series)
     * Grouping: Implicit by time series (_tsid)
     *
     * Examples:
     * - Rate functions: rate(), irate(), increase(), delta(), idelta()
     * - Aggregations: avg_over_time(), sum_over_time(), max_over_time(), min_over_time(), count_over_time()
     * - Selection: first_over_time(), last_over_time()
     * - Presence: present_over_time(), absent_over_time()
     */
    WITHIN_SERIES_AGGREGATION,

    /**
     * Aggregates data across multiple time series at a single point in time.
     *
     * Input: Instant vector (one sample per series at evaluation time)
     * Output: Instant vector (aggregated across series)
     * Grouping: Explicit by labels (by/without)
     *
     * Examples:
     * - Basic: sum(), avg(), max(), min(), count()
     * - Statistical: stddev(), stdvar(), quantile()
     * - Top-k: topk(), bottomk()
     * - Grouping: group(), count_values()
     */
    ACROSS_SERIES_AGGREGATION,

    /**
     * Transforms each sample in a vector independently (element-wise operations).
     *
     * Input: Instant vector
     * Output: Instant vector (same cardinality, transformed values)
     *
     * Examples:
     * - Math: abs(), ceil(), floor(), round(), sqrt(), exp(), ln(), log2(), log10()
     * - Trigonometric: sin(), cos(), tan(), asin(), acos(), atan(), sinh(), cosh(), tanh()
     * - Clamping: clamp(), clamp_max(), clamp_min()
     * - Sign: sgn()
     */
    VALUE_TRANSFORMATION,

    /**
     * Manipulates or queries the label set of time series.
     *
     * Input: Instant vector
     * Output: Instant vector (modified labels or label-based filtering)
     *
     * Examples:
     * - Manipulation: label_replace(), label_join()
     * - Querying: absent()
     */
    METADATA_MANIPULATION,

    /**
     * Extracts or computes time-based values from timestamps.
     *
     * Input: Instant vector
     * Output: Instant vector (timestamp replaced with time component)
     *
     * Examples: day_of_month(), day_of_week(), hour(), minute(), month(), year(), timestamp()
     */
    TIME_EXTRACTION,

    /**
     * Operates on histogram data types.
     *
     * Input: Instant vector (histogram samples)
     * Output: Instant vector or scalar
     *
     * Examples: histogram_quantile(), histogram_avg(), histogram_count(), histogram_sum()
     */
    HISTOGRAM,

    /**
     * Special functions that don't fit standard patterns.
     *
     * Examples:
     * - vector() - converts scalar to vector
     * - scalar() - converts single-element vector to scalar
     * - time() - current timestamp as scalar
     * - pi() - mathematical constant
     */
    SPECIAL;

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
}
