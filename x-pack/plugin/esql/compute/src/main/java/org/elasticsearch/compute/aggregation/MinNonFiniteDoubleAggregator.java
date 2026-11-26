/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;

/**
 * A {@code double} minimum aggregator with Prometheus/IEEE-754 non-finite semantics, used only by the PromQL translation.
 * {@link MinDoubleAggregator} also skips {@code NaN} and answers {@code NaN} only when every input is {@code NaN}; the
 * difference is signed zero. That aggregator orders {@code -0.0} below {@code 0.0} through {@code Math.min}, while this
 * one compares with {@code >}, as PromQL does, and treats the two zeros as equal. {@code ±Inf} participate as ordinary
 * ordered values. Emptiness is still reported as {@code null} via the generated {@code seen} tracking.
 */
@Aggregator({ @IntermediateState(name = "min", type = "DOUBLE"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class MinNonFiniteDoubleAggregator {

    public static double init() {
        return Double.NaN;
    }

    public static double combine(double current, double v) {
        // NaN is skipped as long as a non-NaN value has been seen (current is non-NaN); the seed NaN adopts the first value.
        return Double.isNaN(current) || current > v ? v : current;
    }
}
