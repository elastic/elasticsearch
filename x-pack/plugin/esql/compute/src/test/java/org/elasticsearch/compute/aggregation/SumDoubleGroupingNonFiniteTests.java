/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A grouped sum that receives a non-finite value must report that group as seen. Group id tracking is switched on
 * whenever a {@code null} reaches the aggregator, and an untracked group is rendered as {@code null} on evaluation, so
 * a group whose running total is {@code ±Inf} or {@code NaN} would otherwise disappear from the result.
 */
public class SumDoubleGroupingNonFiniteTests extends ComputeTestCase {

    public void testNonFiniteValueMarksGroupAsSeen() {
        var blockFactory = blockFactory();
        try (var state = SumDoubleAggregator.initGrouping(blockFactory.bigArrays())) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
            SumDoubleAggregator.combine(state, 0, Double.POSITIVE_INFINITY);

            assertTrue("a group summing to +Inf has a value", state.hasValue(0));
        }
    }

    public void testFiniteValueAddedToNonFiniteTotalMarksGroupAsSeen() {
        var blockFactory = blockFactory();
        try (var state = SumDoubleAggregator.initGrouping(blockFactory.bigArrays())) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
            SumDoubleAggregator.combine(state, 0, Double.NaN);
            SumDoubleAggregator.combine(state, 0, 1.0);

            assertTrue("a group summing to NaN has a value", state.hasValue(0));
        }
    }

    public void testNonFiniteGroupIsNotDroppedFromResult() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var state = SumDoubleAggregator.initGrouping(blockFactory.bigArrays());
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2);
            var evaluationContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
            SumDoubleAggregator.combine(state, 0, Double.POSITIVE_INFINITY);
            SumDoubleAggregator.combine(state, 1, 2.0);

            try (Block result = SumDoubleAggregator.evaluateFinal(state, selected, evaluationContext)) {
                assertThat("the +Inf group must be emitted", BlockUtils.toJavaObject(result, 0), notNullValue());
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(Double.POSITIVE_INFINITY));
                assertThat(((DoubleBlock) result).getDouble(1), equalTo(2.0));
            }
        }
    }

    /**
     * Ordering must not change the outcome: a group is either seen or not, regardless of whether the non-finite value
     * arrived first or last.
     */
    public void testNonFiniteGroupIsOrderIndependent() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var state = SumDoubleAggregator.initGrouping(blockFactory.bigArrays());
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2);
            var evaluationContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            state.enableGroupIdTracking(new SeenGroupIds.Empty());
            // group 0 sees the non-finite value first, group 1 sees it last
            SumDoubleAggregator.combine(state, 0, Double.POSITIVE_INFINITY);
            SumDoubleAggregator.combine(state, 0, 1.0);
            SumDoubleAggregator.combine(state, 1, 1.0);
            SumDoubleAggregator.combine(state, 1, Double.POSITIVE_INFINITY);

            try (Block result = SumDoubleAggregator.evaluateFinal(state, selected, evaluationContext)) {
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(Double.POSITIVE_INFINITY));
                assertThat(((DoubleBlock) result).getDouble(1), equalTo(Double.POSITIVE_INFINITY));
            }
        }
    }
}
