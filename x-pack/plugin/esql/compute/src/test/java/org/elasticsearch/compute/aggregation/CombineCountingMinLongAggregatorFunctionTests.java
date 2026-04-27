/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that the {@code combineOnceForConstant} optimisation in generated aggregator
 * functions short-circuits constant vectors to a single {@code combine} call.
 * <p>
 * Uses {@link CombineCountingMinLongAggregator} as a test spy: its static
 * {@link CombineCountingMinLongAggregator#COMBINE_CALL_COUNT} counter is
 * incremented on every {@code combine} invocation, allowing tests to assert
 * exactly how many times the value was accessed.
 */
public class CombineCountingMinLongAggregatorFunctionTests extends ESTestCase {

    public void testConstantVectorCombinesOnce() {
        DriverContext driverContext = driverContext();
        int positions = between(2, 1024);

        try (
            LongBlock block = driverContext.blockFactory().newConstantLongBlockWith(42L, positions);
            BooleanVector mask = driverContext.blockFactory().newConstantBooleanVector(true, positions);
            CombineCountingMinLongAggregatorFunction agg = new CombineCountingMinLongAggregatorFunction(driverContext, List.of(0))
        ) {
            CombineCountingMinLongAggregator.COMBINE_CALL_COUNT.set(0);
            agg.addRawInput(new Page(block), mask);
            assertThat(CombineCountingMinLongAggregator.COMBINE_CALL_COUNT.get(), equalTo(1));
        }
    }

    public void testNonConstantVectorCombinesPerPosition() {
        DriverContext driverContext = driverContext();
        int positions = between(2, 1024);

        try (
            LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positions);
            BooleanVector mask = driverContext.blockFactory().newConstantBooleanVector(true, positions)
        ) {
            for (int i = 0; i < positions; i++) {
                builder.appendLong(randomLong());
            }
            try (
                LongBlock block = builder.build();
                CombineCountingMinLongAggregatorFunction agg = new CombineCountingMinLongAggregatorFunction(driverContext, List.of(0))
            ) {
                CombineCountingMinLongAggregator.COMBINE_CALL_COUNT.set(0);
                agg.addRawInput(new Page(block), mask);
                assertThat(CombineCountingMinLongAggregator.COMBINE_CALL_COUNT.get(), equalTo(positions));
            }
        }
    }

    public void testConstantVectorProducesCorrectResult() {
        DriverContext driverContext = driverContext();
        long value = randomLong();
        int positions = between(2, 1024);

        try (
            LongBlock block = driverContext.blockFactory().newConstantLongBlockWith(value, positions);
            BooleanVector mask = driverContext.blockFactory().newConstantBooleanVector(true, positions);
            CombineCountingMinLongAggregatorFunction agg = new CombineCountingMinLongAggregatorFunction(driverContext, List.of(0))
        ) {
            agg.addRawInput(new Page(block), mask);

            Block[] result = new Block[1];
            agg.evaluateFinal(result, 0, driverContext);
            try (LongBlock resultBlock = (LongBlock) result[0]) {
                assertThat(resultBlock.getLong(0), equalTo(value));
            }
        }
    }

    /**
     * Verifies that the grouping aggregator's constant-vector path produces
     * correct per-group results: every group should see the constant value.
     */
    public void testGroupingConstantVectorProducesCorrectResults() {
        DriverContext driverContext = driverContext();
        int groups = between(2, 128);
        long value = randomLong();

        try (
            LongBlock valuesBlock = driverContext.blockFactory().newConstantLongBlockWith(value, groups);
            IntVector groupIds = driverContext.blockFactory().newIntRangeVector(0, groups);
            var agg = new CombineCountingMinLongGroupingAggregatorFunction(List.of(0), driverContext)
        ) {
            Page page = new Page(valuesBlock);
            var addInput = agg.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page);
            addInput.add(0, groupIds);
            addInput.close();

            IntVector selected = driverContext.blockFactory().newIntRangeVector(0, groups);
            var evalCtx = new GroupingAggregatorEvaluationContext(driverContext);
            try (var prepared = agg.prepareEvaluateFinal(selected, evalCtx)) {
                Block[] result = new Block[1];
                prepared.evaluate(result, 0, selected);
                try (LongBlock resultBlock = (LongBlock) result[0]) {
                    for (int g = 0; g < groups; g++) {
                        assertThat("group " + g, resultBlock.getLong(g), equalTo(value));
                    }
                }
            } finally {
                evalCtx.close();
                selected.close();
            }
        }
    }

    private static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
    }
}
