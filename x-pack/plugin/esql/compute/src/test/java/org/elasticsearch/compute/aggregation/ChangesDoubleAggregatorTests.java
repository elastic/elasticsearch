/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.OperatorTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public class ChangesDoubleAggregatorTests extends OperatorTests {

    public void testCountsTimestampOrderedChangesAcrossIntermediateMerge() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var left = newAggregator(driverContext);
            var right = newAggregator(driverContext);
            var merged = newIntermediateAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(left, driverContext, new double[] { 1.0, 2.0, 1.0 }, new long[] { 30, 20, 10 });
            addRaw(right, driverContext, new double[] { 4.0, 1.0 }, new long[] { 50, 40 });

            Block[] leftIntermediate = new Block[left.intermediateBlockCount()];
            left.prepareEvaluateIntermediate(selected, evalContext).evaluate(leftIntermediate, 0, selected);
            try (Page leftPage = new Page(leftIntermediate)) {
                merged.addIntermediateInput(0, selected, leftPage);
            }

            Block[] rightIntermediate = new Block[right.intermediateBlockCount()];
            right.prepareEvaluateIntermediate(selected, evalContext).evaluate(rightIntermediate, 0, selected);
            try (Page rightPage = new Page(rightIntermediate)) {
                merged.addIntermediateInput(0, selected, rightPage);
            }

            Block[] resultBlocks = new Block[1];
            merged.prepareEvaluateFinal(selected, evalContext).evaluate(resultBlocks, 0, selected);
            try (LongBlock result = (LongBlock) resultBlocks[0]) {
                assertFalse(result.isNull(0));
                assertEquals(3L, result.getLong(0));
            }
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
        }
    }

    public void testIntermediateKeepsUpToThreePointsUncompacted() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, new double[] { 3.0, 2.0, 1.0 }, new long[] { 30, 20, 10 });

            Block[] intermediate = new Block[state.intermediateBlockCount()];
            state.prepareEvaluateIntermediate(selected, evalContext).evaluate(intermediate, 0, selected);
            try (
                LongBlock timestamps = (LongBlock) intermediate[0];
                DoubleBlock values = (DoubleBlock) intermediate[1];
                LongBlock changes = (LongBlock) intermediate[2]
            ) {
                assertEquals(3, timestamps.getValueCount(0));
                assertEquals(3, values.getValueCount(0));
                assertEquals(1, changes.getValueCount(0));
                assertEquals(-3L, changes.getLong(0));
            }
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
        }
    }

    public void testIntermediateCompactsFourPointsToInterval() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, new double[] { 1.0, 1.0, 2.0, 1.0 }, new long[] { 40, 30, 20, 10 });

            Block[] intermediate = new Block[state.intermediateBlockCount()];
            state.prepareEvaluateIntermediate(selected, evalContext).evaluate(intermediate, 0, selected);
            try (
                LongBlock timestamps = (LongBlock) intermediate[0];
                DoubleBlock values = (DoubleBlock) intermediate[1];
                LongBlock changes = (LongBlock) intermediate[2]
            ) {
                assertEquals(2, timestamps.getValueCount(0));
                assertEquals(2, values.getValueCount(0));
                assertEquals(1, changes.getValueCount(0));
                assertEquals(2L, changes.getLong(0));
            }
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
        }
    }

    public void testSingleSampleReturnsZeroAndMissingGroupReturnsNull() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newIntArrayVector(new int[] { 0, 1 }, 2);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, new double[] { 1.0 }, new long[] { 10 });
            Block[] resultBlocks = new Block[1];
            state.prepareEvaluateFinal(selected, evalContext).evaluate(resultBlocks, 0, selected);
            try (LongBlock result = (LongBlock) resultBlocks[0]) {
                assertFalse(result.isNull(0));
                assertEquals(0L, result.getLong(0));
                assertTrue(result.isNull(1));
            }
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
        }
    }

    private static GroupingAggregatorFunction newAggregator(DriverContext driverContext) {
        return new ChangesDoubleAggregatorFunctionSupplier().groupingAggregator(driverContext, List.of(0, 1));
    }

    private static GroupingAggregatorFunction newIntermediateAggregator(DriverContext driverContext) {
        return new ChangesDoubleAggregatorFunctionSupplier().groupingAggregator(driverContext, List.of(0, 1, 2));
    }

    private static void addRaw(GroupingAggregatorFunction aggregator, DriverContext driverContext, double[] values, long[] timestamps) {
        DoubleBlock valuesBlock = driverContext.blockFactory().newDoubleArrayVector(values, values.length).asBlock();
        LongBlock timestampsBlock = driverContext.blockFactory().newLongArrayVector(timestamps, timestamps.length).asBlock();
        try (
            Page page = new Page(valuesBlock, timestampsBlock);
            var groups = driverContext.blockFactory().newConstantIntVector(0, values.length);
            var addInput = aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page)
        ) {
            addInput.add(0, groups);
        }
    }
}
