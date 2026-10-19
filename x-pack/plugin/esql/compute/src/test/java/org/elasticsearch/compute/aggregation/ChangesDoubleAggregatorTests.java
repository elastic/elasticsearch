/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;

public class ChangesDoubleAggregatorTests extends ComputeTestCase {

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

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
        }
    }

    public void testCountsInterleavedChangesAcrossRawPages() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, new double[] { 0, 0, 0, 0 }, new long[] { 8, 6, 4, 2 });
            addRaw(state, driverContext, new double[] { 1, 1, 1, 1 }, new long[] { 7, 5, 3, 1 });

            Block[] resultBlocks = new Block[1];
            state.prepareEvaluateFinal(selected, evalContext).evaluate(resultBlocks, 0, selected);
            try (LongBlock result = (LongBlock) resultBlocks[0]) {
                assertEquals(7L, result.getLong(0));
            }
        } finally {
            driverContext.finish();
        }
    }

    public void testFlushesRawBufferWhenSliceIndexChanges() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, new double[] { 1, 2 }, new long[] { 40, 30 }, 0);
            addRaw(state, driverContext, new double[] { 3, 4 }, new long[] { 20, 10 }, 1);

            Block[] intermediate = new Block[state.intermediateBlockCount()];
            state.prepareEvaluateIntermediate(selected, evalContext).evaluate(intermediate, 0, selected);
            try (
                LongBlock timestamps = (LongBlock) intermediate[0];
                DoubleBlock values = (DoubleBlock) intermediate[1];
                LongBlock sampleCounts = (LongBlock) intermediate[2];
                LongBlock changes = (LongBlock) intermediate[3]
            ) {
                assertEquals(4, timestamps.getValueCount(0));
                assertEquals(4, values.getValueCount(0));
                assertEquals(4, sampleCounts.getLong(0));
                assertEquals(2, changes.getLong(0));
            }
        } finally {
            driverContext.finish();
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
        }
    }

    public void testRepeatedNaNIsNotAChange() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, new double[] { Double.NaN, Double.NaN, 1 }, new long[] { 30, 20, 10 });
            Block[] resultBlocks = new Block[1];
            state.prepareEvaluateFinal(selected, evalContext).evaluate(resultBlocks, 0, selected);
            try (LongBlock result = (LongBlock) resultBlocks[0]) {
                assertEquals(1L, result.getLong(0));
            }
        } finally {
            driverContext.finish();
        }
    }

    private static GroupingAggregatorFunction newAggregator(DriverContext driverContext) {
        return new ChangesDoubleAggregatorFunctionSupplier().groupingAggregator(driverContext, List.of(0, 1, 2));
    }

    private static GroupingAggregatorFunction newIntermediateAggregator(DriverContext driverContext) {
        return new ChangesDoubleAggregatorFunctionSupplier().groupingAggregator(driverContext, List.of(0, 1, 2, 3));
    }

    private static void addRaw(GroupingAggregatorFunction aggregator, DriverContext driverContext, double[] values, long[] timestamps) {
        addRaw(aggregator, driverContext, values, timestamps, 0);
    }

    private static void addRaw(
        GroupingAggregatorFunction aggregator,
        DriverContext driverContext,
        double[] values,
        long[] timestamps,
        int sliceIndex
    ) {
        DoubleBlock valuesBlock = driverContext.blockFactory().newDoubleArrayVector(values, values.length).asBlock();
        LongBlock timestampsBlock = driverContext.blockFactory().newLongArrayVector(timestamps, timestamps.length).asBlock();
        IntBlock sliceIndices = driverContext.blockFactory().newConstantIntBlockWith(sliceIndex, values.length);
        try (
            Page page = new Page(valuesBlock, timestampsBlock, sliceIndices);
            var groups = driverContext.blockFactory().newConstantIntVector(0, values.length);
            var addInput = aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page)
        ) {
            addInput.add(0, groups);
        }
    }

}
