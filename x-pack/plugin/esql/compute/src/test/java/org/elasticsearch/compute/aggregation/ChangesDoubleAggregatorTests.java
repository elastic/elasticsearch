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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
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

    public void testCountsInterleavedChangesAcrossIntermediateMerge() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var left = newAggregator(driverContext);
            var right = newAggregator(driverContext);
            var merged = newIntermediateAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(left, driverContext, new double[] { 0, 0, 0, 0 }, new long[] { 8, 6, 4, 2 });
            addRaw(right, driverContext, new double[] { 1, 1, 1, 1 }, new long[] { 7, 5, 3, 1 });

            for (var source : List.of(left, right)) {
                Block[] intermediate = new Block[source.intermediateBlockCount()];
                source.prepareEvaluateIntermediate(selected, evalContext).evaluate(intermediate, 0, selected);
                try (Page page = new Page(intermediate)) {
                    merged.addIntermediateInput(0, selected, page);
                }
            }

            Block[] resultBlocks = new Block[1];
            merged.prepareEvaluateFinal(selected, evalContext).evaluate(resultBlocks, 0, selected);
            try (LongBlock result = (LongBlock) resultBlocks[0]) {
                assertEquals(7L, result.getLong(0));
            }
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
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

    public void testMultivaluedMetricSamplesAreCounted() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var groups = driverContext.blockFactory().newConstantIntVector(0, 3);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext);
            var valueBuilder = driverContext.blockFactory().newDoubleBlockBuilder(3)
        ) {
            valueBuilder.beginPositionEntry().appendDouble(2).appendDouble(1).endPositionEntry();
            valueBuilder.appendDouble(1).appendDouble(1);
            DoubleBlock values = valueBuilder.build();
            assertEquals(2, values.getValueCount(0));
            LongBlock timestamps = driverContext.blockFactory().newLongArrayVector(new long[] { 30, 20, 10 }, 3).asBlock();
            addRaw(state, values, timestamps, groups);

            Block[] intermediate = new Block[state.intermediateBlockCount()];
            state.prepareEvaluateIntermediate(selected, evalContext).evaluate(intermediate, 0, selected);
            try (
                LongBlock intermediateTimestamps = (LongBlock) intermediate[0];
                DoubleBlock intermediateValues = (DoubleBlock) intermediate[1]
            ) {
                assertEquals(4, intermediateTimestamps.getValueCount(0));
                assertEquals(4, intermediateValues.getValueCount(0));
            }

            Block[] resultBlocks = new Block[1];
            state.prepareEvaluateFinal(selected, evalContext).evaluate(resultBlocks, 0, selected);
            try (LongBlock result = (LongBlock) resultBlocks[0]) {
                assertEquals(2L, result.getLong(0));
            }
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
        }
    }

    public void testMultivaluedTimestampsAreCounted() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext);
            var timestampBuilder = driverContext.blockFactory().newLongBlockBuilder(2)
        ) {
            timestampBuilder.beginPositionEntry().appendLong(30).appendLong(10).endPositionEntry();
            timestampBuilder.appendLong(20);
            DoubleBlock values = driverContext.blockFactory().newDoubleArrayVector(new double[] { 1, 2 }, 2).asBlock();
            LongBlock timestamps = timestampBuilder.build();
            try (var groups = driverContext.blockFactory().newIntBlockBuilder(2)) {
                groups.appendInt(0).appendInt(0);
                try (IntBlock groupBlock = groups.build()) {
                    addRaw(state, values, timestamps, groupBlock);
                }
            }

            Block[] resultBlocks = new Block[1];
            state.prepareEvaluateFinal(selected, evalContext).evaluate(resultBlocks, 0, selected);
            try (LongBlock result = (LongBlock) resultBlocks[0]) {
                assertEquals(2L, result.getLong(0));
            }
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
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
            assertDriverContext(driverContext);
        }
    }

    private static GroupingAggregatorFunction newAggregator(DriverContext driverContext) {
        return new ChangesDoubleAggregatorFunctionSupplier().groupingAggregator(driverContext, List.of(0, 1));
    }

    private static GroupingAggregatorFunction newIntermediateAggregator(DriverContext driverContext) {
        return new ChangesDoubleAggregatorFunctionSupplier().groupingAggregator(driverContext, List.of(0, 1));
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

    private static void addRaw(GroupingAggregatorFunction aggregator, DoubleBlock values, LongBlock timestamps, IntBlock groups) {
        try (
            Page page = new Page(values, timestamps);
            var addInput = aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page)
        ) {
            addInput.add(0, groups);
        }
    }

    private static void addRaw(GroupingAggregatorFunction aggregator, DoubleBlock values, LongBlock timestamps, IntVector groups) {
        try (
            Page page = new Page(values, timestamps);
            var addInput = aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page)
        ) {
            addInput.add(0, groups);
        }
    }
}
