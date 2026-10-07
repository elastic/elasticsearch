/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.OperatorTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;

import java.util.List;

public class ChangesExponentialHistogramAggregatorTests extends OperatorTests {

    public void testCountsInterleavedChangesAcrossIntermediateMerge() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var left = newAggregator(driverContext);
            var right = newAggregator(driverContext);
            var merged = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(left, driverContext, repeatedSamples(new double[] { 1, 2 }, 4), new long[] { 8, 6, 4, 2 });
            addRaw(right, driverContext, repeatedSamples(new double[] { 3, 4 }, 4), new long[] { 7, 5, 3, 1 });

            for (var source : List.of(left, right)) {
                Block[] intermediate = new Block[source.intermediateBlockCount()];
                source.prepareEvaluateIntermediate(selected, evalContext).evaluate(intermediate, 0, selected);
                try (Page page = new Page(intermediate)) {
                    merged.addIntermediateInput(0, selected, page);
                }
            }

            assertChanges(merged, selected, evalContext, 7L);
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
        }
    }

    public void testEqualHistogramValuesAreNotChanges() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, new double[][] { { 1, 2, 3 }, { 1, 2, 3 }, { 1, 2, 3 } }, new long[] { 30, 20, 10 });

            assertChanges(state, selected, evalContext, 0L);
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
            addRaw(state, driverContext, new double[][] { { 1, 2, 3 } }, new long[] { 10 });

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

    public void testDuplicateTimestampsHaveDeterministicOrdering() {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, new double[][] { { 1 }, { 2 }, { 1 } }, new long[] { 10, 10, 10 });

            assertChanges(state, selected, evalContext, 1L);
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
        }
    }

    private static GroupingAggregatorFunction newAggregator(DriverContext driverContext) {
        return new ChangesExponentialHistogramAggregatorFunctionSupplier().groupingAggregator(driverContext, List.of(0, 1));
    }

    private static void addRaw(GroupingAggregatorFunction aggregator, DriverContext driverContext, double[][] samples, long[] timestamps) {
        assert samples.length == timestamps.length;
        ExponentialHistogramBlock values;
        try (ExponentialHistogramBlock.Builder builder = driverContext.blockFactory().newExponentialHistogramBlockBuilder(samples.length)) {
            for (double[] histogramSamples : samples) {
                try (var histogram = ExponentialHistogram.create(16, ExponentialHistogramCircuitBreaker.noop(), histogramSamples)) {
                    builder.append(histogram);
                }
            }
            values = builder.build();
        }
        LongBlock timestampBlock = driverContext.blockFactory().newLongArrayVector(timestamps, timestamps.length).asBlock();
        try (
            Page page = new Page(values, timestampBlock);
            var groups = driverContext.blockFactory().newConstantIntVector(0, samples.length);
            var addInput = aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page)
        ) {
            addInput.add(0, groups);
        }
    }

    private static double[][] repeatedSamples(double[] samples, int count) {
        double[][] result = new double[count][];
        for (int i = 0; i < count; i++) {
            result[i] = samples;
        }
        return result;
    }

    private static void assertChanges(
        GroupingAggregatorFunction aggregator,
        IntVector selected,
        GroupingAggregatorEvaluationContext evalContext,
        long expected
    ) {
        Block[] resultBlocks = new Block[1];
        aggregator.prepareEvaluateFinal(selected, evalContext).evaluate(resultBlocks, 0, selected);
        try (LongBlock result = (LongBlock) resultBlocks[0]) {
            assertEquals(expected, result.getLong(0));
        }
    }
}
