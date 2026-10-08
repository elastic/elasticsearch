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
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.util.List;
import java.util.function.Consumer;

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

    public void testIgnoresMinAndMax() {
        assertChanges(List.of(histogram(3.0, 1.0, 3.0), histogram(3.0, -100.0, 100.0)), new long[] { 20, 10 }, 0L);
    }

    public void testSumComparisonIsExact() {
        assertChanges(List.of(histogram(1.0, 1.0, 1.0), histogram(Math.nextUp(1.0), 1.0, 1.0)), new long[] { 20, 10 }, 1L);
    }

    public void testSumComparisonDistinguishesSignedZero() {
        assertChanges(List.of(histogram(+0.0, 1.0, 1.0), histogram(-0.0, 1.0, 1.0)), new long[] { 20, 10 }, 1L);
    }

    public void testDuplicateTimestampOrderingMatchesSumEquality() {
        assertChanges(
            List.of(histogram(+0.0, 1.0, 1.0), histogram(-0.0, 1.0, 1.0), histogram(+0.0, 1.0, 1.0)),
            new long[] { 10, 10, 10 },
            1L
        );
    }

    public void testZeroBucketDifferencesAreChanges() {
        assertChanges(
            List.of(
                histogramWithZeroBucket(ZeroBucket.create(0.5, 1)),
                histogramWithZeroBucket(ZeroBucket.create(1.0, 1)),
                histogramWithZeroBucket(ZeroBucket.create(1.0, 2))
            ),
            new long[] { 30, 20, 10 },
            2L
        );
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

    private static void addRaw(
        GroupingAggregatorFunction aggregator,
        DriverContext driverContext,
        List<Consumer<ExponentialHistogramBuilder>> histograms,
        long[] timestamps
    ) {
        assert histograms.size() == timestamps.length;
        ExponentialHistogramBlock values;
        try (
            ExponentialHistogramBlock.Builder blockBuilder = driverContext.blockFactory()
                .newExponentialHistogramBlockBuilder(histograms.size())
        ) {
            for (Consumer<ExponentialHistogramBuilder> configure : histograms) {
                try (
                    ExponentialHistogramBuilder histogramBuilder = ExponentialHistogram.builder(
                        0,
                        ExponentialHistogramCircuitBreaker.noop()
                    )
                ) {
                    configure.accept(histogramBuilder);
                    try (var histogram = histogramBuilder.build()) {
                        blockBuilder.append(histogram);
                    }
                }
            }
            values = blockBuilder.build();
        }
        LongBlock timestampBlock = driverContext.blockFactory().newLongArrayVector(timestamps, timestamps.length).asBlock();
        try (
            Page page = new Page(values, timestampBlock);
            var groups = driverContext.blockFactory().newConstantIntVector(0, histograms.size());
            var addInput = aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page)
        ) {
            addInput.add(0, groups);
        }
    }

    private static Consumer<ExponentialHistogramBuilder> histogram(double sum, double min, double max) {
        return builder -> builder.sum(sum).min(min).max(max).setPositiveBucket(0, 1);
    }

    private static Consumer<ExponentialHistogramBuilder> histogramWithZeroBucket(ZeroBucket zeroBucket) {
        return builder -> builder.zeroBucket(zeroBucket).sum(0.0).min(0.0).max(0.0);
    }

    private void assertChanges(List<Consumer<ExponentialHistogramBuilder>> histograms, long[] timestamps, long expected) {
        DriverContext driverContext = driverContext();
        try (
            var selected = driverContext.blockFactory().newConstantIntVector(0, 1);
            var state = newAggregator(driverContext);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            addRaw(state, driverContext, histograms, timestamps);
            assertChanges(state, selected, evalContext, expected);
        } finally {
            driverContext.finish();
            assertDriverContext(driverContext);
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
