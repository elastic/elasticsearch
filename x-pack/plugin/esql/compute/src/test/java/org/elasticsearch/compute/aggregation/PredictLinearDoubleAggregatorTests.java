/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

public class PredictLinearDoubleAggregatorTests extends ComputeTestCase {
    private static final long EPOCH = 1_730_000_000_000L;

    public void testConstantDoesNotOverflow() {
        assertPrediction(new double[] { Double.MAX_VALUE, Double.MAX_VALUE }, new long[] { 0, 2000 }, 10_000, 3, Double.MAX_VALUE);
    }

    public void testHorizonIsRelativeToEvaluationTime() {
        for (double horizon : new double[] { 3, 0, -4, 0.25 }) {
            assertPrediction(new double[] { 1, 5, 11 }, new long[] { 0, 2000, 5000 }, 10_000, horizon, 21 + 2 * horizon);
        }
    }

    public void testNoisySamplesUseAllObservations() {
        assertPrediction(new double[] { 0, 3, 0 }, new long[] { 0, 1000, 2000 }, 2000, 1, 1.0);
    }

    public void testMillisecondTimestampsAndHorizon() {
        // The line y = 1 + 2*t is evaluated at 2.125s plus a 500ms horizon.
        assertPrediction(new double[] { 1.25, 2.25, 3.75 }, new long[] { 125, 625, 1375 }, 2125, 0.5, 6.25);
    }

    public void testNonFiniteSamplesAndHorizon() {
        for (double nonFinite : new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
            assertPrediction(new double[] { nonFinite, nonFinite }, new long[] { 0, 2000 }, 10_000, 3, Double.NaN);
            assertPrediction(new double[] { 1, nonFinite, 11 }, new long[] { 0, 2000, 5000 }, 10_000, 3, Double.NaN);
            assertPrediction(new double[] { 7, 7 }, new long[] { 0, 2000 }, 10_000, nonFinite, Double.NaN);
        }
        assertPrediction(new double[] { 1, 5 }, new long[] { 0, 2000 }, 10_000, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    public void testPrometheusResetShapedGaugeFixture() {
        // prometheus/prometheus@9993b506, promql/promqltest/testdata/functions.test:417-428.
        // The left-open 50m window excludes the observation at t=0; a 55m window includes it.
        double[] values = { 0, 10, 20, 30, 40, 0, 10, 20, 30, 40, 50 };
        long[] timestamps = IntStream.range(0, values.length).mapToLong(i -> i * 300_000L).toArray();
        assertPrediction(values, timestamps, 3_000_000, 3600, 76.81818181818181);
        assertPrediction(
            Arrays.copyOfRange(values, 1, values.length),
            Arrays.copyOfRange(timestamps, 1, timestamps.length),
            3_000_000,
            3600,
            70.0
        );
    }

    public void testTimestampDifferenceDoesNotOverflow() {
        assertPrediction(
            new double[] { 0, 2 },
            new long[] { -9_000_000_000_000_000_000L - EPOCH, 9_000_000_000_000_000_000L - EPOCH },
            0,
            0,
            1 + EPOCH / 9e18
        );
    }

    public void testInsufficientSamples() {
        assertPrediction(new double[0], new long[0], 10_000, 3, null);
        assertPrediction(new double[] { 7 }, new long[] { 1000 }, 10_000, 3, null);
    }

    private void assertPrediction(double[] values, long[] offsets, long evaluationOffset, double horizon, Double expected) {
        var factory = blockFactory();
        var driverContext = new DriverContext(factory.bigArrays(), factory, null);
        int intermediateCount = new PredictLinearDoubleAggregatorFunctionSupplier(horizon).groupingIntermediateStateDesc().size();
        try (
            var input = new PredictLinearDoubleAggregatorFunctionSupplier(horizon).groupingAggregator(driverContext, List.of(0, 1));
            var merged = new PredictLinearDoubleAggregatorFunctionSupplier(horizon).groupingAggregator(
                driverContext,
                IntStream.range(0, intermediateCount).boxed().toList()
            );
            var selected = factory.newConstantIntVector(0, 1);
            var context = evaluationContext(driverContext, EPOCH + evaluationOffset)
        ) {
            long[] timestamps = new long[offsets.length];
            for (int i = 0; i < offsets.length; i++) {
                timestamps[i] = EPOCH + offsets[i];
            }
            addRaw(input, driverContext, values, timestamps);
            // Merge independently accumulated single-sample partitions in random order. Their origins differ.
            var positions = new ArrayList<>(IntStream.range(0, values.length).boxed().toList());
            Collections.shuffle(positions, random());
            for (int position : positions) {
                try (
                    var partition = new PredictLinearDoubleAggregatorFunctionSupplier(horizon).groupingAggregator(
                        driverContext,
                        List.of(0, 1)
                    )
                ) {
                    addRaw(partition, driverContext, new double[] { values[position] }, new long[] { timestamps[position] });
                    Block[] intermediate = new Block[intermediateCount];
                    partition.prepareEvaluateIntermediate(selected, context).evaluate(intermediate, 0, selected);
                    try (Page page = new Page(intermediate)) {
                        merged.addIntermediateInput(0, selected, page);
                    }
                }
            }
            for (GroupingAggregatorFunction aggregator : List.of(input, merged)) {
                Block[] output = new Block[1];
                aggregator.prepareEvaluateFinal(selected, context).evaluate(output, 0, selected);
                try (DoubleBlock result = (DoubleBlock) output[0]) {
                    if (expected == null) {
                        assertTrue(result.isNull(0));
                    } else {
                        assertFalse(result.isNull(0));
                        assertEquals(expected, result.getDouble(0), Double.isFinite(expected) ? Math.abs(expected) * 1e-12 : 0);
                    }
                }
            }
        } finally {
            driverContext.finish();
        }
    }

    private static void addRaw(GroupingAggregatorFunction aggregator, DriverContext context, double[] values, long[] timestamps) {
        var factory = context.blockFactory();
        try (
            Page page = new Page(
                factory.newDoubleArrayVector(values, values.length).asBlock(),
                factory.newLongArrayVector(timestamps, timestamps.length).asBlock()
            );
            var groups = factory.newConstantIntVector(0, values.length);
            var input = aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page)
        ) {
            input.add(0, groups);
        }
    }

    private static TimeSeriesGroupingAggregatorEvaluationContext evaluationContext(DriverContext context, long evaluationTime) {
        return new TimeSeriesGroupingAggregatorEvaluationContext(context) {
            @Override
            public long rangeStartInMillis(int groupId) {
                return EPOCH;
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return evaluationTime;
            }

            @Override
            public void forEachGroupInRange(int groupId, long start, long end, IntConsumer action) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int previousGroupId(int groupId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int nextGroupId(int groupId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void computeAdjacentGroupIds() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
