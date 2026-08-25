/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;

import static org.elasticsearch.compute.aggregation.QuantileStates.DEFAULT_COMPRESSION;
import static org.hamcrest.Matchers.equalTo;

/**
 * The lenient quantile state must accept non-finite observations, which IEEE-754 arithmetic can legitimately produce,
 * and rank them as {@code NaN < -Inf < finite < +Inf}. The strict state's t-digest rejects such observations outright,
 * aborting the whole query rather than producing a result for the series.
 */
public class LenientQuantileStatesTests extends ComputeTestCase {

    public void testAllPositiveInfinity() {
        assertQuantile(
            50.0,
            List.of(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
            Double.POSITIVE_INFINITY
        );
    }

    public void testAllNegativeInfinity() {
        assertQuantile(50.0, List.of(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
    }

    public void testAllNaN() {
        assertQuantile(50.0, List.of(Double.NaN, Double.NaN), Double.NaN);
    }

    /**
     * The highest rank of an all-infinite input is still that infinity; the clamp on the top rank must not fall through
     * into the (empty) finite region.
     */
    public void testTopAndBottomRankOfAllNegativeInfinity() {
        assertQuantile(0.0, List.of(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
        assertQuantile(100.0, List.of(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
    }

    /**
     * {@code NaN} occupies the lowest ranks, so the median of one {@code NaN} and two finite values is a finite value,
     * matching the comparator Prometheus sorts by before selecting a quantile.
     */
    public void testNaNRanksLowest() {
        assertQuantile(0.0, List.of(2.0, Double.NaN, 1.0), Double.NaN);
        assertQuantile(50.0, List.of(2.0, Double.NaN, 1.0), 1.0);
        assertQuantile(100.0, List.of(2.0, Double.NaN, 1.0), 2.0);
    }

    public void testInfinitiesBracketTheFiniteValues() {
        List<Double> values = List.of(Double.NEGATIVE_INFINITY, 1.0, Double.POSITIVE_INFINITY);
        assertQuantile(0.0, values, Double.NEGATIVE_INFINITY);
        assertQuantile(50.0, values, 1.0);
        assertQuantile(100.0, values, Double.POSITIVE_INFINITY);
    }

    /**
     * A rank is resolved by interpolating between the two observations bracketing it in the total order. An infinity
     * that brackets neither side contributes only to the rank, so it must not pull the result: the median of
     * {@code [1, 2, 3, +Inf]} sits halfway between {@code 2} and {@code 3}.
     */
    public void testUnrelatedInfinityDoesNotShiftAFiniteQuantile() {
        assertQuantile(50.0, List.of(1.0, 2.0, 3.0, Double.POSITIVE_INFINITY), 2.5, 0.01);
    }

    /**
     * When {@code +Inf} is one of the two bracketing observations and carries a non-zero weight, the interpolated
     * result is {@code +Inf} rather than the largest finite observation.
     */
    public void testRankBracketedByInfinityYieldsInfinity() {
        assertQuantile(70.0, List.of(1.0, 2.0, 3.0, Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    }

    /**
     * With no non-finite observation the lenient state must agree with the strict one, so ordinary data is unaffected
     * by the different rank resolution.
     */
    public void testMatchesStrictStateForFiniteValues() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        List<Double> values = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
        double percentile = randomFrom(0.0, 25.0, 50.0, 75.0, 90.0, 100.0);

        try (
            var lenient = new LenientQuantileStates.SingleState(blockFactory.breaker(), percentile, DEFAULT_COMPRESSION);
            var strict = new QuantileStates.SingleState(blockFactory.breaker(), percentile, DEFAULT_COMPRESSION)
        ) {
            values.forEach(v -> {
                lenient.add(v);
                strict.add(v);
            });
            try (
                Block lenientResult = lenient.evaluatePercentile(driverContext);
                Block strictResult = strict.evaluatePercentile(driverContext)
            ) {
                assertEquals(((DoubleBlock) strictResult).getDouble(0), ((DoubleBlock) lenientResult).getDouble(0), 0.0);
            }
        }
    }

    public void testEmptyStateIsNull() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (var state = new LenientQuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION)) {
            try (Block result = state.evaluatePercentile(driverContext)) {
                assertTrue(result.isNull(0));
            }
        }
    }

    /**
     * A t-digest cannot carry the non-finite observations, so they ride along the intermediate state as separate
     * tallies. Without them a partial aggregation computed on a data node would lose its infinities on the way to the
     * coordinating node.
     */
    public void testIntermediateStateCarriesNonFiniteTallies() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Block[] intermediate = new Block[4];

        try (var partial = new LenientQuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION)) {
            partial.add(Double.POSITIVE_INFINITY);
            partial.add(Double.POSITIVE_INFINITY);
            partial.add(1.0);
            partial.toIntermediate(intermediate, 0, driverContext);
        }

        try (
            var merged = new LenientQuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION);
            Block digest = intermediate[0];
            Block nan = intermediate[1];
            Block negInf = intermediate[2];
            Block posInf = intermediate[3]
        ) {
            merged.add(
                ((BytesRefBlock) digest).getBytesRef(0, new BytesRef()),
                ((LongBlock) nan).getLong(0),
                ((LongBlock) negInf).getLong(0),
                ((LongBlock) posInf).getLong(0)
            );
            try (Block result = merged.evaluatePercentile(driverContext)) {
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(Double.POSITIVE_INFINITY));
            }
        }
    }

    /**
     * A group whose every observation is non-finite holds a value and must not be rendered as {@code null}, which would
     * drop the series from the result.
     */
    public void testGroupWithOnlyNonFiniteValuesIsNotDropped() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var state = new LenientQuantileStates.GroupingState(
                blockFactory.breaker(),
                blockFactory.bigArrays(),
                50.0,
                DEFAULT_COMPRESSION
            );
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1, 2 }, 3)
        ) {
            state.add(0, Double.POSITIVE_INFINITY);
            state.add(1, Double.NaN);
            state.add(2, 2.0);

            try (Block result = state.evaluatePercentile(selected, driverContext)) {
                assertFalse(result.isNull(0));
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(Double.POSITIVE_INFINITY));
                assertTrue(Double.isNaN(((DoubleBlock) result).getDouble(1)));
                assertThat(((DoubleBlock) result).getDouble(2), equalTo(2.0));
            }
        }
    }

    public void testGroupWithNoObservationsIsNull() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var state = new LenientQuantileStates.GroupingState(
                blockFactory.breaker(),
                blockFactory.bigArrays(),
                50.0,
                DEFAULT_COMPRESSION
            );
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2)
        ) {
            state.add(1, 2.0);

            try (Block result = state.evaluatePercentile(selected, driverContext)) {
                assertTrue(result.isNull(0));
                assertThat(((DoubleBlock) result).getDouble(1), equalTo(2.0));
            }
        }
    }

    private void assertQuantile(double percentile, List<Double> values, double expected) {
        assertQuantile(percentile, values, expected, 0.0);
    }

    private void assertQuantile(double percentile, List<Double> values, double expected, double delta) {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (var state = new LenientQuantileStates.SingleState(blockFactory.breaker(), percentile, DEFAULT_COMPRESSION)) {
            values.forEach(state::add);
            try (Block result = state.evaluatePercentile(driverContext)) {
                assertFalse(result.isNull(0));
                double actual = ((DoubleBlock) result).getDouble(0);
                if (Double.isNaN(expected)) {
                    assertTrue("expected NaN but got [" + actual + "]", Double.isNaN(actual));
                } else {
                    assertEquals(expected, actual, delta);
                }
            }
        }
    }
}
