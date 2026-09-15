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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;

import static org.elasticsearch.compute.aggregation.QuantileStates.DEFAULT_COMPRESSION;
import static org.hamcrest.Matchers.equalTo;

/**
 * In non-finite mode the quantile state must accept the non-finite observations that IEEE-754 arithmetic legitimately
 * produces, and rank them as {@code NaN < -Inf < finite < +Inf}. Strict mode leaves them to the t-digest, which
 * rejects them outright.
 */
public class QuantileStatesTests extends ComputeTestCase {

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
     * With no non-finite observation the two modes must agree, so ordinary data is unaffected by the different rank
     * resolution.
     */
    public void testModesAgreeForFiniteValues() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        List<Double> values = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
        double percentile = randomFrom(0.0, 25.0, 50.0, 75.0, 90.0, 100.0);

        try (
            var nonFinite = new QuantileStates.SingleState(blockFactory.breaker(), percentile, DEFAULT_COMPRESSION, true);
            var strict = new QuantileStates.SingleState(blockFactory.breaker(), percentile, DEFAULT_COMPRESSION)
        ) {
            values.forEach(v -> {
                nonFinite.add(v);
                strict.add(v);
            });
            try (
                Block nonFiniteResult = nonFinite.evaluatePercentile(driverContext);
                Block strictResult = strict.evaluatePercentile(driverContext)
            ) {
                assertEquals(((DoubleBlock) strictResult).getDouble(0), ((DoubleBlock) nonFiniteResult).getDouble(0), 0.0);
            }
        }
    }

    /**
     * Strict mode hands every observation to the t-digest, which rejects a non-finite one. This is what the PromQL
     * translation opts out of; it is also why the tallies cannot simply be kept unconditionally.
     */
    public void testStrictModeRejectsNonFiniteObservation() {
        var blockFactory = blockFactory();
        try (var state = new QuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION)) {
            expectThrows(IllegalArgumentException.class, () -> state.add(Double.POSITIVE_INFINITY));
            expectThrows(IllegalArgumentException.class, () -> state.add(Double.NaN));
        }
    }

    public void testEmptyStateIsNull() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (var state = new QuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION, true)) {
            try (Block result = state.evaluatePercentile(driverContext)) {
                assertTrue(result.isNull(0));
            }
        }
    }

    /**
     * A t-digest cannot carry the non-finite observations, so they ride along in the serialized intermediate state.
     * Without them a partial aggregation computed on a data node would lose its infinities on the way to the
     * coordinating node.
     */
    public void testIntermediateStateCarriesNonFiniteTallies() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Block[] intermediate = new Block[1];

        try (var partial = new QuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION, true)) {
            partial.add(Double.POSITIVE_INFINITY);
            partial.add(Double.POSITIVE_INFINITY);
            partial.add(1.0);
            partial.toIntermediate(intermediate, 0, driverContext);
        }

        try (
            var merged = new QuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION, true);
            Block serialized = intermediate[0]
        ) {
            merged.add(((BytesRefBlock) serialized).getBytesRef(0, new BytesRef()));
            try (Block result = merged.evaluatePercentile(driverContext)) {
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(Double.POSITIVE_INFINITY));
            }
        }
    }

    /**
     * The tallies are only appended in non-finite mode, so a strict intermediate state stays byte-identical to what an
     * older node writes and reads.
     */
    public void testStrictIntermediateStateRoundTrips() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Block[] intermediate = new Block[1];

        try (var partial = new QuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION)) {
            partial.add(1.0);
            partial.add(3.0);
            partial.toIntermediate(intermediate, 0, driverContext);
        }

        try (
            var merged = new QuantileStates.SingleState(blockFactory.breaker(), 50.0, DEFAULT_COMPRESSION);
            Block serialized = intermediate[0]
        ) {
            merged.add(((BytesRefBlock) serialized).getBytesRef(0, new BytesRef()));
            try (Block result = merged.evaluatePercentile(driverContext)) {
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(2.0));
            }
        }
    }

    /**
     * A group whose every observation is non-finite holds a value and must not be rendered as {@code null}, which
     * would drop the series from the result.
     */
    public void testGroupWithOnlyNonFiniteValuesIsNotDropped() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var state = new QuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 50.0, DEFAULT_COMPRESSION, true);
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
            var state = new QuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 50.0, DEFAULT_COMPRESSION, true);
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2)
        ) {
            state.add(1, 2.0);

            try (Block result = state.evaluatePercentile(selected, driverContext)) {
                assertTrue(result.isNull(0));
                assertThat(((DoubleBlock) result).getDouble(1), equalTo(2.0));
            }
        }
    }

    /**
     * The per-group tallies must travel with their own group. Merging a serialized group into a different group id
     * would otherwise attribute the infinities to the wrong series.
     */
    public void testGroupingIntermediateStateKeepsTalliesPerGroup() {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Block[] intermediate = new Block[1];

        try (
            var partial = new QuantileStates.GroupingState(
                blockFactory.breaker(),
                blockFactory.bigArrays(),
                50.0,
                DEFAULT_COMPRESSION,
                true
            );
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2)
        ) {
            partial.add(0, Double.NEGATIVE_INFINITY);
            partial.add(1, 5.0);
            partial.toIntermediate(intermediate, 0, selected, driverContext);
        }

        try (
            var merged = new QuantileStates.GroupingState(
                blockFactory.breaker(),
                blockFactory.bigArrays(),
                50.0,
                DEFAULT_COMPRESSION,
                true
            );
            Block serialized = intermediate[0];
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2)
        ) {
            BytesRefBlock block = (BytesRefBlock) serialized;
            merged.add(0, block.getBytesRef(0, new BytesRef()));
            merged.add(1, block.getBytesRef(1, new BytesRef()));

            try (Block result = merged.evaluatePercentile(selected, driverContext)) {
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(Double.NEGATIVE_INFINITY));
                assertThat(((DoubleBlock) result).getDouble(1), equalTo(5.0));
            }
        }
    }

    private void assertQuantile(double percentile, List<Double> values, double expected) {
        assertQuantile(percentile, values, expected, 0.0);
    }

    private void assertQuantile(double percentile, List<Double> values, double expected, double delta) {
        var blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (var state = new QuantileStates.SingleState(blockFactory.breaker(), percentile, DEFAULT_COMPRESSION, true)) {
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
