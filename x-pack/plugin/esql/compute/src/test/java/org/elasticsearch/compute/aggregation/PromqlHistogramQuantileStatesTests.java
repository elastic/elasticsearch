/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.PromqlHistogramQuantileStates.Bucket;
import org.elasticsearch.compute.aggregation.PromqlHistogramQuantileStates.SingleState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PromqlHistogramQuantileStatesTests extends ComputeTestCase {

    public void testBucketQuantileInterpolatesWithinBucket() {
        double result = PromqlHistogramQuantileStates.bucketQuantile(
            0.5,
            List.of(
                new PromqlHistogramQuantileStates.Bucket(1.0, 1.0),
                new PromqlHistogramQuantileStates.Bucket(2.0, 3.0),
                new PromqlHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 4.0)
            )
        );

        assertThat(result, equalTo(1.5));
    }

    public void testBucketQuantileRepairsNonMonotonicBuckets() {
        List<Bucket> brokenBuckets = List.of(
            new Bucket(1.0, 2.0),
            new Bucket(2.0, 1.0),
            new Bucket(3.0, 5.0),
            new Bucket(Double.POSITIVE_INFINITY, 5.0)
        );
        List<Bucket> monotonicBuckets = List.of(
            new Bucket(1.0, 2.0),
            new Bucket(2.0, 2.0),
            new Bucket(3.0, 5.0),
            new Bucket(Double.POSITIVE_INFINITY, 5.0)
        );

        assertBucketQuantileMatchesMonotonicVariant(0.5, brokenBuckets, monotonicBuckets);
        assertThat(PromqlHistogramQuantileStates.bucketQuantile(0.5, monotonicBuckets), closeTo(2.1666666666666665, 1e-12));
    }

    public void testBucketQuantileIgnoresSmallRelativeDeltas() {
        double quantile = 0.500000000000125;
        List<Bucket> brokenBuckets = List.of(
            new Bucket(1.0, 1e15),
            new Bucket(2.0, 1e15 - 500.0),
            new Bucket(3.0, 2e15),
            new Bucket(Double.POSITIVE_INFINITY, 2e15)
        );
        List<Bucket> monotonicBuckets = List.of(
            new Bucket(1.0, 1e15),
            new Bucket(2.0, 1e15),
            new Bucket(3.0, 2e15),
            new Bucket(Double.POSITIVE_INFINITY, 2e15)
        );

        assertBucketQuantileMatchesMonotonicVariant(quantile, brokenBuckets, monotonicBuckets);
        assertThat(PromqlHistogramQuantileStates.bucketQuantile(quantile, monotonicBuckets), closeTo(2.00000000000025, 1e-15));
    }

    public void testBucketQuantileAssertsDuplicateBounds() {
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> PromqlHistogramQuantileStates.bucketQuantile(
                0.5,
                List.of(new Bucket(1.0, 1.0), new Bucket(1.0, 1.0), new Bucket(Double.POSITIVE_INFINITY, 2.0))
            )
        );
        assertThat(e.getMessage(), equalTo("histogram buckets must be pre-aggregated by upper bound"));
    }

    public void testBucketQuantileSortsUnsortedBuckets() {
        List<Bucket> unsortedBuckets = PromqlHistogramQuantileTestHelpers.unsortedCanonicalHistogram();
        List<Bucket> sortedBuckets = PromqlHistogramQuantileTestHelpers.canonicalHistogram();

        assertBucketQuantileMatchesMonotonicVariant(0.5, unsortedBuckets, sortedBuckets);
        assertThat(PromqlHistogramQuantileStates.bucketQuantile(0.5, sortedBuckets), equalTo(1.5));
    }

    public void testBucketQuantileReturnsZeroBucketBoundForQuantileZero() {
        List<Bucket> buckets = List.of(new Bucket(0.0, 5.0), new Bucket(1.0, 10.0), new Bucket(Double.POSITIVE_INFINITY, 10.0));

        assertThat(PromqlHistogramQuantileStates.bucketQuantile(0.0, buckets), equalTo(0.0));
    }

    public void testBucketQuantileReturnsPreviousFiniteBoundForQuantileOne() {
        List<Bucket> buckets = List.of(new Bucket(1.0, 0.4), new Bucket(2.0, 0.6), new Bucket(Double.POSITIVE_INFINITY, 1.0));

        assertThat(PromqlHistogramQuantileStates.bucketQuantile(1.0, buckets), equalTo(2.0));
    }

    public void testParseUpperBound() {
        // "+Inf" is the sentinel terminating every classic histogram. The accepted spellings mirror Go's
        // strconv.ParseFloat (what Prometheus uses to parse `le`): inf/infinity with an optional sign, case-insensitive.
        for (String text : List.of("+Inf", "Inf", "inf", "+INF", "Infinity", "+Infinity")) {
            assertThat(text, PromqlHistogramQuantileStates.parseUpperBound(new BytesRef(text)), equalTo(Double.POSITIVE_INFINITY));
        }
        for (String text : List.of("-Inf", "-inf", "-Infinity")) {
            assertThat(text, PromqlHistogramQuantileStates.parseUpperBound(new BytesRef(text)), equalTo(Double.NEGATIVE_INFINITY));
        }
        assertThat(PromqlHistogramQuantileStates.parseUpperBound(new BytesRef("0.5")), equalTo(0.5));
        assertThat(PromqlHistogramQuantileStates.parseUpperBound(new BytesRef("1000")), equalTo(1000.0));
        assertThat(PromqlHistogramQuantileStates.parseUpperBound(new BytesRef("-7.25")), equalTo(-7.25));
        assertTrue(Double.isNaN(PromqlHistogramQuantileStates.parseUpperBound(new BytesRef("NaN"))));
        // The exception names the offending value, mirroring Prometheus' "bad bucket label" warning.
        NumberFormatException e = expectThrows(
            NumberFormatException.class,
            () -> PromqlHistogramQuantileStates.parseUpperBound(new BytesRef("not_a_number"))
        );
        assertThat(e.getMessage(), equalTo("bucket label [le] has a malformed value of [not_a_number]"));
    }

    public void testCombineSkipsUnparseableBound() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, TestWarningsSource.INSTANCE);

        // Prometheus warns and skips malformed `le` buckets, preserving the valid buckets for the same histogram.
        try (var state = new SingleState(blockFactory.breaker(), 0.5, warnings)) {
            PromqlHistogramQuantileAggregator.combine(state, 2.0, new BytesRef("1.0"));
            PromqlHistogramQuantileAggregator.combine(state, 1.0, new BytesRef("not_a_number"));
            PromqlHistogramQuantileAggregator.combine(state, 4.0, new BytesRef("+Inf"));

            try (DoubleBlock result = (DoubleBlock) state.evaluateFinal(driverContext)) {
                assertThat(result.getDouble(0), equalTo(1.0));
            }
        }
        assertWarnings(
            "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.NumberFormatException: bucket label [le] has a malformed value of [not_a_number]"
        );
    }

    public void testBucketQuantileNaNQuantile() {
        assertTrue(
            Double.isNaN(PromqlHistogramQuantileStates.bucketQuantile(Double.NaN, PromqlHistogramQuantileTestHelpers.canonicalHistogram()))
        );
    }

    public void testSingleStateEmptyReturnsNull() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        try (var state = new SingleState(blockFactory.breaker(), 0.5)) {
            Block[] blocks = new Block[1];
            try {
                state.toIntermediate(blocks, 0, driverContext);
                assertThat(blocks[0].elementType(), equalTo(ElementType.NULL));
                assertTrue(blocks[0].areAllValuesNull());
            } finally {
                Releasables.close(blocks);
            }

            try (Block result = state.evaluateFinal(driverContext)) {
                assertThat(result.elementType(), equalTo(ElementType.NULL));
                assertTrue(result.areAllValuesNull());
            }
        }
    }

    public void testSingleStateNaNEstimateReturnsNull() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        // A histogram without a +Inf bucket cannot produce an estimate, so bucketQuantile returns NaN.
        try (var state = new SingleState(blockFactory.breaker(), 0.5)) {
            state.add(1.0, 1.0);
            state.add(2.0, 2.0);
            assertTrue(
                Double.isNaN(PromqlHistogramQuantileStates.bucketQuantile(0.5, List.of(new Bucket(1.0, 1.0), new Bucket(2.0, 2.0))))
            );

            try (Block result = state.evaluateFinal(driverContext)) {
                assertThat(result.elementType(), equalTo(ElementType.NULL));
                assertTrue(result.areAllValuesNull());
            }
        }
    }

    public void testSingleStateEmitsInfinityForOutOfRangeQuantile() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        // PromQL maps quantiles below 0 to -Inf and above 1 to +Inf; both must round-trip through the DoubleBlock output.
        try (var below = new SingleState(blockFactory.breaker(), -0.1); var above = new SingleState(blockFactory.breaker(), 1.1)) {
            below.add(1.0, 1.0);
            below.add(Double.POSITIVE_INFINITY, 1.0);
            above.add(1.0, 1.0);
            above.add(Double.POSITIVE_INFINITY, 1.0);

            try (Block result = below.evaluateFinal(driverContext)) {
                assertFalse(result.areAllValuesNull());
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(Double.NEGATIVE_INFINITY));
            }
            try (Block result = above.evaluateFinal(driverContext)) {
                assertFalse(result.areAllValuesNull());
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(Double.POSITIVE_INFINITY));
            }
        }
    }

    public void testSingleStateMergesPartialHistograms() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        List<Bucket> shardOne = List.of(new Bucket(1.0, 2.0), new Bucket(2.0, 4.0), new Bucket(Double.POSITIVE_INFINITY, 4.0));
        List<Bucket> shardTwo = List.of(new Bucket(1.0, 1.0), new Bucket(2.0, 2.0), new Bucket(Double.POSITIVE_INFINITY, 2.0));
        List<Bucket> merged = List.of(new Bucket(1.0, 3.0), new Bucket(2.0, 6.0), new Bucket(Double.POSITIVE_INFINITY, 6.0));

        try (
            var shardOneState = new SingleState(blockFactory.breaker(), 0.5);
            var shardTwoState = new SingleState(blockFactory.breaker(), 0.5);
            var combined = new SingleState(blockFactory.breaker(), 0.5)
        ) {
            addAll(shardOneState, shardOne);
            addAll(shardTwoState, shardTwo);
            mergeIntermediate(combined, shardOneState, driverContext);
            mergeIntermediate(combined, shardTwoState, driverContext);

            try (Block result = combined.evaluateFinal(driverContext)) {
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(PromqlHistogramQuantileStates.bucketQuantile(0.5, merged)));
            }
        }
    }

    public void testGroupingStateReturnsNullForUnseenGroups() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        try (var state = new PromqlHistogramQuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 0.5)) {
            state.add(0, 1.0, 1.0);
            state.add(0, 2.0, 3.0);
            state.add(0, Double.POSITIVE_INFINITY, 4.0);

            try (IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 5 }, 2)) {
                Block[] intermediates = new Block[1];
                try {
                    state.toIntermediate(intermediates, 0, selected, driverContext);
                    DoubleBlock serialized = (DoubleBlock) intermediates[0];
                    assertFalse(serialized.isNull(0));
                    assertTrue(serialized.isNull(1));
                } finally {
                    Releasables.close(intermediates);
                }

                try (DoubleBlock results = (DoubleBlock) state.evaluateFinal(selected, driverContext)) {
                    assertThat(results.getDouble(0), equalTo(1.5));
                    assertTrue(results.isNull(1));
                }
            }
        }
    }

    public void testSingleStateCloseReleasesBreakerAccounting() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(4));
        assertThat(breaker.getUsed(), equalTo(0L));

        try (var state = new SingleState(breaker, 0.5)) {
            state.add(1.0, 1.0);
            state.add(2.0, 3.0);
            state.add(Double.POSITIVE_INFINITY, 4.0);
            assertThat(breaker.getUsed(), greaterThan(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSingleStateMergingEqualBoundsDoesNotGrowBreaker() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(4));

        try (var state = new SingleState(breaker, 0.5)) {
            state.add(1.0, 1.0);
            long afterFirst = breaker.getUsed();
            assertThat(afterFirst, greaterThan(0L));
            // Re-adding the same upper bound sums into the existing entry and must not reserve more memory.
            state.add(1.0, 2.0);
            assertThat(breaker.getUsed(), equalTo(afterFirst));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSingleStateMergingEqualBoundsSumsCounts() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        try (var state = new SingleState(blockFactory.breaker(), 0.5)) {
            state.add(1.0, 1.0);
            state.add(1.0, 2.0);
            state.add(Double.POSITIVE_INFINITY, 3.0);
            try (DoubleBlock result = (DoubleBlock) state.evaluateFinal(driverContext)) {
                assertThat(result.getDouble(0), equalTo(0.5));
            }
        }
    }

    public void testSingleStateCrankyCircuitBreaker() {
        CircuitBreaker breaker = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        assertThrows(CircuitBreakingException.class, () -> {
            while (true) {
                try (var state = new SingleState(breaker, 0.5)) {
                    for (int i = 0; i < 10_000; i++) {
                        state.add(i, i + 1.0);
                        state.add(Double.POSITIVE_INFINITY, i + 1.0);
                    }
                }
            }
        });
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBucketQuantileHandlesPrometheusEdgeCases() {
        assertTrue(Double.isNaN(PromqlHistogramQuantileStates.bucketQuantile(0.5, List.of())));
        assertThat(
            PromqlHistogramQuantileStates.bucketQuantile(
                -0.1,
                List.of(
                    new PromqlHistogramQuantileStates.Bucket(1.0, 1.0),
                    new PromqlHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 1.0)
                )
            ),
            equalTo(Double.NEGATIVE_INFINITY)
        );
        assertThat(
            PromqlHistogramQuantileStates.bucketQuantile(
                1.1,
                List.of(
                    new PromqlHistogramQuantileStates.Bucket(1.0, 1.0),
                    new PromqlHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 1.0)
                )
            ),
            equalTo(Double.POSITIVE_INFINITY)
        );
        assertTrue(
            Double.isNaN(
                PromqlHistogramQuantileStates.bucketQuantile(
                    0.5,
                    List.of(new PromqlHistogramQuantileStates.Bucket(1.0, 1.0), new PromqlHistogramQuantileStates.Bucket(2.0, 2.0))
                )
            )
        );
        assertTrue(
            Double.isNaN(
                PromqlHistogramQuantileStates.bucketQuantile(
                    0.5,
                    List.of(
                        new PromqlHistogramQuantileStates.Bucket(1.0, 0.0),
                        new PromqlHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 0.0)
                    )
                )
            )
        );
    }

    public void testGroupingStateIntermediateMergeRoundTrip() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        try (
            var source = new PromqlHistogramQuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 0.5);
            var target = new PromqlHistogramQuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 0.5)
        ) {
            source.add(0, 1.0, 1.0);
            source.add(0, 2.0, 3.0);
            source.add(0, Double.POSITIVE_INFINITY, 4.0);

            source.add(1, 1.0, 2.0);
            source.add(1, 3.0, 4.0);
            source.add(1, Double.POSITIVE_INFINITY, 4.0);

            Block[] intermediates = new Block[1];
            try {
                try (IntVector selected = blockFactory.newIntRangeVector(0, 2)) {
                    source.toIntermediate(intermediates, 0, selected, driverContext);
                }

                DoubleBlock serialized = (DoubleBlock) intermediates[0];
                PromqlHistogramQuantileAggregator.combineIntermediate(target, 0, serialized, 0);
                PromqlHistogramQuantileAggregator.combineIntermediate(target, 1, serialized, 1);
            } finally {
                Releasables.close(intermediates);
            }

            try (
                IntVector selected = blockFactory.newIntRangeVector(0, 2);
                DoubleBlock results = (DoubleBlock) target.evaluateFinal(selected, driverContext)
            ) {
                assertThat(results.getDouble(0), equalTo(1.5));
                assertThat(results.getDouble(1), equalTo(1.0));
            }
        }
    }

    public void testGroupingStateSkipsMalformedBoundForThatGroup() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        try (var state = new PromqlHistogramQuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 0.5)) {
            state.add(0, new BytesRef("1.0"), 2.0);
            state.add(0, new BytesRef("not_a_number"), 1.0);
            state.add(0, new BytesRef("+Inf"), 4.0);

            state.add(1, 1.0, 2.0);
            state.add(1, Double.POSITIVE_INFINITY, 4.0);

            try (
                IntVector selected = blockFactory.newIntRangeVector(0, 2);
                DoubleBlock results = (DoubleBlock) state.evaluateFinal(selected, driverContext)
            ) {
                assertThat(results.getDouble(0), equalTo(1.0));
                assertThat(results.getDouble(1), equalTo(1.0));
            }
        }
    }

    private static void addAll(SingleState state, List<Bucket> buckets) {
        for (Bucket bucket : buckets) {
            state.add(bucket.upperBound(), bucket.count());
        }
    }

    /**
     * Serializes {@code source} to its intermediate {@link DoubleBlock} representation and merges it into {@code target},
     * exercising the same partial-aggregation path the engine uses when combining states across nodes.
     */
    private static void mergeIntermediate(SingleState target, SingleState source, DriverContext driverContext) {
        Block[] blocks = new Block[1];
        try {
            source.toIntermediate(blocks, 0, driverContext);
            target.addIntermediate((DoubleBlock) blocks[0], 0);
        } finally {
            Releasables.close(blocks);
        }
    }

    private static void assertBucketQuantileMatchesMonotonicVariant(
        double quantile,
        List<Bucket> brokenBuckets,
        List<Bucket> monotonicBuckets
    ) {
        assertThat(
            PromqlHistogramQuantileStates.bucketQuantile(quantile, brokenBuckets),
            equalTo(PromqlHistogramQuantileStates.bucketQuantile(quantile, monotonicBuckets))
        );
    }
}
