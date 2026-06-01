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
import org.elasticsearch.compute.aggregation.ClassicHistogramQuantileStates.Bucket;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ClassicHistogramQuantileStatesTests extends ComputeTestCase {

    public void testBucketQuantileInterpolatesWithinBucket() {
        double result = ClassicHistogramQuantileStates.bucketQuantile(
            0.5,
            List.of(
                new ClassicHistogramQuantileStates.Bucket(1.0, 1.0),
                new ClassicHistogramQuantileStates.Bucket(2.0, 3.0),
                new ClassicHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 4.0)
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
        assertThat(ClassicHistogramQuantileStates.bucketQuantile(0.5, monotonicBuckets), closeTo(2.1666666666666665, 1e-12));
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
        assertThat(ClassicHistogramQuantileStates.bucketQuantile(quantile, monotonicBuckets), closeTo(2.00000000000025, 1e-15));
    }

    public void testBucketQuantileCoalescesDuplicateBounds() {
        List<Bucket> duplicateBoundBuckets = List.of(new Bucket(1.0, 1.0), new Bucket(1.0, 1.0), new Bucket(Double.POSITIVE_INFINITY, 2.0));
        List<Bucket> coalescedBuckets = List.of(new Bucket(1.0, 2.0), new Bucket(Double.POSITIVE_INFINITY, 2.0));

        assertBucketQuantileMatchesMonotonicVariant(0.5, duplicateBoundBuckets, coalescedBuckets);
        assertThat(ClassicHistogramQuantileStates.bucketQuantile(0.5, coalescedBuckets), equalTo(0.5));
    }

    public void testBucketQuantileSortsUnsortedBuckets() {
        List<Bucket> unsortedBuckets = ClassicHistogramQuantileTestHelpers.unsortedCanonicalHistogram();
        List<Bucket> sortedBuckets = ClassicHistogramQuantileTestHelpers.canonicalHistogram();

        assertBucketQuantileMatchesMonotonicVariant(0.5, unsortedBuckets, sortedBuckets);
        assertThat(ClassicHistogramQuantileStates.bucketQuantile(0.5, sortedBuckets), equalTo(1.5));
    }

    public void testBucketQuantileReturnsZeroBucketBoundForQuantileZero() {
        List<Bucket> buckets = List.of(new Bucket(0.0, 5.0), new Bucket(1.0, 10.0), new Bucket(Double.POSITIVE_INFINITY, 10.0));

        assertThat(ClassicHistogramQuantileStates.bucketQuantile(0.0, buckets), equalTo(0.0));
    }

    public void testBucketQuantileReturnsPreviousFiniteBoundForQuantileOne() {
        List<Bucket> buckets = List.of(new Bucket(1.0, 0.4), new Bucket(2.0, 0.6), new Bucket(Double.POSITIVE_INFINITY, 1.0));

        assertThat(ClassicHistogramQuantileStates.bucketQuantile(1.0, buckets), equalTo(2.0));
    }

    public void testBucketQuantileNaNQuantile() {
        assertTrue(
            Double.isNaN(
                ClassicHistogramQuantileStates.bucketQuantile(Double.NaN, ClassicHistogramQuantileTestHelpers.canonicalHistogram())
            )
        );
    }

    public void testSerializeDeserializeRoundTrip() {
        List<Bucket> buckets = ClassicHistogramQuantileTestHelpers.canonicalHistogram();
        BytesRef serialized = ClassicHistogramQuantileStates.serializeBuckets(buckets);
        List<Bucket> deserialized = ClassicHistogramQuantileStates.deserializeBuckets(serialized);

        assertBucketQuantileMatchesMonotonicVariant(0.5, buckets, deserialized);
        assertThat(deserialized, equalTo(buckets));
    }

    public void testSingleStateEmptyReturnsNull() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        try (var state = new ClassicHistogramQuantileStates.SingleState(blockFactory.breaker(), 0.5)) {
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

    public void testSingleStateMergesPartialHistograms() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        List<Bucket> shardOne = List.of(new Bucket(1.0, 2.0), new Bucket(2.0, 4.0), new Bucket(Double.POSITIVE_INFINITY, 4.0));
        List<Bucket> shardTwo = List.of(new Bucket(1.0, 1.0), new Bucket(2.0, 2.0), new Bucket(Double.POSITIVE_INFINITY, 2.0));
        List<Bucket> merged = List.of(new Bucket(1.0, 3.0), new Bucket(2.0, 6.0), new Bucket(Double.POSITIVE_INFINITY, 6.0));

        try (var state = new ClassicHistogramQuantileStates.SingleState(blockFactory.breaker(), 0.5)) {
            state.add(ClassicHistogramQuantileStates.serializeBuckets(shardOne));
            state.add(ClassicHistogramQuantileStates.serializeBuckets(shardTwo));

            try (Block result = state.evaluateFinal(driverContext)) {
                assertThat(((DoubleBlock) result).getDouble(0), equalTo(ClassicHistogramQuantileStates.bucketQuantile(0.5, merged)));
            }
        }
    }

    public void testGroupingStateReturnsNullForUnseenGroups() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        try (var state = new ClassicHistogramQuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 0.5)) {
            state.add(0, 1.0, 1.0);
            state.add(0, 2.0, 3.0);
            state.add(0, Double.POSITIVE_INFINITY, 4.0);

            try (IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 5 }, 2)) {
                Block[] intermediates = new Block[1];
                try {
                    state.toIntermediate(intermediates, 0, selected, driverContext);
                    BytesRefBlock serialized = (BytesRefBlock) intermediates[0];
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

        try (var state = new ClassicHistogramQuantileStates.SingleState(breaker, 0.5)) {
            state.add(1.0, 1.0);
            state.add(2.0, 3.0);
            state.add(Double.POSITIVE_INFINITY, 4.0);
            assertThat(breaker.getUsed(), greaterThan(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSingleStateCrankyCircuitBreaker() {
        CircuitBreaker breaker = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        assertThrows(CircuitBreakingException.class, () -> {
            while (true) {
                try (var state = new ClassicHistogramQuantileStates.SingleState(breaker, 0.5)) {
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
        assertTrue(Double.isNaN(ClassicHistogramQuantileStates.bucketQuantile(0.5, List.of())));
        assertThat(
            ClassicHistogramQuantileStates.bucketQuantile(
                -0.1,
                List.of(
                    new ClassicHistogramQuantileStates.Bucket(1.0, 1.0),
                    new ClassicHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 1.0)
                )
            ),
            equalTo(Double.NEGATIVE_INFINITY)
        );
        assertThat(
            ClassicHistogramQuantileStates.bucketQuantile(
                1.1,
                List.of(
                    new ClassicHistogramQuantileStates.Bucket(1.0, 1.0),
                    new ClassicHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 1.0)
                )
            ),
            equalTo(Double.POSITIVE_INFINITY)
        );
        assertTrue(
            Double.isNaN(
                ClassicHistogramQuantileStates.bucketQuantile(
                    0.5,
                    List.of(new ClassicHistogramQuantileStates.Bucket(1.0, 1.0), new ClassicHistogramQuantileStates.Bucket(2.0, 2.0))
                )
            )
        );
        assertTrue(
            Double.isNaN(
                ClassicHistogramQuantileStates.bucketQuantile(
                    0.5,
                    List.of(
                        new ClassicHistogramQuantileStates.Bucket(1.0, 0.0),
                        new ClassicHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 0.0)
                    )
                )
            )
        );
    }

    public void testGroupingStateSerializedMergeRoundTrip() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);

        try (
            var source = new ClassicHistogramQuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 0.5);
            var target = new ClassicHistogramQuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 0.5)
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

                BytesRef scratch = new BytesRef();
                BytesRefBlock serialized = (BytesRefBlock) intermediates[0];
                target.add(0, serialized.getBytesRef(0, scratch));
                target.add(1, serialized.getBytesRef(1, scratch));
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

    private static void assertBucketQuantileMatchesMonotonicVariant(
        double quantile,
        List<Bucket> brokenBuckets,
        List<Bucket> monotonicBuckets
    ) {
        assertThat(
            ClassicHistogramQuantileStates.bucketQuantile(quantile, brokenBuckets),
            equalTo(ClassicHistogramQuantileStates.bucketQuantile(quantile, monotonicBuckets))
        );
    }
}
