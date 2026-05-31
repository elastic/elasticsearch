/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

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
        double result = ClassicHistogramQuantileStates.bucketQuantile(
            0.5,
            List.of(
                new ClassicHistogramQuantileStates.Bucket(1.0, 2.0),
                new ClassicHistogramQuantileStates.Bucket(2.0, 1.0),
                new ClassicHistogramQuantileStates.Bucket(3.0, 5.0),
                new ClassicHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 5.0)
            )
        );

        assertThat(result, closeTo(2.1666666666666665, 1e-12));
    }

    public void testBucketQuantileIgnoresSmallRelativeDeltas() {
        double result = ClassicHistogramQuantileStates.bucketQuantile(
            0.500000000000125,
            List.of(
                new ClassicHistogramQuantileStates.Bucket(1.0, 1e15),
                new ClassicHistogramQuantileStates.Bucket(2.0, 1e15 - 500.0),
                new ClassicHistogramQuantileStates.Bucket(3.0, 2e15),
                new ClassicHistogramQuantileStates.Bucket(Double.POSITIVE_INFINITY, 2e15)
            )
        );

        assertThat(result, closeTo(2.00000000000025, 1e-15));
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
}
