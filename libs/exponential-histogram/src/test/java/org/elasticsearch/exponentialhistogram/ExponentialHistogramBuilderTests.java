/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.core.Releasables;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ExponentialHistogramBuilderTests extends ExponentialHistogramTestCase {

    public void testBuildAndCopyWithAllFieldsSet() {
        ZeroBucket zeroBucket = ZeroBucket.create(1, 2);
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(3, breaker())) {
            builder.zeroBucket(zeroBucket)
                .sum(100.0)
                .min(1.0)
                .max(50.0)
                .setPositiveBucket(2, 10)
                .setPositiveBucket(0, 1)
                .setPositiveBucket(5, 2)
                .setNegativeBucket(-2, 5)
                .setNegativeBucket(1, 2);

            try (ReleasableExponentialHistogram histogram = builder.build()) {
                assertThat(histogram.scale(), equalTo(3));
                assertThat(histogram.zeroBucket(), equalTo(zeroBucket));
                assertThat(histogram.sum(), equalTo(100.0));
                assertThat(histogram.min(), equalTo(1.0));
                assertThat(histogram.max(), equalTo(50.0));
                assertBuckets(histogram.positiveBuckets(), List.of(0L, 2L, 5L), List.of(1L, 10L, 2L));
                assertBuckets(histogram.negativeBuckets(), List.of(-2L, 1L), List.of(5L, 2L));

                // Ensure copy is efficient with only a single allocation and no use of TreeMaps
                AllocationCountingBreaker countingBreaker = new AllocationCountingBreaker(breaker());
                ExponentialHistogramBuilder copyBuilder = ExponentialHistogram.builder(histogram, countingBreaker);
                assertThat(copyBuilder.negativeBuckets, nullValue());
                try (ReleasableExponentialHistogram copy = copyBuilder.build()) {
                    assertThat(copy, equalTo(histogram));
                    assertThat(countingBreaker.numAllocations, equalTo(1));
                }
            }
        }
    }

    public void testBuildWithEstimation() {
        ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, breaker())
            .setPositiveBucket(0, 1)
            .setPositiveBucket(1, 1)
            .setNegativeBucket(0, 4);

        try (ReleasableExponentialHistogram histogram = builder.build()) {
            assertThat(histogram.scale(), equalTo(0));
            assertThat(histogram.zeroBucket(), equalTo(ZeroBucket.minimalEmpty()));
            assertThat(histogram.sum(), equalTo(-1.3333333333333335));
            assertThat(histogram.min(), equalTo(-2.0));
            assertThat(histogram.max(), equalTo(4.0));
            assertBuckets(histogram.positiveBuckets(), List.of(0L, 1L), List.of(1L, 1L));
            assertBuckets(histogram.negativeBuckets(), List.of(0L), List.of(4L));
        }
    }

    public void testDuplicateBucketOverrides() {
        ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, breaker());
        builder.setPositiveBucket(1, 10);
        builder.setNegativeBucket(2, 1);
        builder.setPositiveBucket(1, 100);
        builder.setNegativeBucket(2, 123);
        try (ReleasableExponentialHistogram histogram = builder.build()) {
            assertBuckets(histogram.positiveBuckets(), List.of(1L), List.of(100L));
            assertBuckets(histogram.negativeBuckets(), List.of(2L), List.of(123L));
        }
    }

    public void testEfficientCreationForBucketsInOrder() {
        Consumer<ExponentialHistogramBuilder> generator = builder -> builder.setNegativeBucket(-2, 5)
            .setNegativeBucket(1, 2)
            .setPositiveBucket(0, 1)
            .setPositiveBucket(2, 10)
            .setPositiveBucket(5, 2);

        // Test with a correct count estimate
        AllocationCountingBreaker countingBreaker = new AllocationCountingBreaker(breaker());
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, countingBreaker)) {
            builder.estimatedBucketCount(5);
            generator.accept(builder);
            assertThat(builder.negativeBuckets, nullValue());
            assertThat(builder.positiveBuckets, nullValue());
            try (ReleasableExponentialHistogram histogram = builder.build()) {
                assertThat(countingBreaker.numAllocations, equalTo(1));
                assertBuckets(histogram.positiveBuckets(), List.of(0L, 2L, 5L), List.of(1L, 10L, 2L));
                assertBuckets(histogram.negativeBuckets(), List.of(-2L, 1L), List.of(5L, 2L));
            }
        }
        // Test with a too small estimate, requiring a single resize
        countingBreaker = new AllocationCountingBreaker(breaker());
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, countingBreaker)) {
            builder.estimatedBucketCount(3);
            generator.accept(builder);
            assertThat(builder.negativeBuckets, nullValue());
            assertThat(builder.positiveBuckets, nullValue());
            try (ReleasableExponentialHistogram histogram = builder.build()) {
                assertThat(countingBreaker.numAllocations, equalTo(2));
                assertBuckets(histogram.positiveBuckets(), List.of(0L, 2L, 5L), List.of(1L, 10L, 2L));
                assertBuckets(histogram.negativeBuckets(), List.of(-2L, 1L), List.of(5L, 2L));
            }
        }
    }

    public void testPositiveBucketsOutOfOrder() {
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, breaker())) {
            builder.setPositiveBucket(1, 2);
            builder.setPositiveBucket(0, 1);
            try (ReleasableExponentialHistogram histogram = builder.build()) {
                assertBuckets(histogram.positiveBuckets(), List.of(0L, 1L), List.of(1L, 2L));
            }
        }
    }

    public void testNegativeBucketsOutOfOrder() {
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, breaker())) {
            builder.setNegativeBucket(1, 2);
            builder.setNegativeBucket(0, 1);
            try (ReleasableExponentialHistogram histogram = builder.build()) {
                assertBuckets(histogram.negativeBuckets(), List.of(0L, 1L), List.of(1L, 2L));
            }
        }
    }

    public void testPositiveBucketsBeforeNegativeBuckets() {
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, breaker())) {
            builder.setPositiveBucket(0, 2);
            builder.setNegativeBucket(1, 1);
            try (ReleasableExponentialHistogram histogram = builder.build()) {
                assertBuckets(histogram.negativeBuckets(), List.of(1L), List.of(1L));
                assertBuckets(histogram.positiveBuckets(), List.of(0L), List.of(2L));
            }
        }
    }

    public void testCloseWithoutBuildDoesNotLeak() {
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, breaker())) {
            builder.setPositiveBucket(0, 1);
        }
    }

    public void testMutationsAfterBuild() {
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(3, breaker())) {
            ZeroBucket zeroBucket = ZeroBucket.create(1, 2);
            builder.zeroBucket(zeroBucket)
                .sum(100.0)
                .min(1.0)
                .max(50.0)
                .setPositiveBucket(2, 10)
                .setPositiveBucket(0, 1)
                .setPositiveBucket(5, 2)
                .setNegativeBucket(-2, 5)
                .setNegativeBucket(1, 2);

            ReleasableExponentialHistogram original = builder.build();
            ReleasableExponentialHistogram copy = builder.build();
            assertThat(original, equalTo(copy));
            assertThat(original, not(sameInstance(copy)));

            builder.zeroBucket(ZeroBucket.minimalEmpty())
                .scale(0)
                .sum(10.0)
                .min(0.0)
                .max(2.0)
                .setPositiveBucket(0, 1)
                .setPositiveBucket(2, 1)
                .setPositiveBucket(5, 1)
                .setPositiveBucket(6, 1)
                .setNegativeBucket(-2, 1)
                .setNegativeBucket(-1, 1)
                .setNegativeBucket(1, 1);

            ReleasableExponentialHistogram modified = builder.build();

            assertThat(original.scale(), equalTo(3));
            assertThat(original.zeroBucket(), equalTo(zeroBucket));
            assertThat(original.sum(), equalTo(100.0));
            assertThat(original.min(), equalTo(1.0));
            assertThat(original.max(), equalTo(50.0));
            assertBuckets(original.positiveBuckets(), List.of(0L, 2L, 5L), List.of(1L, 10L, 2L));
            assertBuckets(original.negativeBuckets(), List.of(-2L, 1L), List.of(5L, 2L));

            assertThat(modified.scale(), equalTo(0));
            assertThat(modified.zeroBucket(), equalTo(ZeroBucket.minimalEmpty()));
            assertThat(modified.sum(), equalTo(10.0));
            assertThat(modified.min(), equalTo(0.0));
            assertThat(modified.max(), equalTo(2.0));
            assertBuckets(modified.positiveBuckets(), List.of(0L, 2L, 5L, 6L), List.of(1L, 1L, 1L, 1L));
            assertBuckets(modified.negativeBuckets(), List.of(-2L, -1L, 1L), List.of(1L, 1L, 1L));

            Releasables.close(original, copy, modified);
        }
    }

    private static void assertBuckets(ExponentialHistogram.Buckets buckets, List<Long> indices, List<Long> counts) {
        List<Long> actualIndices = new java.util.ArrayList<>();
        List<Long> actualCounts = new java.util.ArrayList<>();
        BucketIterator it = buckets.iterator();
        while (it.hasNext()) {
            actualIndices.add(it.peekIndex());
            actualCounts.add(it.peekCount());
            it.advance();
        }
        assertThat("Expected bucket indices to match", actualIndices, equalTo(indices));
        assertThat("Expected bucket counts to match", actualCounts, equalTo(counts));
    }

    private static class AllocationCountingBreaker implements ExponentialHistogramCircuitBreaker {
        private final ExponentialHistogramCircuitBreaker delegate;
        int numAllocations = 0;

        private AllocationCountingBreaker(ExponentialHistogramCircuitBreaker delegate) {
            this.delegate = delegate;
        }

        @Override
        public void adjustBreaker(long bytesAllocated) {
            if (bytesAllocated > 0) {
                numAllocations++;
            }
            delegate.adjustBreaker(bytesAllocated);
        }
    }
}
