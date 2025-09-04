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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramBuilderTests extends ExponentialHistogramTestCase {

    public void testBuildAndCopyWithAllFieldsSet() {
        ZeroBucket zeroBucket = ZeroBucket.create(1, 2);
        ExponentialHistogramBuilder builder = ExponentialHistogram.builder(3, breaker())
            .zeroBucket(zeroBucket)
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

            try (ReleasableExponentialHistogram copy = ExponentialHistogram.builder(histogram, breaker()).build()) {
                assertThat(copy, equalTo(histogram));
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
}
