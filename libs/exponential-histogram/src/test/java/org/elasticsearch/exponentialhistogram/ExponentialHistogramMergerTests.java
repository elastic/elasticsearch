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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramMergerTests extends ExponentialHistogramTestCase {

    public void testZeroThresholdCollapsesOverlappingBuckets() {
        ExponentialHistogram first = createAutoReleasedHistogram(b -> b.zeroBucket(ZeroBucket.create(2.0001, 10)));

        ExponentialHistogram second = createAutoReleasedHistogram(
            b -> b.scale(0)
                .setNegativeBucket(0, 1)
                .setNegativeBucket(1, 1)
                .setNegativeBucket(2, 7)
                .setPositiveBucket(0, 1)
                .setPositiveBucket(1, 1)
                .setPositiveBucket(2, 42)
        );

        ExponentialHistogram mergeResult = mergeWithMinimumScale(100, 0, first, second);

        assertThat(mergeResult.zeroBucket().zeroThreshold(), equalTo(4.0));
        assertThat(mergeResult.zeroBucket().count(), equalTo(14L));
        assertThat(mergeResult.zeroBucket().isIndexBased(), equalTo(true));

        // only the (4, 8] bucket should be left
        assertThat(mergeResult.scale(), equalTo(0));

        BucketIterator negBuckets = mergeResult.negativeBuckets().iterator();
        assertThat(negBuckets.peekIndex(), equalTo(2L));
        assertThat(negBuckets.peekCount(), equalTo(7L));
        negBuckets.advance();
        assertThat(negBuckets.hasNext(), equalTo(false));

        BucketIterator posBuckets = mergeResult.positiveBuckets().iterator();
        assertThat(posBuckets.peekIndex(), equalTo(2L));
        assertThat(posBuckets.peekCount(), equalTo(42L));
        posBuckets.advance();
        assertThat(posBuckets.hasNext(), equalTo(false));

        // ensure buckets of the accumulated histogram are collapsed too if needed
        ExponentialHistogram third = createAutoReleasedHistogram(b -> b.zeroBucket(ZeroBucket.create(45.0, 1)));

        mergeResult = mergeWithMinimumScale(100, 0, mergeResult, third);
        assertThat(mergeResult.zeroBucket().zeroThreshold(), closeTo(45.0, 0.000001));
        assertThat(mergeResult.zeroBucket().count(), equalTo(1L + 14L + 42L + 7L));
        assertThat(mergeResult.zeroBucket().isIndexBased(), equalTo(false));
        assertThat(mergeResult.positiveBuckets().iterator().hasNext(), equalTo(false));
        assertThat(mergeResult.negativeBuckets().iterator().hasNext(), equalTo(false));
    }

    public void testEmptyZeroBucketIgnored() {
        ExponentialHistogram first = createAutoReleasedHistogram(
            b -> b.zeroBucket(ZeroBucket.create(2.0, 10)).scale(0).setPositiveBucket(2, 42)
        );

        ExponentialHistogram second = createAutoReleasedHistogram(b -> b.zeroBucket(ZeroBucket.create(100.0, 0)));
        ExponentialHistogram mergeResult = mergeWithMinimumScale(100, 0, first, second);

        assertThat(mergeResult.zeroBucket().zeroThreshold(), equalTo(2.0));
        assertThat(mergeResult.zeroBucket().count(), equalTo(10L));

        BucketIterator posBuckets = mergeResult.positiveBuckets().iterator();
        assertThat(posBuckets.peekIndex(), equalTo(2L));
        assertThat(posBuckets.peekCount(), equalTo(42L));
        posBuckets.advance();
        assertThat(posBuckets.hasNext(), equalTo(false));
    }

    public void testAggregatesCorrectness() {
        double[] firstValues = randomDoubles(100).map(val -> val * 2 - 1).toArray();
        double[] secondValues = randomDoubles(50).map(val -> val * 2 - 1).toArray();
        double correctSum = Arrays.stream(firstValues).sum() + Arrays.stream(secondValues).sum();
        double correctMin = DoubleStream.concat(Arrays.stream(firstValues), Arrays.stream(secondValues)).min().getAsDouble();
        double correctMax = DoubleStream.concat(Arrays.stream(firstValues), Arrays.stream(secondValues)).max().getAsDouble();
        try (
            // Merge some empty histograms too to test that code path
            ReleasableExponentialHistogram merged = ExponentialHistogram.merge(
                4,
                breaker(),
                ExponentialHistogram.empty(),
                createAutoReleasedHistogram(10, firstValues),
                createAutoReleasedHistogram(20, secondValues),
                ExponentialHistogram.empty()
            )
        ) {
            assertThat(merged.sum(), closeTo(correctSum, 0.000001));
            assertThat(merged.min(), equalTo(correctMin));
            assertThat(merged.max(), equalTo(correctMax));
        }
    }

    public void testMinimumBucketCountBounded() {
        try {
            ExponentialHistogram.merge(3, breaker(), ExponentialHistogram.empty(), ExponentialHistogram.empty());
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("limit must be at least 4"));
        }
    }

    /**
     * Verify that the resulting histogram is independent of the order of elements and therefore merges performed.
     */
    public void testMergeOrderIndependence() {
        List<Double> values = IntStream.range(0, 10_000)
            .mapToDouble(i -> i < 17 ? 0 : (-1 + 2 * randomDouble()) * Math.pow(10, randomIntBetween(-4, 4)))
            .boxed()
            .collect(Collectors.toCollection(ArrayList::new));

        ReleasableExponentialHistogram reference = ExponentialHistogram.create(
            20,
            breaker(),
            values.stream().mapToDouble(Double::doubleValue).toArray()
        );
        autoReleaseOnTestEnd(reference);

        for (int i = 0; i < 100; i++) {
            Collections.shuffle(values, random());
            double[] vals = values.stream().mapToDouble(Double::doubleValue).toArray();
            try (ReleasableExponentialHistogram shuffled = ExponentialHistogram.create(20, breaker(), vals)) {
                assertThat("Expected same scale", shuffled.scale(), equalTo(reference.scale()));
                assertThat(
                    "Expected same threshold for zero-bucket",
                    shuffled.zeroBucket().zeroThreshold(),
                    equalTo(reference.zeroBucket().zeroThreshold())
                );
                assertThat("Expected same count for zero-bucket", shuffled.zeroBucket().count(), equalTo(reference.zeroBucket().count()));
                assertBucketsEqual(shuffled.negativeBuckets(), reference.negativeBuckets());
                assertBucketsEqual(shuffled.positiveBuckets(), reference.positiveBuckets());
            }
        }
    }

    public void testMemoryAccounting() {
        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        try (
            ExponentialHistogramMerger.Factory factory = ExponentialHistogramMerger.createFactory(100, breaker(esBreaker));
            ExponentialHistogramMerger merger = factory.createMerger()
        ) {

            long emptyMergerSize = merger.ramBytesUsed() + factory.ramBytesUsed();
            assertThat(emptyMergerSize, greaterThan(0L));
            assertThat(esBreaker.getUsed(), equalTo(emptyMergerSize));

            merger.add(createAutoReleasedHistogram(10, 1.0, 2.0, 3.0));

            long singleBufferSize = merger.ramBytesUsed() + factory.ramBytesUsed();
            assertThat(singleBufferSize, greaterThan(emptyMergerSize));
            assertThat(esBreaker.getUsed(), equalTo(singleBufferSize));

            merger.add(createAutoReleasedHistogram(10, 1.0, 2.0, 3.0));

            long doubleBufferSize = merger.ramBytesUsed() + factory.ramBytesUsed();
            assertThat(doubleBufferSize, greaterThan(singleBufferSize));
            assertThat(esBreaker.getUsed(), equalTo(doubleBufferSize));

            ReleasableExponentialHistogram result = merger.getAndClear();

            assertThat(merger.ramBytesUsed() + factory.ramBytesUsed(), equalTo(singleBufferSize));
            assertThat(esBreaker.getUsed(), equalTo(doubleBufferSize));

            result.close();
            assertThat(esBreaker.getUsed(), equalTo(singleBufferSize));
        }
        assertThat(esBreaker.getUsed(), equalTo(0L));
    }

    public void testDifference() {
        ExponentialHistogram a = ExponentialHistogramTestUtils.randomHistogram();
        ExponentialHistogram b = ExponentialHistogramTestUtils.randomHistogram();
        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        try (
            ExponentialHistogramMerger.Factory factory = ExponentialHistogramMerger.createFactory(100, breaker(esBreaker));
            ExponentialHistogramMerger merged = factory.createMerger();
            ExponentialHistogramMerger diffMerger = factory.createMerger();
        ) {

            merged.add(a);
            merged.add(b);

            ExponentialHistogram reference;
            try (ExponentialHistogramMerger temp = factory.createMerger()) {
                // merge A with dummy to (a) reduce the scale and (b) increase the zero threshold if required
                ExponentialHistogram dummy = ExponentialHistogram.builder(merged.get().scale(), ExponentialHistogramCircuitBreaker.noop())
                    .zeroBucket(merged.get().zeroBucket().withCount(1))
                    .build();
                temp.add(dummy);
                temp.add(a);

                ZeroBucket zeroBucket = temp.get().zeroBucket();
                zeroBucket = zeroBucket.withCount(zeroBucket.count() - 1);

                reference = ExponentialHistogram.builder(temp.get(), ExponentialHistogramCircuitBreaker.noop())
                    .zeroBucket(zeroBucket)
                    .build();
            }

            // Add a histogram to the merger just to make sure it is properly cleared when setToDifference is called
            diffMerger.add(ExponentialHistogramTestUtils.randomHistogram());

            diffMerger.setToDifference(merged.get(), b);
            ExponentialHistogram diff = diffMerger.get();

            if (reference.zeroBucket().count() == 0) {
                assertThat(diff.zeroBucket().count(), equalTo(0L));
            } else {
                assertThat(diff.zeroBucket(), equalTo(reference.zeroBucket()));
            }
            assertBucketsEqual(diff.negativeBuckets(), reference.negativeBuckets());
            assertBucketsEqual(diff.positiveBuckets(), reference.positiveBuckets());

            assertThat(diff.valueCount(), equalTo(a.valueCount()));
            assertThat(diff.sum(), closeTo(a.sum(), 0.000001));

            if (a.valueCount() > 0) {
                if (b.valueCount() == 0 || a.max() > b.max()) {
                    // maximum should be preserved
                    assertThat(diff.max(), equalTo(a.max()));
                } else {
                    double estimatedMax = ExponentialHistogramUtils.estimateMax(
                        reference.zeroBucket(),
                        reference.negativeBuckets(),
                        reference.positiveBuckets()
                    ).getAsDouble();
                    // exact maximum of a was lost during merging, the result should have a sane estimate.
                    assertThat(diff.max(), greaterThanOrEqualTo(a.max()));
                    assertThat(diff.max(), lessThanOrEqualTo(b.max()));
                    assertThat(diff.max(), lessThanOrEqualTo(estimatedMax));
                }

                if (b.valueCount() == 0 || a.min() < b.min()) {
                    // minimum should be preserved
                    assertThat(diff.min(), equalTo(a.min()));
                } else {
                    double estimatedMin = ExponentialHistogramUtils.estimateMin(
                        reference.zeroBucket(),
                        reference.negativeBuckets(),
                        reference.positiveBuckets()
                    ).getAsDouble();
                    // exact minimum of a was lost during merging, the result should have a sane estimate.
                    assertThat(diff.min(), lessThanOrEqualTo(a.min()));
                    assertThat(diff.min(), greaterThanOrEqualTo(b.min()));
                    assertThat(diff.min(), greaterThanOrEqualTo(estimatedMin));
                }
            } else {
                assertThat(diff.min(), equalTo(Double.NaN));
                assertThat(diff.max(), equalTo(Double.NaN));
            }
        }
    }

    public void testDifferenceFailsWhenACountLessThanBCount() {
        ExponentialHistogram a = ExponentialHistogram.create(100, breaker(), 1.0, 2.0);
        autoReleaseOnTestEnd((ReleasableExponentialHistogram) a);
        ExponentialHistogram b = ExponentialHistogram.create(100, breaker(), 1.0, 2.0, 3.0);
        autoReleaseOnTestEnd((ReleasableExponentialHistogram) b);

        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merger.setToDifference(a, b));
            assertThat(e.getMessage(), containsString("a.count < b.count"));
        }
    }

    public void testDifferenceFailsWhenAScaleGreaterThanBScale() {
        ExponentialHistogram a = createAutoReleasedHistogram(b -> b.scale(10).setPositiveBucket(100, 5).min(1.0).max(10.0));
        ExponentialHistogram b = createAutoReleasedHistogram(b2 -> b2.scale(5).setPositiveBucket(3, 2).min(1.0).max(10.0));

        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merger.setToDifference(a, b));
            assertThat(e.getMessage(), containsString("a.scale > b.scale"));
        }
    }

    public void testDifferenceFailsWhenAMinGreaterThanBMin() {
        ExponentialHistogram a = ExponentialHistogram.create(100, breaker(), 5.0, 10.0);
        autoReleaseOnTestEnd((ReleasableExponentialHistogram) a);
        ExponentialHistogram b = ExponentialHistogram.create(100, breaker(), 1.0);
        autoReleaseOnTestEnd((ReleasableExponentialHistogram) b);

        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merger.setToDifference(a, b));
            assertThat(e.getMessage(), containsString("a.min > b.min"));
        }
    }

    public void testDifferenceFailsWhenAMaxLessThanBMax() {
        ExponentialHistogram a = ExponentialHistogram.create(100, breaker(), 1.0, 5.0);
        autoReleaseOnTestEnd((ReleasableExponentialHistogram) a);
        ExponentialHistogram b = ExponentialHistogram.create(100, breaker(), 10.0);
        autoReleaseOnTestEnd((ReleasableExponentialHistogram) b);

        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merger.setToDifference(a, b));
            assertThat(e.getMessage(), containsString("a.max < b.max"));
        }
    }

    public void testDifferenceFailsWhenAZeroThresholdLessThanBZeroThreshold() {
        ExponentialHistogram a = createAutoReleasedHistogram(
            b -> b.zeroBucket(ZeroBucket.create(1.0, 5)).setPositiveBucket(10, 3).min(0.5).max(10.0)
        );
        ExponentialHistogram b = createAutoReleasedHistogram(
            b2 -> b2.zeroBucket(ZeroBucket.create(5.0, 2)).setPositiveBucket(10, 1).min(0.5).max(10.0)
        );

        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merger.setToDifference(a, b));
            assertThat(e.getMessage(), containsString("a.zeroThreshold < b.zeroThreshold"));
        }
    }

    public void testDifferenceFailsWhenAZeroCountLessThanBZeroCount() {
        ExponentialHistogram a = createAutoReleasedHistogram(
            b -> b.zeroBucket(ZeroBucket.create(5.0, 2)).setPositiveBucket(10, 5).min(0.1).max(10.0)
        );
        ExponentialHistogram b = createAutoReleasedHistogram(
            b2 -> b2.zeroBucket(ZeroBucket.create(5.0, 5)).setPositiveBucket(10, 1).min(0.1).max(10.0)
        );

        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merger.setToDifference(a, b));
            assertThat(e.getMessage(), containsString("a.zeroCount < b.zeroCount"));
        }
    }

    public void testDifferenceFailsWhenBucketCountInALessThanB() {
        ExponentialHistogram a = createAutoReleasedHistogram(
            b -> b.scale(0).setPositiveBucket(5, 2).setPositiveBucket(6, 5).min(1.0).max(100.0)
        );
        ExponentialHistogram b = createAutoReleasedHistogram(b2 -> b2.scale(0).setPositiveBucket(5, 5).min(1.0).max(100.0));

        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merger.setToDifference(a, b));
            assertThat(e.getMessage(), containsString("A has a smaller count for the same bucket than B"));
        }
    }

    private void assertBucketsEqual(ExponentialHistogram.Buckets bucketsA, ExponentialHistogram.Buckets bucketsB) {
        BucketIterator itA = bucketsA.iterator();
        BucketIterator itB = bucketsB.iterator();
        assertThat("Expecting both set of buckets to be empty or non-empty", itA.hasNext(), equalTo(itB.hasNext()));
        while (itA.hasNext() && itB.hasNext()) {
            assertThat(itA.peekIndex(), equalTo(itB.peekIndex()));
            assertThat(itA.peekCount(), equalTo(itB.peekCount()));
            assertThat("The number of buckets is different", itA.hasNext(), equalTo(itB.hasNext()));
            itA.advance();
            itB.advance();
        }
    }

    private ExponentialHistogram mergeWithMinimumScale(int bucketCount, int scale, ExponentialHistogram... histograms) {
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.createWithMaxScale(bucketCount, scale, breaker())) {
            Arrays.stream(histograms).forEach(merger::add);
            ReleasableExponentialHistogram result = merger.getAndClear();
            autoReleaseOnTestEnd(result);
            return result;
        }
    }

}
