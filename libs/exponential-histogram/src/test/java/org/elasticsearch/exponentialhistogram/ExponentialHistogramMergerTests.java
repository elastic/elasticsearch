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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

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

    /**
     * Subtracts random histograms, which means they are typically not cumulative.
     */
    public void testDifferenceForRandomHistos() {
        ExponentialHistogram a = ExponentialHistogramTestUtils.randomHistogram();
        ExponentialHistogram b = ExponentialHistogramTestUtils.randomHistogram();
        // Make sure that a.count() > b.count
        if (a.valueCount() <= b.valueCount()) {
            ExponentialHistogram temp = a;
            a = b;
            b = temp;
        }

        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        try (
            ExponentialHistogramMerger.Factory factory = ExponentialHistogramMerger.createFactory(1000, breaker(esBreaker));
            ExponentialHistogramMerger diffMerger = factory.createMerger();
        ) {
            boolean isCumulative = diffMerger.setToDifference(a, b);

            ExponentialHistogram diff = diffMerger.get();
            assertThat(diff.valueCount(), equalTo(a.valueCount() - b.valueCount()));

            if (a.valueCount() == b.valueCount()) {
                assertThat(diff, equalTo(ExponentialHistogram.empty()));
            } else {
                assertThat(diff.sum(), closeTo(a.sum() - b.sum(), 0.000001));
                assertThat(diff.min(), greaterThanOrEqualTo(a.min()));
                assertThat(diff.max(), lessThanOrEqualTo(a.max()));
                assertThat(diff.min(), lessThanOrEqualTo(diff.max()));
                // it is unlikely for the two random histograms to be cumulative, unless b is empty.
                // however, to prevent flakes and to be sure that isCumulative is actually correct we still
                // validate both cases
                if (isCumulative) {
                    int scale = Math.min(a.scale(), b.scale());
                    ExponentialHistogramMerger merger = ExponentialHistogramMerger.createWithMaxScale(
                        1000,
                        scale,
                        ExponentialHistogramCircuitBreaker.noop()
                    );
                    merger.add(diff);
                    merger.add(b);
                    ExponentialHistogram merged = merger.get();
                    // adjust the sum of merged to exactly match a
                    ExponentialHistogramBuilder correctedBuilder = ExponentialHistogram.builder(
                        merged,
                        ExponentialHistogramCircuitBreaker.noop()
                    );
                    correctedBuilder.sum(a.sum());
                    if (diff.zeroBucket().count() == 0) {
                        // zero threshold is lost in this case, preserve it
                        correctedBuilder.zeroBucket(a.zeroBucket().withCount(merged.zeroBucket().count()));
                    }
                    assertThat(correctedBuilder.build(), equalTo(a));
                } else {
                    // Make sure that all buckets that are present in the result correspond to buckets that happened to be cumulative
                    assertOnlyCumulativeBucketsPresent(a.negativeBuckets(), b.negativeBuckets(), diff.negativeBuckets());
                    assertOnlyCumulativeBucketsPresent(a.positiveBuckets(), b.positiveBuckets(), diff.positiveBuckets());
                }
            }
        }
    }

    private static void assertOnlyCumulativeBucketsPresent(
        ExponentialHistogram.Buckets bucketsA,
        ExponentialHistogram.Buckets bucketsB,
        ExponentialHistogram.Buckets diffBuckets
    ) {
        Map<Long, Long> rawDiffByIndex = new HashMap<>();
        MergingBucketIterator rawDiff = new MergingBucketIterator(
            bucketsA.iterator(),
            bucketsB.iterator(),
            diffBuckets.iterator().scale(),
            (aCount, bCount) -> aCount - bCount
        );
        while (rawDiff.hasNext()) {
            rawDiffByIndex.put(rawDiff.peekIndex(), rawDiff.peekCount());
            rawDiff.advance();
        }

        BucketIterator diffIt = diffBuckets.iterator();
        while (diffIt.hasNext()) {
            long diffIndex = diffIt.peekIndex();
            Long rawCount = rawDiffByIndex.get(diffIndex);
            assertThat("diff bucket at index " + diffIndex + " has no corresponding bucket in raw subtraction", rawCount, notNullValue());
            assertThat("diff bucket at index " + diffIndex + " exists but raw a.count <= b.count", rawCount, greaterThan(0L));
            diffIt.advance();
        }
    }

    /**
     * Tests the happy path when the subtracted histograms are actually cumulative.
     */
    public void testDifferenceForCumulativeHistos() {
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

            boolean areHistosCumulative = diffMerger.setToDifference(merged.get(), b);
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
            assertThat(areHistosCumulative, equalTo(true));
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

    public void testDifferenceWhenAScaleGreaterThanBScale() {
        var noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        // At scale 0: a's bucket 3>>1=1 with count 5, b's bucket 1 with count 2. Diff: bucket 1 -> 3.
        ExponentialHistogram a = ExponentialHistogram.builder(1, noopBreaker).setPositiveBucket(3, 5).min(2.0).max(4.0).build();
        ExponentialHistogram b = ExponentialHistogram.builder(0, noopBreaker).setPositiveBucket(1, 2).min(2.0).max(4.0).build();

        ExponentialHistogram expected = ExponentialHistogram.builder(0, noopBreaker)
            .setPositiveBucket(1, 3)
            .sum(a.sum() - b.sum())
            .min(2.0)
            .max(4.0)
            .build();
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            boolean isCumulative = merger.setToDifference(a, b);
            assertThat(isCumulative, equalTo(false));
            assertThat(merger.get(), equalTo(expected));
        }
    }

    public void testDifferenceWhenAMinGreaterThanBMin() {
        var noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        // a.min(2.0) > b.min(1.0): not cumulative. No overlapping buckets, desired=1, raw=3, scaled to 1.
        ExponentialHistogram a = ExponentialHistogram.builder(0, noopBreaker).setPositiveBucket(1, 3).min(2.0).max(4.0).build();
        ExponentialHistogram b = ExponentialHistogram.builder(0, noopBreaker).setPositiveBucket(0, 2).min(1.0).max(2.0).build();

        ExponentialHistogram expected = ExponentialHistogram.builder(0, noopBreaker)
            .setPositiveBucket(1, 1)
            .sum(a.sum() - b.sum())
            .min(2.0)
            .max(4.0)
            .build();
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            boolean isCumulative = merger.setToDifference(a, b);
            assertThat(isCumulative, equalTo(false));
            assertThat(merger.get(), equalTo(expected));
        }
    }

    public void testDifferenceWhenAMaxLessThanBMax() {
        var noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        // a.max(4.0) < b.max(8.0): not cumulative. No overlapping buckets, desired=2, raw=3, scaled.
        ExponentialHistogram a = ExponentialHistogram.builder(0, noopBreaker)
            .setPositiveBucket(1, 2)
            .setPositiveBucket(2, 1)
            .min(1.0)
            .max(4.0)
            .build();
        ExponentialHistogram b = ExponentialHistogram.builder(0, noopBreaker).setPositiveBucket(3, 1).min(4.0).max(8.0).build();

        ExponentialHistogram expected = ExponentialHistogram.builder(0, noopBreaker)
            .setPositiveBucket(1, 1)
            .setPositiveBucket(2, 1)
            .sum(a.sum() - b.sum())
            .min(1.0)
            .max(4.0)
            .build();
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            boolean isCumulative = merger.setToDifference(a, b);
            assertThat(isCumulative, equalTo(false));
            assertThat(merger.get(), equalTo(expected));
        }
    }

    public void testDifferenceWhenAZeroThresholdLessThanBZeroThreshold() {
        var noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        // B's zero threshold adjusted to A's (1.0). No B buckets collapsed.
        // Raw diff: zero -> max(0, 5-2)=3, pos(1) -> max(0, 3-1)=2. Total=5=8-3. No scaling.
        ExponentialHistogram a = ExponentialHistogram.builder(0, noopBreaker)
            .zeroBucket(ZeroBucket.create(1.0, 5))
            .setPositiveBucket(1, 3)
            .min(0.5)
            .max(4.0)
            .build();
        ExponentialHistogram b = ExponentialHistogram.builder(0, noopBreaker)
            .zeroBucket(ZeroBucket.create(2.0, 2))
            .setPositiveBucket(1, 1)
            .min(0.5)
            .max(4.0)
            .build();

        ExponentialHistogram expected = ExponentialHistogram.builder(0, noopBreaker)
            .zeroBucket(ZeroBucket.create(1.0, 3))
            .setPositiveBucket(1, 2)
            .sum(a.sum() - b.sum())
            .min(0.5)
            .max(4.0)
            .build();
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            boolean isCumulative = merger.setToDifference(a, b);
            assertThat(isCumulative, equalTo(false));
            assertThat(merger.get(), equalTo(expected));
        }
    }

    public void testDifferenceWhenAZeroCountLessThanBZeroCount() {
        var noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        // Raw diff: zero -> max(0, 2-5)=0, pos(1) -> max(0, 5-1)=4. Total raw=4, desired=1. Scaled to 1.
        ExponentialHistogram a = ExponentialHistogram.builder(0, noopBreaker)
            .zeroBucket(ZeroBucket.create(1.0, 2))
            .setPositiveBucket(1, 5)
            .min(0.5)
            .max(4.0)
            .build();
        ExponentialHistogram b = ExponentialHistogram.builder(0, noopBreaker)
            .zeroBucket(ZeroBucket.create(1.0, 5))
            .setPositiveBucket(1, 1)
            .min(0.5)
            .max(4.0)
            .build();

        ExponentialHistogram expected = ExponentialHistogram.builder(0, noopBreaker)
            .zeroBucket(ZeroBucket.create(1.0, 0))
            .setPositiveBucket(1, 1)
            .sum(a.sum() - b.sum())
            .min(2.0)
            .max(4.0)
            .build();
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            boolean isCumulative = merger.setToDifference(a, b);
            assertThat(isCumulative, equalTo(false));
            assertThat(merger.get(), equalTo(expected));
        }
    }

    public void testDifferenceWhenBucketCountInALessThanB() {
        var noopBreaker = ExponentialHistogramCircuitBreaker.noop();
        // Raw diff: pos(0) -> max(0, 2-5)=0, pos(1) -> 5. Total raw=5, desired=2. Scaled to 2.
        ExponentialHistogram a = ExponentialHistogram.builder(0, noopBreaker)
            .setPositiveBucket(0, 2)
            .setPositiveBucket(1, 5)
            .min(1.0)
            .max(4.0)
            .build();
        ExponentialHistogram b = ExponentialHistogram.builder(0, noopBreaker).setPositiveBucket(0, 5).min(1.0).max(2.0).build();

        ExponentialHistogram expected = ExponentialHistogram.builder(0, noopBreaker)
            .setPositiveBucket(1, 2)
            .sum(a.sum() - b.sum())
            .min(2.0)
            .max(4.0)
            .build();
        try (ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(breaker())) {
            boolean isCumulative = merger.setToDifference(a, b);
            assertThat(isCumulative, equalTo(false));
            assertThat(merger.get(), equalTo(expected));
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
