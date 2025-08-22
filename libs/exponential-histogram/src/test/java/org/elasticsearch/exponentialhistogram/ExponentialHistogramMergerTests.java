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

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.adjustScale;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramMergerTests extends ESTestCase {

    public void testZeroThresholdCollapsesOverlappingBuckets() {
        FixedCapacityExponentialHistogram first = new FixedCapacityExponentialHistogram(100);
        first.setZeroBucket(new ZeroBucket(2.0001, 10));

        FixedCapacityExponentialHistogram second = new FixedCapacityExponentialHistogram(100);
        first.resetBuckets(0); // scale 0 means base 2
        first.tryAddBucket(0, 1, false); // bucket (-2, 1]
        first.tryAddBucket(1, 1, false); // bucket (-4, 2]
        first.tryAddBucket(2, 7, false); // bucket (-8, 4]
        first.tryAddBucket(0, 1, true); // bucket (1, 2]
        first.tryAddBucket(1, 1, true); // bucket (2, 4]
        first.tryAddBucket(2, 42, true); // bucket (4, 8]

        ExponentialHistogram mergeResult = mergeWithMinimumScale(100, 0, first, second);

        assertThat(mergeResult.zeroBucket().zeroThreshold(), equalTo(4.0));
        assertThat(mergeResult.zeroBucket().count(), equalTo(14L));

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
        FixedCapacityExponentialHistogram third = new FixedCapacityExponentialHistogram(100);
        third.setZeroBucket(new ZeroBucket(45.0, 1));

        mergeResult = mergeWithMinimumScale(100, 0, mergeResult, third);
        assertThat(mergeResult.zeroBucket().zeroThreshold(), closeTo(45.0, 0.000001));
        assertThat(mergeResult.zeroBucket().count(), equalTo(1L + 14L + 42L + 7L));
        assertThat(mergeResult.positiveBuckets().iterator().hasNext(), equalTo(false));
        assertThat(mergeResult.negativeBuckets().iterator().hasNext(), equalTo(false));
    }

    public void testEmptyZeroBucketIgnored() {
        FixedCapacityExponentialHistogram first = new FixedCapacityExponentialHistogram(100);
        first.setZeroBucket(new ZeroBucket(2.0, 10));
        first.resetBuckets(0); // scale 0 means base 2
        first.tryAddBucket(2, 42L, true); // bucket (4, 8]

        FixedCapacityExponentialHistogram second = new FixedCapacityExponentialHistogram(100);
        second.setZeroBucket(new ZeroBucket(100.0, 0));

        ExponentialHistogram mergeResult = mergeWithMinimumScale(100, 0, first, second);

        assertThat(mergeResult.zeroBucket().zeroThreshold(), equalTo(2.0));
        assertThat(mergeResult.zeroBucket().count(), equalTo(10L));

        BucketIterator posBuckets = mergeResult.positiveBuckets().iterator();
        assertThat(posBuckets.peekIndex(), equalTo(2L));
        assertThat(posBuckets.peekCount(), equalTo(42L));
        posBuckets.advance();
        assertThat(posBuckets.hasNext(), equalTo(false));
    }

    public void testUpscalingDoesNotExceedIndexLimits() {
        for (int i = 0; i < 4; i++) {

            boolean isPositive = i % 2 == 0;
            boolean useMinIndex = i > 1;

            FixedCapacityExponentialHistogram histo = new FixedCapacityExponentialHistogram(2);
            histo.resetBuckets(20);

            long index = useMinIndex ? MIN_INDEX / 2 : MAX_INDEX / 2;

            histo.tryAddBucket(index, 1, isPositive);

            ExponentialHistogramMerger merger = new ExponentialHistogramMerger(100);
            merger.add(histo);
            ExponentialHistogram result = merger.get();

            assertThat(result.scale(), equalTo(21));
            if (isPositive) {
                assertThat(result.positiveBuckets().iterator().peekIndex(), equalTo(adjustScale(index, 20, 1)));
            } else {
                assertThat(result.negativeBuckets().iterator().peekIndex(), equalTo(adjustScale(index, 20, 1)));
            }
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

        ExponentialHistogram reference = ExponentialHistogram.create(20, values.stream().mapToDouble(Double::doubleValue).toArray());

        for (int i = 0; i < 100; i++) {
            Collections.shuffle(values, random());
            ExponentialHistogram shuffled = ExponentialHistogram.create(20, values.stream().mapToDouble(Double::doubleValue).toArray());

            assertThat("Expected same scale", shuffled.scale(), equalTo(reference.scale()));
            assertThat("Expected same zero-bucket", shuffled.zeroBucket(), equalTo(reference.zeroBucket()));
            assertBucketsEqual(shuffled.negativeBuckets(), reference.negativeBuckets());
            assertBucketsEqual(shuffled.positiveBuckets(), reference.positiveBuckets());
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

    private static ExponentialHistogram mergeWithMinimumScale(int bucketCount, int scale, ExponentialHistogram... histograms) {
        ExponentialHistogramMerger merger = ExponentialHistogramMerger.createForTesting(bucketCount, scale);
        Arrays.stream(histograms).forEach(merger::add);
        return merger.get();
    }

}
