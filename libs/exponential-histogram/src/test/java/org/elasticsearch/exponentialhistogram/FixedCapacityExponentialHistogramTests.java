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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class FixedCapacityExponentialHistogramTests extends ExponentialHistogramTestCase {

    public void testConcurrentHashCode() throws ExecutionException, InterruptedException {
        List<ExponentialHistogram> originalHistograms = IntStream.range(0, 1000)
            .mapToObj(i -> ExponentialHistogramTestUtils.randomHistogram())
            .toList();

        List<? extends ExponentialHistogram> copies = originalHistograms.stream()
            .map(histo -> ExponentialHistogram.builder(histo, ExponentialHistogramCircuitBreaker.noop()).build())
            .toList();

        // Compute potentially lazy data correctly on the originals
        originalHistograms.forEach(Object::hashCode);
        concurrentTest(() -> {
            for (int i = 0; i < originalHistograms.size(); i++) {
                ExponentialHistogram original = originalHistograms.get(i);
                ExponentialHistogram copy = copies.get(i);
                assertThat(copy.hashCode(), equalTo(original.hashCode()));
            }
        });
    }

    public void testValueCountUpdatedCorrectly() {

        FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(100, breaker());
        autoReleaseOnTestEnd(histogram);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(0L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(0L));

        histogram.tryAddBucket(1, 10, false);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(10L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(0L));

        histogram.tryAddBucket(2, 3, false);
        histogram.tryAddBucket(3, 4, false);
        histogram.tryAddBucket(1, 5, true);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(17L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(5L));

        histogram.tryAddBucket(2, 3, true);
        histogram.tryAddBucket(3, 4, true);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(17L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(12L));

        histogram.resetBuckets(0);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(0L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(0L));
    }

    public void testMemoryAccounting() {
        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        try (FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(100, breaker(esBreaker))) {
            assertThat(histogram.ramBytesUsed(), greaterThan(2 * RamEstimationUtil.estimateLongArray(100)));
            assertThat(esBreaker.getUsed(), equalTo(histogram.ramBytesUsed()));
        }
        assertThat(esBreaker.getUsed(), equalTo(0L));
    }

    public void testReverseIterator() {
        FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(10, breaker());
        autoReleaseOnTestEnd(histogram);

        assertTrue(histogram.tryAddBucket(1, 2, false));
        assertTrue(histogram.tryAddBucket(3, 4, false));

        assertTrue(histogram.tryAddBucket(2, 10, true));
        assertTrue(histogram.tryAddBucket(7, 20, true));
        assertTrue(histogram.tryAddBucket(12, 30, true));

        BucketIterator it = histogram.positiveBuckets().reverseIterator();
        assertThat(it.peekIndex(), equalTo(12L));
        assertThat(it.peekCount(), equalTo(30L));
        it.advance();
        assertThat(it.peekIndex(), equalTo(7L));
        assertThat(it.peekCount(), equalTo(20L));
        it.advance();
        assertThat(it.peekIndex(), equalTo(2L));
        assertThat(it.peekCount(), equalTo(10L));
        it.advance();
        assertFalse(it.hasNext());
    }

    public void testScaleBucketCountsTo() {
        for (int iter = 0; iter < 100; iter++) {
            boolean useNegative = randomBoolean();
            boolean usePositive = randomBoolean();
            boolean useZero = randomBoolean();

            int numNegBuckets = useNegative ? randomIntBetween(1, 500) : 0;
            int numPosBuckets = usePositive ? randomIntBetween(1, 500) : 0;

            // Intentionally use very large and very small bucket counts
            LongSupplier randomCount = () -> randomBoolean() ? randomLongBetween(1, 10) : randomLongBetween(1, 1L << 34);

            FixedCapacityExponentialHistogram original = FixedCapacityExponentialHistogram.create(1000, breaker());
            FixedCapacityExponentialHistogram scaled = FixedCapacityExponentialHistogram.create(1000, breaker());
            autoReleaseOnTestEnd(original);
            autoReleaseOnTestEnd(scaled);

            if (useZero) {
                ZeroBucket zb = ExponentialHistogramTestUtils.randomHistogram().zeroBucket().withCount(randomCount.getAsLong());
                original.setZeroBucket(zb);
                scaled.setZeroBucket(zb);
            }

            for (int i = 0; i < numNegBuckets; i++) {
                long cnt = randomCount.getAsLong();
                original.tryAddBucket(i - 10, cnt, false);
                scaled.tryAddBucket(i - 10, cnt, false);
            }

            for (int i = 0; i < numPosBuckets; i++) {
                long cnt = randomCount.getAsLong();
                original.tryAddBucket(i - 10, cnt, true);
                scaled.tryAddBucket(i - 10, cnt, true);
            }

            long targetCount = Math.round(original.valueCount() * (randomBoolean() ? randomDouble() : randomDouble() * 1_000));
            double factor = 1.0 * targetCount / original.valueCount();
            scaled.scaleBucketCountsTo(targetCount);

            assertThat(scaled.valueCount(), equalTo(targetCount));

            // Compare each original bucket against its scaled counterpart.
            // When scaling up (factor >= 1), every bucket count must not decrease.
            // When scaling down (factor < 1), every bucket count must not increase (but may be pruned to 0).
            // In both cases the scaled count must be within a relative error of the expected value.
            assertBucketBounds(original.negativeBuckets().iterator(), scaled.negativeBuckets().iterator(), factor);
            assertScaledZeroBucket(original.zeroBucket().count(), scaled.zeroBucket().count(), factor);
            assertBucketBounds(original.positiveBuckets().iterator(), scaled.positiveBuckets().iterator(), factor);
        }
    }

    private void assertScaledZeroBucket(long origCount, long scaledCount, double factor) {
        double expected = origCount * factor;
        long minValue = (long) Math.floor(expected * 0.99999);
        long maxValue = (long) Math.ceil(expected * 1.00001);
        if (factor > 1.0) {
            minValue = Math.max(minValue, origCount);
        }
        if (factor < 1.0) {
            maxValue = Math.min(maxValue, origCount);
        }
        assertThat(scaledCount, greaterThanOrEqualTo(minValue));
        assertThat(scaledCount, lessThanOrEqualTo(maxValue));
    }

    private void assertBucketBounds(BucketIterator origIt, BucketIterator scaledIt, double factor) {
        while (origIt.hasNext()) {
            long origCount = origIt.peekCount();
            double expected = origCount * factor;
            long minValue = (long) Math.floor(expected * 0.99999);
            long maxValue = (long) Math.ceil(expected * 1.00001);

            if (factor > 1.0) {
                // count should never decrease
                minValue = Math.max(minValue, origCount);
            }
            if (factor < 1.0) {
                // count should never increase
                maxValue = Math.min(maxValue, origCount);
            }

            if (minValue == 0 && (scaledIt.hasNext() == false || scaledIt.peekIndex() != origIt.peekIndex())) {
                origIt.advance();
                continue; // bucket was pruned because it rounded to 0
            }
            assertThat(scaledIt.peekCount(), greaterThan(0L));
            assertThat("expected bucket at index " + origIt.peekIndex() + " to be present", scaledIt.hasNext(), equalTo(true));
            assertThat(scaledIt.peekIndex(), equalTo(origIt.peekIndex()));
            long scaledCount = scaledIt.peekCount();
            assertThat(scaledCount, greaterThanOrEqualTo(minValue));
            assertThat(scaledCount, lessThanOrEqualTo(maxValue));
            origIt.advance();
            scaledIt.advance();
        }
        assertThat("unexpected extra buckets after scaling", scaledIt.hasNext(), equalTo(false));
    }

    protected void concurrentTest(Runnable r) throws InterruptedException, ExecutionException {
        int threads = 5;
        int tasks = threads * 2;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int t = 0; t < tasks; t++) {
                results.add(exec.submit(r));
            }
            for (Future<?> f : results) {
                f.get();
            }
        } finally {
            exec.shutdown();
        }
    }
}
