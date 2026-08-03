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
import static org.hamcrest.Matchers.lessThan;
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
            // The capacity is a ceiling, not an allocation: a fresh histogram has not paid for a single one of its 100 buckets, so it
            // costs less than the bucket index array alone would at full capacity.
            long fresh = histogram.ramBytesUsed();
            assertThat(
                "a fresh histogram must not have allocated for its capacity",
                fresh,
                lessThan(RamEstimationUtil.estimateLongArray(100))
            );
            assertThat(esBreaker.getUsed(), equalTo(fresh));

            // Filling it to capacity grows it to what it used to cost eagerly: the indices in full, the counts at the narrowest width.
            for (int i = 0; i < 100; i++) {
                assertTrue(histogram.tryAddBucket(i, 1, true));
                assertThat("the breaker must track every growth", esBreaker.getUsed(), equalTo(histogram.ramBytesUsed()));
            }
            long full = histogram.ramBytesUsed();
            assertThat("a histogram filled to capacity holds more than a fresh one", full, greaterThan(fresh));
            assertThat(full, greaterThan(RamEstimationUtil.estimateLongArray(100) + RamEstimationUtil.estimateByteArray(100)));
            assertThat(full, lessThan(2 * RamEstimationUtil.estimateLongArray(100)));
            assertThat(esBreaker.getUsed(), equalTo(full));
        }
        assertThat(esBreaker.getUsed(), equalTo(0L));
    }

    /**
     * The bucket arrays grow on demand, so what a histogram costs must follow how many buckets it was actually given rather than the
     * capacity it was created with — which is the whole point of growing them.
     */
    public void testHistogramWithFewBucketsHoldsLessThanOneWithMany() {
        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        try (
            FixedCapacityExponentialHistogram few = FixedCapacityExponentialHistogram.create(1000, breaker(esBreaker));
            FixedCapacityExponentialHistogram many = FixedCapacityExponentialHistogram.create(1000, breaker(esBreaker))
        ) {
            for (int i = 0; i < 5; i++) {
                assertTrue(few.tryAddBucket(i, 1, true));
            }
            for (int i = 0; i < 900; i++) {
                assertTrue(many.tryAddBucket(i, 1, true));
            }

            assertThat(few.ramBytesUsed(), lessThan(many.ramBytesUsed()));
            assertThat("a sparsely populated histogram must not pay for its capacity", few.ramBytesUsed(), lessThan(1000L));
            assertThat(esBreaker.getUsed(), equalTo(few.ramBytesUsed() + many.ramBytesUsed()));
        }
        assertThat("closing must give back exactly what was taken", esBreaker.getUsed(), equalTo(0L));
    }

    /**
     * Growth must be invisible to consumers: a histogram which reached its buckets one at a time, growing repeatedly on the way, has to
     * answer identically to one which was given the same buckets at its full size from the start.
     */
    public void testGrownHistogramIsIndistinguishableFromAPreSizedOne() {
        int buckets = randomIntBetween(2, 200);
        long[] indices = new long[buckets];
        long[] counts = new long[buckets];
        for (int i = 0; i < buckets; i++) {
            indices[i] = i == 0 ? randomLongBetween(-1000, 0) : indices[i - 1] + randomLongBetween(1, 20);
            counts[i] = randomLongBetween(1, 100);
        }

        // the second one is created at exactly the number of buckets it will receive, so it never grows
        FixedCapacityExponentialHistogram grown = FixedCapacityExponentialHistogram.create(1000, breaker());
        autoReleaseOnTestEnd(grown);
        FixedCapacityExponentialHistogram preSized = FixedCapacityExponentialHistogram.create(buckets, breaker());
        autoReleaseOnTestEnd(preSized);

        for (FixedCapacityExponentialHistogram histogram : List.of(grown, preSized)) {
            for (int i = 0; i < buckets; i++) {
                assertTrue(histogram.tryAddBucket(indices[i], counts[i], true));
            }
            histogram.setSum(123.5);
            histogram.setMin(0.5);
            histogram.setMax(9000.0);
        }

        assertThat(grown.valueCount(), equalTo(preSized.valueCount()));
        for (double quantile : new double[] { 0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0 }) {
            assertThat(
                "quantile " + quantile + " must not depend on how the buckets were grown",
                ExponentialHistogramQuantile.getQuantile(grown, quantile),
                equalTo(ExponentialHistogramQuantile.getQuantile(preSized, quantile))
            );
        }
        assertTrue(ExponentialHistogram.equals(grown, preSized));
    }

    /**
     * A growth the circuit breaker refuses must leave the histogram exactly as it was — still holding every bucket it had, still usable
     * up to the size it had already reached, and accounted for to the byte.
     */
    public void testHistogramSurvivesARefusedGrowth() {
        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        RefusingCircuitBreaker refusing = new RefusingCircuitBreaker(breaker(esBreaker));
        FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(1000, refusing);
        try {
            // fill until a growth is refused, which must happen well before the capacity is reached
            int added = 0;
            refusing.refuseFrom(esBreaker.getUsed() + 512);
            while (added < 1000) {
                try {
                    assertTrue(histogram.tryAddBucket(added, added + 1, true));
                } catch (RefusedException expected) {
                    break;
                }
                added++;
            }
            assertThat("the breaker should have refused a growth before the capacity was reached", added, lessThan(1000));
            assertThat("some buckets must have been added before the refusal", added, greaterThan(0));

            long afterRefusal = histogram.ramBytesUsed();
            assertThat("a refused growth must not leave the breaker charged for it", esBreaker.getUsed(), equalTo(afterRefusal));

            // the histogram is still perfectly usable, it just cannot grow any further
            assertThat(histogram.positiveBuckets().bucketCount(), equalTo(added));
            long expectedCount = 0;
            for (int i = 0; i < added; i++) {
                expectedCount += i + 1;
            }
            assertThat(histogram.positiveBuckets().valueCount(), equalTo(expectedCount));
            BucketIterator it = histogram.positiveBuckets().iterator();
            for (int i = 0; i < added; i++) {
                assertThat(it.peekIndex(), equalTo((long) i));
                assertThat(it.peekCount(), equalTo((long) i + 1));
                it.advance();
            }
            assertFalse(it.hasNext());

            // and once the breaker relents it can grow again
            refusing.allowEverything();
            assertTrue(histogram.tryAddBucket(added, 1, true));
            assertThat(esBreaker.getUsed(), equalTo(histogram.ramBytesUsed()));
        } finally {
            histogram.close();
        }
        assertThat("closing must give back exactly what was taken", esBreaker.getUsed(), equalTo(0L));
    }

    private static class RefusedException extends RuntimeException {}

    /**
     * Refuses every allocation that would take the delegate above a given number of bytes, so that a growth can be made to fail at a
     * chosen point. A real {@link CircuitBreaker} would do this too, but only at a limit large enough to make the test slow and its
     * failure point dependent on the exact object layout.
     */
    private static class RefusingCircuitBreaker implements ExponentialHistogramCircuitBreaker {

        private final ExponentialHistogramCircuitBreaker delegate;
        private long used;
        private long limit = Long.MAX_VALUE;

        RefusingCircuitBreaker(ExponentialHistogramCircuitBreaker delegate) {
            this.delegate = delegate;
        }

        void refuseFrom(long limit) {
            this.limit = limit;
        }

        void allowEverything() {
            this.limit = Long.MAX_VALUE;
        }

        @Override
        public void adjustBreaker(long bytesAllocated) {
            if (bytesAllocated > 0 && used + bytesAllocated > limit) {
                throw new RefusedException();
            }
            used += bytesAllocated;
            delegate.adjustBreaker(bytesAllocated);
        }
    }

    /**
     * Counts are stored in the narrowest integer width that fits them, so a histogram must charge the circuit breaker for the extra
     * memory the moment a count forces a wider one, and hand it all back when it is closed.
     */
    public void testMemoryAccountingFollowsCountWidthPromotions() {
        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        try (FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(100, breaker(esBreaker))) {
            long narrow = histogram.ramBytesUsed();
            assertThat(esBreaker.getUsed(), equalTo(narrow));

            long previous = narrow;
            // each of these counts overflows the width the previous one fit into
            for (long count : new long[] { 1L, (long) Byte.MAX_VALUE + 1, (long) Short.MAX_VALUE + 1, (long) Integer.MAX_VALUE + 1 }) {
                histogram.resetBuckets(0);
                assertTrue(histogram.tryAddBucket(1, count, true));
                assertThat(histogram.positiveBuckets().valueCount(), equalTo(count));
                assertThat(histogram.ramBytesUsed(), greaterThanOrEqualTo(previous));
                assertThat("the breaker must track every promotion", esBreaker.getUsed(), equalTo(histogram.ramBytesUsed()));
                previous = histogram.ramBytesUsed();
            }
            assertThat("a promoted histogram costs more than a fresh one", previous, greaterThan(narrow));

            // a full reset means the contents are gone, so the promotion must be given back
            histogram.reset();
            assertThat(histogram.ramBytesUsed(), equalTo(narrow));
            assertThat(esBreaker.getUsed(), equalTo(narrow));
        }
        assertThat("closing must give back exactly what was taken", esBreaker.getUsed(), equalTo(0L));
    }

    /**
     * Whatever width the counts end up stored in, the values that come back out must be the ones that went in, including at each of the
     * exact boundaries where a promotion happens.
     */
    public void testCountsSurvivePromotionAtEveryWidthBoundary() {
        long[] counts = new long[] {
            1L,
            Byte.MAX_VALUE,
            (long) Byte.MAX_VALUE + 1,
            Short.MAX_VALUE,
            (long) Short.MAX_VALUE + 1,
            Integer.MAX_VALUE,
            (long) Integer.MAX_VALUE + 1,
            Long.MAX_VALUE / 16 };

        FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(counts.length, breaker());
        autoReleaseOnTestEnd(histogram);

        // add them in an order that forces a promotion in the middle rather than up front
        for (int i = 0; i < counts.length; i++) {
            assertTrue(histogram.tryAddBucket(i, counts[i], true));
        }

        BucketIterator it = histogram.positiveBuckets().iterator();
        for (int i = 0; i < counts.length; i++) {
            assertThat(it.peekIndex(), equalTo((long) i));
            assertThat("count at slot " + i + " must read back unchanged", it.peekCount(), equalTo(counts[i]));
            it.advance();
        }
        assertFalse(it.hasNext());
    }

    /**
     * The width the counts happen to be stored in must be invisible to every consumer, so a histogram that has been forced to a wide
     * one has to answer identically to one holding the same buckets at the narrowest width.
     */
    public void testPromotedHistogramIsIndistinguishableFromANarrowOne() {
        int buckets = randomIntBetween(2, 50);
        long[] indices = new long[buckets];
        long[] counts = new long[buckets];
        for (int i = 0; i < buckets; i++) {
            indices[i] = i == 0 ? randomLongBetween(-1000, 0) : indices[i - 1] + randomLongBetween(1, 20);
            counts[i] = randomLongBetween(1, 100);
        }

        FixedCapacityExponentialHistogram narrow = FixedCapacityExponentialHistogram.create(buckets, breaker());
        autoReleaseOnTestEnd(narrow);
        FixedCapacityExponentialHistogram promoted = FixedCapacityExponentialHistogram.create(buckets, breaker());
        autoReleaseOnTestEnd(promoted);

        // force the second one to the widest storage, then drop those buckets again; resetBuckets deliberately keeps the width
        assertTrue(promoted.tryAddBucket(0, Long.MAX_VALUE / 2, true));
        promoted.resetBuckets(promoted.scale());

        for (FixedCapacityExponentialHistogram histogram : List.of(narrow, promoted)) {
            for (int i = 0; i < buckets; i++) {
                assertTrue(histogram.tryAddBucket(indices[i], counts[i], true));
            }
            histogram.setSum(123.5);
            histogram.setMin(0.5);
            histogram.setMax(9000.0);
        }

        assertThat(promoted.ramBytesUsed(), greaterThan(narrow.ramBytesUsed()));
        assertThat(promoted.valueCount(), equalTo(narrow.valueCount()));
        assertThat(promoted.sum(), equalTo(narrow.sum()));
        assertThat(promoted.min(), equalTo(narrow.min()));
        assertThat(promoted.max(), equalTo(narrow.max()));
        for (double quantile : new double[] { 0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0 }) {
            assertThat(
                "quantile " + quantile + " must not depend on how the counts are stored",
                ExponentialHistogramQuantile.getQuantile(promoted, quantile),
                equalTo(ExponentialHistogramQuantile.getQuantile(narrow, quantile))
            );
        }
        assertTrue(ExponentialHistogram.equals(narrow, promoted));
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
