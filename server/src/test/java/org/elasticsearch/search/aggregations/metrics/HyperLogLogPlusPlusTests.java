/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.search.aggregations.metrics.AbstractCardinalityAlgorithm.MAX_PRECISION;
import static org.elasticsearch.search.aggregations.metrics.AbstractCardinalityAlgorithm.MIN_PRECISION;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HyperLogLogPlusPlusTests extends ESTestCase {
    public void testEncodeDecode() {
        final int iters = scaledRandomIntBetween(100000, 500000);
        // random hashes
        for (int i = 0; i < iters; ++i) {
            final int p1 = randomIntBetween(4, 24);
            final long hash = randomLong();
            testEncodeDecode(p1, hash);
        }
        // special cases
        for (int p1 = MIN_PRECISION; p1 <= MAX_PRECISION; ++p1) {
            testEncodeDecode(p1, 0);
            testEncodeDecode(p1, 1);
            testEncodeDecode(p1, ~0L);
        }
    }

    private void testEncodeDecode(int p1, long hash) {
        final long index = AbstractHyperLogLog.index(hash, p1);
        final int runLen = AbstractHyperLogLog.runLen(hash, p1);
        final int encoded = AbstractLinearCounting.encodeHash(hash, p1);
        assertEquals(index, AbstractHyperLogLog.decodeIndex(encoded, p1));
        assertEquals(runLen, AbstractHyperLogLog.decodeRunLen(encoded, p1));
    }

    public void testAccuracy() {
        final long bucket = randomInt(20);
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000 : 100000);
        final int p = randomIntBetween(14, MAX_PRECISION);
        Set<Integer> set = new HashSet<>();
        final CircuitBreaker breaker = new NoopCircuitBreaker("test");
        HyperLogLogPlusPlus e = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, breaker, 1);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            set.add(n);
            final long hash = BitMixer.mix64(n);
            e.collect(bucket, hash);
            if (randomInt(100) == 0) {
                // System.out.println(e.cardinality(bucket) + " <> " + set.size());
                assertThat((double) e.cardinality(bucket), closeTo(set.size(), 0.1 * set.size()));
            }
        }
        assertThat((double) e.cardinality(bucket), closeTo(set.size(), 0.1 * set.size()));
    }

    public void testMerge() {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final CircuitBreaker breaker = new NoopCircuitBreaker("test");
        final HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, breaker, 0);
        final HyperLogLogPlusPlus[] multi = new HyperLogLogPlusPlus[randomIntBetween(2, 100)];
        final long[] bucketOrds = new long[multi.length];
        for (int i = 0; i < multi.length; ++i) {
            bucketOrds[i] = randomInt(20);
            multi[i] = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, breaker, 5);
        }
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000 : 1000000);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            final long hash = BitMixer.mix64(n);
            single.collect(0, hash);
            // use a gaussian so that all instances don't collect as many hashes
            final int index = (int) (Math.pow(randomDouble(), 2));
            multi[index].collect(bucketOrds[index], hash);
            if (randomInt(100) == 0) {
                HyperLogLogPlusPlus merged = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, breaker, 0);
                for (int j = 0; j < multi.length; ++j) {
                    merged.merge(0, multi[j], bucketOrds[j]);
                }
                assertEquals(single.cardinality(0), merged.cardinality(0));
            }
        }
    }

    public void testFakeHashes() {
        // hashes with lots of leading zeros trigger different paths in the code that we try to go through here
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final CircuitBreaker breaker = new NoopCircuitBreaker("test");
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, breaker, 0);

        counts.collect(0, 0);
        assertEquals(1, counts.cardinality(0));
        if (randomBoolean()) {
            counts.collect(0, 1);
            assertEquals(2, counts.cardinality(0));
        }
        counts.upgradeToHll(0);
        // all hashes felt into the same bucket so hll would expect a count of 1
        assertEquals(1, counts.cardinality(0));
    }

    public void testPrecisionFromThreshold() {
        assertEquals(4, HyperLogLogPlusPlus.precisionFromThreshold(0));
        assertEquals(6, HyperLogLogPlusPlus.precisionFromThreshold(10));
        assertEquals(10, HyperLogLogPlusPlus.precisionFromThreshold(100));
        assertEquals(13, HyperLogLogPlusPlus.precisionFromThreshold(1000));
        assertEquals(16, HyperLogLogPlusPlus.precisionFromThreshold(10000));
        assertEquals(18, HyperLogLogPlusPlus.precisionFromThreshold(100000));
        assertEquals(18, HyperLogLogPlusPlus.precisionFromThreshold(1000000));
    }

    public void testCircuitBreakerOnConstruction() {
        int whenToBreak = randomInt(10);
        AtomicLong total = new AtomicLong();
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
            private int countDown = whenToBreak;

            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (countDown-- == 0) {
                    throw new CircuitBreakingException("test error", bytes, Long.MAX_VALUE, Durability.TRANSIENT);
                }
                total.addAndGet(bytes);
            }

            @Override
            public void addWithoutBreaking(long bytes) {
                total.addAndGet(bytes);
            }
        });
        BigArrays bigArrays = new BigArrays(null, breakerService, CircuitBreaker.REQUEST).withCircuitBreaking();
        final int p = randomIntBetween(HyperLogLogPlusPlus.MIN_PRECISION, HyperLogLogPlusPlus.MAX_PRECISION);
        try {
            for (int i = 0; i < whenToBreak + 1; ++i) {
                final HyperLogLogPlusPlus subject = new HyperLogLogPlusPlus(p, bigArrays, 0);
                subject.close();
            }
            fail("Must fail");
        } catch (CircuitBreakingException e) {
            // OK
        }

        assertThat(total.get(), equalTo(0L));
    }

    public void testRetrieveCardinality() {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final CircuitBreaker breaker = new NoopCircuitBreaker("test");
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, breaker, 1);
        int bucket = randomInt(100);
        counts.collect(bucket, randomLong());
        for (int i = 0; i < 1000; i++) {
            int cardinality = bucket == i ? 1 : 0;
            assertEquals(cardinality, counts.cardinality(i));
        }
    }

    public void testAllocation() {
        int precision = between(MIN_PRECISION, MAX_PRECISION);
        long initialBucketCount = between(0, 100);
        MockBigArrays.assertFitsIn(
            ByteSizeValue.ofBytes((initialBucketCount << precision) + initialBucketCount * 4 + PageCacheRecycler.PAGE_SIZE_IN_BYTES * 2),
            bigArrays -> new HyperLogLogPlusPlus(precision, bigArrays, initialBucketCount)
        );
    }

    public void testMaxOrdIsExclusiveUpperBound() {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final CircuitBreaker breaker = new NoopCircuitBreaker("test");
        // Use initialBucketCount=1 so that hll.maxOrd() stays at 1 and doesn't mask lc.maxOrd() bugs.
        // Iterate through enough buckets to guarantee we cross at least one internal array growth boundary,
        // where the off-by-one in LinearCounting.maxOrd() would surface.
        try (HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, breaker, 1)) {
            for (int bucket = 0; bucket < 50; bucket++) {
                counts.collect(bucket, BitMixer.mix64(bucket));
                assertThat(
                    "maxOrd must be an exclusive upper bound after collecting into bucket " + bucket,
                    (long) bucket,
                    lessThan(counts.maxOrd())
                );
            }
        }
    }

    public void testDynamicGrowth() {
        int numGroups = between(1000, 10_000);
        int numValuesPerGroup = between(1, 14);
        long requiredBytesOneGroup = 32L * 4L + 48L + 8L; // 48 bytes overhead each group + 8L bytes for the object reference in the array
        long requiredBytes = requiredBytesOneGroup * numGroups;
        requiredBytes += 2 * PageCacheRecycler.PAGE_SIZE_IN_BYTES; // extra pages for the object array
        requiredBytes += 10 * PageCacheRecycler.PAGE_SIZE_IN_BYTES; // full allocations for the first few groups
        requiredBytes += Math.max(PageCacheRecycler.PAGE_SIZE_IN_BYTES, numGroups * 8L);
        CircuitBreakerService breakerService = LimitedBreaker.service("test", ByteSizeValue.ofBytes(requiredBytes));
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService).withCircuitBreaking();
        int precision = 14;
        try (
            HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(precision, bigArrays, breakerService.getBreaker(CircuitBreaker.REQUEST), 1)
        ) {
            Map<Long, Set<Integer>> uniques = new HashMap<>();
            for (long g = 0; g < numGroups; g++) {
                Set<Integer> sets = new HashSet<>();
                for (int i = 0; i < numValuesPerGroup; i++) {
                    int v = randomInt();
                    long hash = BitMixer.mix64(v);
                    hll.collect(g, hash);
                    sets.add(AbstractLinearCounting.encodeHash(hash, precision));
                }
                uniques.put(g, sets);
            }
            int upgradedGroup = randomBoolean() ? randomIntBetween(0, numGroups - 1) : -1;
            if (upgradedGroup >= 0) {
                hll.upgradeToHll(upgradedGroup);
            }
            for (long g = 0; g < numGroups; g++) {
                Set<Integer> values = uniques.get(g);
                long cardinality = hll.cardinality(g);
                if (g == upgradedGroup) {
                    assertThat(
                        "group=" + g + " expected=" + values.size() + " actual=" + cardinality,
                        (double) cardinality,
                        closeTo(values.size(), Math.max(1, 0.1 * values.size()))
                    );
                } else {
                    assertThat("group=" + g + " values=" + values, values, hasSize((int) cardinality));
                }
            }
        }
    }

    /**
     * Merges states in bulk, either from serialized bytes ({@code combine}), from another structure ({@code merge}) or after
     * deserializing ({@code readFrom}), and checks the result against one structure that collected every hash directly. Registers
     * only grow, so the result must not depend on how the hashes were split up or merged.
     */
    public void testBulkMergePaths() throws IOException {
        // Up to 14, so that the registers need several steps of the bulk operations, which move at most 4096 registers at a time.
        final int precision = randomIntBetween(MIN_PRECISION, 14);
        final int threshold = (int) ((1 << precision) / 4 * 0.75);
        final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        try (
            HyperLogLogPlusPlus reference = new HyperLogLogPlusPlus(precision, bigArrays, 1);
            HyperLogLogPlusPlus dest = new HyperLogLogPlusPlus(precision, bigArrays, 1)
        ) {
            final int destBucket = randomIntBetween(0, 3);
            // The destination may start in linear counting or HyperLogLog, or empty.
            final int initial = randomBoolean() ? 0 : between(1, 4 * threshold);
            for (int i = 0; i < initial; i++) {
                final long hash = BitMixer.mix64(randomLong());
                dest.collect(destBucket, hash);
                reference.collect(0, hash);
            }
            final int sources = between(1, 5);
            for (int s = 0; s < sources; s++) {
                try (HyperLogLogPlusPlus source = new HyperLogLogPlusPlus(precision, bigArrays, 1)) {
                    final int values = between(1, 4 * threshold);
                    for (int i = 0; i < values; i++) {
                        final long hash = BitMixer.mix64(randomLong());
                        source.collect(0, hash);
                        reference.collect(0, hash);
                    }
                    switch (between(0, 2)) {
                        case 0 -> {
                            final BytesStreamOutput out = new BytesStreamOutput();
                            source.writeTo(0, out);
                            // Surround the serialized bytes with padding to check the offset is respected.
                            final BytesRef serialized = out.bytes().toBytesRef();
                            final byte[] padded = new byte[serialized.length + 7 + 5];
                            System.arraycopy(serialized.bytes, serialized.offset, padded, 7, serialized.length);
                            dest.combine(destBucket, new BytesRef(padded, 7, serialized.length));
                        }
                        case 1 -> dest.merge(destBucket, source, 0);
                        case 2 -> {
                            final BytesStreamOutput out = new BytesStreamOutput();
                            source.writeTo(0, out);
                            try (
                                AbstractHyperLogLogPlusPlus read = AbstractHyperLogLogPlusPlus.readFrom(
                                    out.bytes().streamInput(),
                                    bigArrays
                                )
                            ) {
                                dest.merge(destBucket, read, 0);
                            }
                        }
                        default -> throw new AssertionError();
                    }
                }
            }
            assertThat(dest.getAlgorithm(destBucket), equalTo(reference.getAlgorithm(0)));
            if (reference.getAlgorithm(0) == AbstractHyperLogLogPlusPlus.HYPERLOGLOG) {
                // Serialized HyperLogLog is the raw registers, so this compares every register.
                final BytesStreamOutput expected = new BytesStreamOutput();
                final BytesStreamOutput actual = new BytesStreamOutput();
                reference.writeTo(0, expected);
                dest.writeTo(destBucket, actual);
                assertThat(actual.bytes(), equalTo(expected.bytes()));
            } else {
                assertTrue(reference.equals(0, dest, destBucket));
            }
            assertThat(dest.cardinality(destBucket), equalTo(reference.cardinality(0)));
        }
    }

    /**
     * Partitioned aggregations merge serialized states into a fresh structure with {@code combine} and then keep collecting into
     * it, across many buckets. Check that works and matches a structure that collected everything directly.
     */
    public void testCollectAfterCombineAcrossManyBuckets() throws IOException {
        final int precision = randomIntBetween(MIN_PRECISION, 12);
        final int threshold = (int) ((1 << precision) / 4 * 0.75);
        final int buckets = between(1, 300);
        final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        try (
            HyperLogLogPlusPlus source = new HyperLogLogPlusPlus(precision, bigArrays, 1);
            HyperLogLogPlusPlus dest = new HyperLogLogPlusPlus(precision, bigArrays, 1);
            HyperLogLogPlusPlus reference = new HyperLogLogPlusPlus(precision, bigArrays, 1)
        ) {
            for (int b = 0; b < buckets; b++) {
                final int values = randomBoolean() ? between(0, 3) : between(0, 4 * threshold);
                for (int i = 0; i < values; i++) {
                    final long hash = BitMixer.mix64(randomLong());
                    source.collect(b, hash);
                    reference.collect(b, hash);
                }
            }
            for (int b = 0; b < buckets; b++) {
                final BytesStreamOutput out = new BytesStreamOutput();
                source.writeTo(b, out);
                dest.combine(b, out.bytes().toBytesRef());
            }
            for (int i = 0; i < buckets * 20; i++) {
                final int b = between(0, buckets - 1);
                final long hash = BitMixer.mix64(randomLong());
                dest.collect(b, hash);
                reference.collect(b, hash);
            }
            for (int b = 0; b < buckets; b++) {
                assertThat("bucket " + b, dest.cardinality(b), equalTo(reference.cardinality(b)));
            }
        }
    }
}
