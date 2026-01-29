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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.search.aggregations.metrics.AbstractCardinalityAlgorithm.MAX_PRECISION;
import static org.elasticsearch.search.aggregations.metrics.AbstractCardinalityAlgorithm.MIN_PRECISION;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
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
        HyperLogLogPlusPlus e = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
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
        final HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
        final HyperLogLogPlusPlus[] multi = new HyperLogLogPlusPlus[randomIntBetween(2, 100)];
        final long[] bucketOrds = new long[multi.length];
        for (int i = 0; i < multi.length; ++i) {
            bucketOrds[i] = randomInt(20);
            multi[i] = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 5);
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
                HyperLogLogPlusPlus merged = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
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
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);

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
        final HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
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

    public void testMergeFromStream() throws IOException {
        // Test that mergeFrom produces the same results as merge(deserialize())
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final int numSources = randomIntBetween(2, 20);
        final int numValues = randomIntBetween(100, 10000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000 : 100000);

        // Create source HLLs and collect values
        HyperLogLogPlusPlus[] sources = new HyperLogLogPlusPlus[numSources];
        for (int i = 0; i < numSources; i++) {
            sources[i] = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        }

        for (int i = 0; i < numValues; i++) {
            final int n = randomInt(maxValue);
            final long hash = BitMixer.mix64(n);
            sources[randomInt(numSources - 1)].collect(0, hash);
        }

        // Merge using traditional approach: deserialize then merge
        HyperLogLogPlusPlus mergedTraditional = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (HyperLogLogPlusPlus source : sources) {
            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            source.writeTo(0, out);

            // Deserialize and merge
            ByteArrayStreamInput in = new ByteArrayStreamInput(out.bytes().toBytesRef().bytes);
            AbstractHyperLogLogPlusPlus deserialized = AbstractHyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
            mergedTraditional.merge(0, deserialized, 0);
        }

        // Merge using streaming approach: mergeFrom
        HyperLogLogPlusPlus mergedStreaming = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (HyperLogLogPlusPlus source : sources) {
            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            source.writeTo(0, out);

            // Merge directly from stream
            ByteArrayStreamInput in = new ByteArrayStreamInput(out.bytes().toBytesRef().bytes);
            mergedStreaming.mergeFrom(0, in);
        }

        // Both should produce the same cardinality
        assertEquals(mergedTraditional.cardinality(0), mergedStreaming.cardinality(0));
    }

    public void testMergeFromStreamWithDifferentAlgorithms() throws IOException {
        // Test merging when source and target are in different modes (LINEAR_COUNTING vs HYPERLOGLOG)

        // Choose counts that will stay in LINEAR_COUNTING vs trigger upgrade to HYPERLOGLOG
        final int valuesForLinearCounting = 100;
        final int valuesForHyperLogLog = 100_000;
        // Use precisionFromThreshold to get a precision where valuesForLinearCounting stays in LINEAR_COUNTING
        // but valuesForHyperLogLog exceeds the threshold and triggers upgrade to HYPERLOGLOG
        final int p = HyperLogLogPlusPlus.precisionFromThreshold(valuesForLinearCounting);
        // HLL has ~2% standard error, so 10% tolerance is safe for equality checks
        final double cardinalityErrorTolerance = 0.1;

        // Create a source with few values (stays in LINEAR_COUNTING)
        HyperLogLogPlusPlus sourceLinear = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (int i = 0; i < valuesForLinearCounting; i++) {
            sourceLinear.collect(0, BitMixer.mix64(i));
        }

        // Create a source with many values (upgrades to HYPERLOGLOG)
        HyperLogLogPlusPlus sourceHll = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (int i = 0; i < valuesForHyperLogLog; i++) {
            sourceHll.collect(0, BitMixer.mix64(i));
        }

        // Serialize both sources
        BytesStreamOutput outLinear = new BytesStreamOutput();
        sourceLinear.writeTo(0, outLinear);
        byte[] linearBytes = outLinear.bytes().toBytesRef().bytes;

        BytesStreamOutput outHll = new BytesStreamOutput();
        sourceHll.writeTo(0, outHll);
        byte[] hllBytes = outHll.bytes().toBytesRef().bytes;

        // Scenario 1: Merge LINEAR_COUNTING source into empty target
        HyperLogLogPlusPlus targetForLinearMerge = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        targetForLinearMerge.mergeFrom(0, new ByteArrayStreamInput(linearBytes));
        assertEquals(sourceLinear.cardinality(0), targetForLinearMerge.cardinality(0));

        // Scenario 2: Merge HYPERLOGLOG source into empty target
        HyperLogLogPlusPlus targetForHllMerge = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        targetForHllMerge.mergeFrom(0, new ByteArrayStreamInput(hllBytes));
        assertEquals(sourceHll.cardinality(0), targetForHllMerge.cardinality(0));

        // Scenario 3: Merge LINEAR_COUNTING into existing HYPERLOGLOG
        // (linear values are a subset of HLL values, so cardinality shouldn't change much)
        HyperLogLogPlusPlus targetHllThenLinear = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        targetHllThenLinear.mergeFrom(0, new ByteArrayStreamInput(hllBytes));
        targetHllThenLinear.mergeFrom(0, new ByteArrayStreamInput(linearBytes));
        long expectedCardinality = sourceHll.cardinality(0);
        assertThat(
            (double) targetHllThenLinear.cardinality(0),
            closeTo(expectedCardinality, cardinalityErrorTolerance * expectedCardinality)
        );

        // Scenario 4: Merge HYPERLOGLOG into existing LINEAR_COUNTING (should trigger upgrade to HLL)
        HyperLogLogPlusPlus targetLinearThenHll = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 1);
        targetLinearThenHll.mergeFrom(0, new ByteArrayStreamInput(linearBytes));
        targetLinearThenHll.mergeFrom(0, new ByteArrayStreamInput(hllBytes));
        assertThat(
            (double) targetLinearThenHll.cardinality(0),
            closeTo(expectedCardinality, cardinalityErrorTolerance * expectedCardinality)
        );
    }
}
