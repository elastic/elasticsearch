/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.BitMixer;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.search.aggregations.metrics.AbstractCardinalityAlgorithm.MAX_PRECISION;
import static org.elasticsearch.search.aggregations.metrics.AbstractCardinalityAlgorithm.MIN_PRECISION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HyperLogLogPlusPlusSparseTests extends ESTestCase {

    public void testEquivalence() throws IOException {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
        final int numBuckets = randomIntBetween(2, 100);
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 1000000);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            final long hash = BitMixer.mix64(n);
            single.collect(randomInt(numBuckets), hash);
        }
        for (int i = 0; i < numBuckets; i++) {
            // test clone
            AbstractHyperLogLogPlusPlus clone = single.clone(i, BigArrays.NON_RECYCLING_INSTANCE);
            if (single.getAlgorithm(i) == AbstractHyperLogLogPlusPlus.LINEAR_COUNTING) {
                assertTrue(clone instanceof HyperLogLogPlusPlusSparse);
            } else {
                assertTrue(clone instanceof HyperLogLogPlusPlus);
            }
            checkEquivalence(single, i, clone, 0);
            // test serialize
            BytesStreamOutput out = new BytesStreamOutput();
            single.writeTo(i, out);
            clone = AbstractHyperLogLogPlusPlus.readFrom(out.bytes().streamInput(), BigArrays.NON_RECYCLING_INSTANCE);
            if (single.getAlgorithm(i) == AbstractHyperLogLogPlusPlus.LINEAR_COUNTING) {
                assertTrue(clone instanceof HyperLogLogPlusPlusSparse);
            } else {
                assertTrue(clone instanceof HyperLogLogPlusPlus);
            }
            checkEquivalence(single, i, clone, 0);
            // test merge
            final HyperLogLogPlusPlus merge = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
            merge.merge(0, clone, 0);
            checkEquivalence(merge, 0, clone, 0);
        }
    }

    private void checkEquivalence(AbstractHyperLogLogPlusPlus first, int firstBucket,
                                  AbstractHyperLogLogPlusPlus second, int secondBucket) {
        assertEquals(first.hashCode(firstBucket), second.hashCode(secondBucket));
        assertEquals(first.cardinality(firstBucket), second.cardinality(0));
        assertTrue(first.equals(firstBucket, second, secondBucket));
        assertTrue(second.equals(secondBucket, first, firstBucket));
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
        final int p = randomIntBetween(AbstractCardinalityAlgorithm.MIN_PRECISION, AbstractCardinalityAlgorithm.MAX_PRECISION);
        try {
            for (int i = 0; i < whenToBreak + 1; ++i) {
                final HyperLogLogPlusPlusSparse subject = new HyperLogLogPlusPlusSparse(p, bigArrays, 1);
                subject.close();
            }
            fail("Must fail");
        } catch (CircuitBreakingException e) {
            // OK
        }

        assertThat(total.get(), CoreMatchers.equalTo(0L));
    }

    public void testAllocation() {
        int precision = between(MIN_PRECISION, MAX_PRECISION);
        long initialBucketCount = between(0, 100);
        MockBigArrays.assertFitsIn(
            ByteSizeValue.ofBytes(Math.max(256, initialBucketCount * 32)),
            bigArrays -> new HyperLogLogPlusPlusSparse(precision, bigArrays, initialBucketCount)
        );
    }

}
