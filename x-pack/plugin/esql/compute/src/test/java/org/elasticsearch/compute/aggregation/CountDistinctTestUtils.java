/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

final class CountDistinctTestUtils {
    static final int PRECISION = 40000;

    /**
     * Linear counting keeps this many leading bits of each hash; see {@code AbstractLinearCounting#encodeHash}.
     */
    private static final int ENCODED_HASH_BITS = 25;

    private CountDistinctTestUtils() {}

    /**
     * Asserts a {@code COUNT_DISTINCT} result against the true number of distinct values. HLL++ is approximate,
     * so the result may be off by up to 10%. That gives small groups no slack, yet even below the precision
     * threshold linear counting counts two distinct values once if their hashes share the leading bits it keeps.
     * So a result outside the tolerance must be exactly the number of distinct encoded hashes, computed here
     * independently of HLL++. Anything else is a real bug.
     *
     * @param hashes the hashes of the distinct values, computed the way {@link HllStates} hashes each type
     */
    static void assertCount(long count, LongStream hashes) {
        long[] distinctHashes = hashes.toArray();
        long distinct = distinctHashes.length;
        if (Math.abs(count - distinct) <= distinct * 0.1) {
            return;
        }
        int p = HyperLogLogPlusPlus.precisionFromThreshold(PRECISION);
        long distinctEncoded = Arrays.stream(distinctHashes).map(h -> encodeHash(h, p)).distinct().count();
        assertThat(
            "count_distinct of " + distinct + " values, " + (distinct - distinctEncoded) + " of them hash collisions",
            count,
            equalTo(distinctEncoded)
        );
    }

    static long hash(long v) {
        return BitMixer.mix64(v);
    }

    static long hash(double v) {
        return BitMixer.mix64(Double.doubleToLongBits(v));
    }

    static long hash(BytesRef v) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(v.bytes, v.offset, v.length, 0, new MurmurHash3.Hash128());
        return hash.h1;
    }

    private static long encodeHash(long hash, int p) {
        long e = hash >>> (Long.SIZE - ENCODED_HASH_BITS);
        if ((e & ((1L << (ENCODED_HASH_BITS - p)) - 1)) == 0) {
            int runLen = 1 + Math.min(Long.numberOfLeadingZeros(hash << ENCODED_HASH_BITS), Long.SIZE - ENCODED_HASH_BITS);
            return (e << 7) | ((long) runLen << 1) | 1;
        }
        return e << 1;
    }
}
