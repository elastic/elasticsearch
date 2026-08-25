/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Verifies {@link BitMixer#mix} spreads several key patterns that are
 * structurally easy for a weak integer hash to get wrong: not just uniform
 * random keys, but ones that vary only above or only below bit 32, or that
 * are sequential at a large magnitude (e.g. millisecond epoch timestamps).
 * {@link LongSwissHash}'s probe/collision rate depends entirely on this
 * spread; a regression here doesn't fail any add/find correctness
 * assertion (the table falls back to linear probing regardless), it just
 * quietly degrades performance, so distribution is exercised directly.
 */
public class BitMixerTests extends ESTestCase {

    private static final int KEY_COUNT = 200_000;
    private static final int BUCKET_BITS = 14; // 16,384 buckets, ~12.2 average load
    private static final int BUCKETS = 1 << BUCKET_BITS;
    private static final int BUCKET_MASK = BUCKETS - 1;

    // Empirically a healthy mix keeps the busiest bucket within a small
    // multiple of the average load; a badly degenerate hash (e.g. one that
    // ignores half the key's bits) blows this out by an order of magnitude
    // or more, so this bound is loose enough not to flake on a good hash
    // but tight enough to catch a real regression.
    private static final int MAX_LOAD_FACTOR = 8;

    // Expect close to KEY_COUNT distinct 32-bit outputs; birthday-bound
    // collisions at this N are negligible for a healthy hash, so heavy
    // collapse below this bound indicates the mix is discarding entropy
    // from the input for this particular key pattern.
    private static final double MIN_DISTINCT_FRACTION = 0.9;

    public void testUniformRandomKeys() {
        long[] keys = new long[KEY_COUNT];
        for (int i = 0; i < KEY_COUNT; i++) {
            keys[i] = randomLong();
        }
        assertWellDistributed("uniform random", keys);
    }

    public void testSequentialSmallKeys() {
        long base = randomIntBetween(0, 1000);
        long[] keys = new long[KEY_COUNT];
        for (int i = 0; i < KEY_COUNT; i++) {
            keys[i] = base + i;
        }
        assertWellDistributed("sequential small", keys);
    }

    public void testSequentialLargeMagnitudeKeys() {
        // Comfortably above 2^32, as e.g. millisecond-epoch timestamps are.
        long base = randomLongBetween(2_000_000_000_000L, 2_000_000_000_000_000L);
        long[] keys = new long[KEY_COUNT];
        for (int i = 0; i < KEY_COUNT; i++) {
            keys[i] = base + i;
        }
        assertWellDistributed("sequential large magnitude", keys);
    }

    public void testHighBitsVaryOnly() {
        // Fixed low 32 bits, only bits >= 32 vary: catches a mix that only
        // looks at the low half of the key.
        long lowFixed = randomLong() & 0xFFFF_FFFFL;
        long[] keys = new long[KEY_COUNT];
        for (int i = 0; i < KEY_COUNT; i++) {
            keys[i] = ((long) i << 32) | lowFixed;
        }
        assertWellDistributed("high bits vary only", keys);
    }

    public void testLowBitsVaryOnlyAtLargeMagnitude() {
        // Fixed high bits well above 2^32, only the low 18 bits vary (2^18
        // > KEY_COUNT). Mask off highFixed's own low 18 bits first: OR-ing
        // with a value that already has some of those bits set would force
        // them to 1 regardless of i, collapsing the intended KEY_COUNT
        // distinct low-bit patterns into far fewer actual keys.
        long highFixed = randomLongBetween(1L << 40, 1L << 50) & ~((1L << 18) - 1);
        long[] keys = new long[KEY_COUNT];
        for (int i = 0; i < KEY_COUNT; i++) {
            keys[i] = highFixed | i;
        }
        assertWellDistributed("low bits vary only, large magnitude", keys);
    }

    private void assertWellDistributed(String label, long[] keys) {
        int[] buckets = new int[BUCKETS];
        Set<Integer> distinctHashes = new HashSet<>();
        for (long key : keys) {
            int hash = BitMixer.mix(key);
            buckets[hash & BUCKET_MASK]++;
            distinctHashes.add(hash);
        }
        int maxLoad = 0;
        for (int load : buckets) {
            maxLoad = Math.max(maxLoad, load);
        }
        double average = (double) keys.length / BUCKETS;
        assertThat(
            label + ": max bucket load " + maxLoad + " (average " + average + ")",
            maxLoad,
            lessThanOrEqualTo((int) Math.ceil(average * MAX_LOAD_FACTOR))
        );
        assertThat(
            label + ": distinct hash count " + distinctHashes.size() + " of " + keys.length,
            distinctHashes.size(),
            greaterThanOrEqualTo((int) (keys.length * MIN_DISTINCT_FRACTION))
        );
    }
}
