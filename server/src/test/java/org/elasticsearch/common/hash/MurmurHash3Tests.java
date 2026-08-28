/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.hash;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class MurmurHash3Tests extends ESTestCase {

    public void testKnownValues() {
        assertHash(0x629942693e10f867L, 0x92db0b82baeb5347L, "hell", 0);
        assertHash(0xa78ddff5adae8d10L, 0x128900ef20900135L, "hello", 1);
        assertHash(0x8a486b23f422e826L, 0xf962a2c58947765fL, "hello ", 2);
        assertHash(0x2ea59f466f6bed8cL, 0xc610990acc428a17L, "hello w", 3);
        assertHash(0x79f6305a386c572cL, 0x46305aed3483b94eL, "hello wo", 4);
        assertHash(0xc2219d213ec1f1b5L, 0xa1d8e2e0a52785bdL, "hello wor", 5);
        assertHash(0xe34bbc7bbc071b6cL, 0x7a433ca9c49a9347L, "The quick brown fox jumps over the lazy dog", 0);
        assertHash(0x658ca970ff85269aL, 0x43fee3eaa68e5c3eL, "The quick brown fox jumps over the lazy cog", 0);
    }

    public void testMixBlockMatchesHash128() {
        int blocks = randomIntBetween(1, 32);
        byte[] bytes = randomByteArrayOfLength(blocks * 16);

        long[] state = new long[MurmurHash3.STATE_SIZE];
        for (int i = 0; i < blocks; i++) {
            MurmurHash3.mixBlock(state, 0, ByteUtils.readLongLE(bytes, i * 16), ByteUtils.readLongLE(bytes, i * 16 + 8));
        }
        MurmurHash3.Hash128 actual = MurmurHash3.finalizeAlignedHash(new MurmurHash3.Hash128(), bytes.length, state, 0);

        assertEquals(MurmurHash3.hash128(bytes, 0, bytes.length, 0L, new MurmurHash3.Hash128()), actual);
    }

    public void testMixTwoBlocksMatchesTwoMixBlocks() {
        long k1 = randomLong();
        long k2 = randomLong();
        long k3 = randomLong();
        long k4 = randomLong();

        long[] pair = new long[MurmurHash3.STATE_SIZE];
        MurmurHash3.mixBlock(pair, 0, k1, k2);
        MurmurHash3.mixBlock(pair, 0, k3, k4);

        long[] fused = new long[MurmurHash3.STATE_SIZE];
        MurmurHash3.mixTwoBlocks(fused, 0, k1, k2, k3, k4);

        assertEquals(pair[MurmurHash3.STATE_H1], fused[MurmurHash3.STATE_H1]);
        assertEquals(pair[MurmurHash3.STATE_H2], fused[MurmurHash3.STATE_H2]);
    }

    // Todo: This is essentially implementing the columnar hashing for a test. When we commit the actual columnar implementation we should
    // test at that level and remove this.
    public void testLongStreamWithOptionalTailMatchesBufferedHasher() {
        for (int n = 1; n <= 9; n++) {
            long[] values = new long[n];
            for (int i = 0; i < n; i++) {
                values[i] = randomLong();
            }

            long[] state = new long[MurmurHash3.STATE_SIZE];
            int i = 0;
            for (; i + 1 < n; i += 2) {
                MurmurHash3.mixBlock(state, 0, values[i], values[i + 1]);
            }
            int byteLength = n * Long.BYTES;
            MurmurHash3.Hash128 actual = new MurmurHash3.Hash128();
            if ((n & 1) == 0) {
                MurmurHash3.finalizeAlignedHash(actual, byteLength, state, 0);
            } else {
                MurmurHash3.finalizeHashWithLongTail(actual, byteLength, state, 0, values[n - 1]);
            }

            BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);
            for (long v : values) {
                hasher.addLong(v);
            }
            assertEquals("n=" + n, hasher.digestHash(), actual);
        }
    }

    /**
     * Four longs per element folded via {@code mixTwoBlocks} must equal
     * {@link BufferedMurmur3Hasher#addLongs(long, long, long, long)}.
     */
    public void testFourLongsPerElementMatchesBufferedHasher() {
        int elements = randomIntBetween(1, 16);
        long[] values = new long[elements * 4];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomLong();
        }

        long[] state = new long[MurmurHash3.STATE_SIZE];
        for (int e = 0; e < elements; e++) {
            MurmurHash3.mixTwoBlocks(state, 0, values[e * 4], values[e * 4 + 1], values[e * 4 + 2], values[e * 4 + 3]);
        }
        MurmurHash3.Hash128 actual = MurmurHash3.finalizeAlignedHash(new MurmurHash3.Hash128(), elements * 32, state, 0);

        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);
        for (int e = 0; e < elements; e++) {
            hasher.addLongs(values[e * 4], values[e * 4 + 1], values[e * 4 + 2], values[e * 4 + 3]);
        }
        assertEquals(hasher.digestHash(), actual);
    }

    /**
     * A block-folded stream with an arbitrary (non-block-aligned) tail handed to the generic
     * {@code finalizeHash} must still match {@code hash128} over the same bytes.
     */
    public void testBlockFoldWithArbitraryTailMatchesHash128() {
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(0, 200));
        long[] state = new long[MurmurHash3.STATE_SIZE];

        int tailOffset = bytes.length & ~15;
        for (int i = 0; i < tailOffset; i += 16) {
            MurmurHash3.mixBlock(state, 0, ByteUtils.readLongLE(bytes, i), ByteUtils.readLongLE(bytes, i + 8));
        }

        MurmurHash3.Hash128 actual = MurmurHash3.finalizeHash(
            new MurmurHash3.Hash128(),
            bytes,
            tailOffset,
            bytes.length,
            state[MurmurHash3.STATE_H1],
            state[MurmurHash3.STATE_H2]
        );
        assertEquals(MurmurHash3.hash128(bytes, 0, bytes.length, 0L, new MurmurHash3.Hash128()), actual);
    }

    /**
     * {@code finalizeHashWithLongTail} must equal the generic tail path fed the same eight bytes in
     * little-endian order. This pins the claim that tail cases 8..1 reassemble exactly that long.
     */
    public void testLongTailMatchesGenericByteTail() {
        int blocks = randomIntBetween(0, 4);
        long tail = randomLong();
        int byteLength = blocks * 16 + Long.BYTES;

        long[] state = new long[MurmurHash3.STATE_SIZE];
        for (int i = 0; i < blocks; i++) {
            MurmurHash3.mixBlock(state, 0, randomLong(), randomLong());
        }
        long h1 = state[MurmurHash3.STATE_H1];
        long h2 = state[MurmurHash3.STATE_H2];

        byte[] remainder = new byte[Long.BYTES];
        ByteUtils.writeLongLE(tail, remainder, 0);

        assertEquals(
            MurmurHash3.finalizeHash(new MurmurHash3.Hash128(), remainder, 0, byteLength, h1, h2),
            MurmurHash3.finalizeHashWithLongTail(new MurmurHash3.Hash128(), byteLength, h1, h2, tail)
        );
    }

    public void testHashLongToH1MatchesBufferedHasher() {
        long value = randomLong();

        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);
        hasher.addLong(value);

        assertEquals(hasher.digestHash().h1, MurmurHash3.hashLongToH1(value, new MurmurHash3.Hash128()));
    }

    /**
     * Independent accumulators packed into one array at different offsets must not interfere. This is
     * the columnar layout: one state per document row in a single {@code long[]}.
     */
    public void testInterleavedStatesAtDifferentOffsets() {
        int states = randomIntBetween(2, 8);
        int rounds = randomIntBetween(1, 6);
        long[][] inputs = new long[states][rounds * 2];
        for (int s = 0; s < states; s++) {
            for (int i = 0; i < rounds * 2; i++) {
                inputs[s][i] = randomLong();
            }
        }

        // Fold round-major, so every state is advanced interleaved with the others.
        long[] shared = new long[states * MurmurHash3.STATE_SIZE];
        for (int r = 0; r < rounds; r++) {
            for (int s = 0; s < states; s++) {
                MurmurHash3.mixBlock(shared, s * MurmurHash3.STATE_SIZE, inputs[s][r * 2], inputs[s][r * 2 + 1]);
            }
        }

        for (int s = 0; s < states; s++) {
            long[] isolated = new long[MurmurHash3.STATE_SIZE];
            for (int r = 0; r < rounds; r++) {
                MurmurHash3.mixBlock(isolated, 0, inputs[s][r * 2], inputs[s][r * 2 + 1]);
            }
            int byteLength = rounds * 16;
            assertEquals(
                "state " + s,
                MurmurHash3.finalizeAlignedHash(new MurmurHash3.Hash128(), byteLength, isolated, 0),
                MurmurHash3.finalizeAlignedHash(new MurmurHash3.Hash128(), byteLength, shared, s * MurmurHash3.STATE_SIZE)
            );
        }
    }

    /**
     * Seed 0 is just the zero-filled state, which is what the columnar accumulator relies on. Any
     * other seed is written into both state words; this pins that layout against {@code hash128}.
     */
    public void testNonZeroSeedViaSeededState() {
        long seed = randomLong();
        int blocks = randomIntBetween(1, 8);
        byte[] bytes = randomByteArrayOfLength(blocks * 16);

        long[] state = new long[MurmurHash3.STATE_SIZE];
        state[MurmurHash3.STATE_H1] = seed;
        state[MurmurHash3.STATE_H2] = seed;
        for (int i = 0; i < blocks; i++) {
            MurmurHash3.mixBlock(state, 0, ByteUtils.readLongLE(bytes, i * 16), ByteUtils.readLongLE(bytes, i * 16 + 8));
        }
        MurmurHash3.Hash128 actual = MurmurHash3.finalizeAlignedHash(new MurmurHash3.Hash128(), bytes.length, state, 0);

        assertEquals(MurmurHash3.hash128(bytes, 0, bytes.length, seed, new MurmurHash3.Hash128()), actual);
    }

    private static void assertHash(long lower, long upper, String inputString, long seed) {
        byte[] bytes = inputString.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 expected = new MurmurHash3.Hash128();
        expected.h1 = lower;
        expected.h2 = upper;
        assertHash(expected, MurmurHash3.hash128(bytes, 0, bytes.length, seed, new MurmurHash3.Hash128()));
    }

    private static void assertHash(MurmurHash3.Hash128 expected, MurmurHash3.Hash128 actual) {
        assertEquals(expected.h1, actual.h1);
        assertEquals(expected.h2, actual.h2);
    }
}
