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

import java.math.BigInteger;

/**
 * MurmurHash3 hashing functions.
 */
public enum MurmurHash3 {
    ;

    /**
     * A 128-bits hash.
     */
    public static class Hash128 {
        /** lower 64 bits part **/
        public long h1;
        /** higher 64 bits part **/
        public long h2;

        public Hash128() {}

        public Hash128(long h1, long h2) {
            this.h1 = h1;
            this.h2 = h2;
        }

        public byte[] getBytes() {
            byte[] hash = new byte[16];
            getBytes(hash, 0);
            return hash;
        }

        public void getBytes(byte[] bytes, int offset) {
            ByteUtils.writeLongBE(h1, bytes, offset);
            ByteUtils.writeLongBE(h2, bytes, offset + 8);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Hash128 that = (Hash128) other;
            return this.h1 == that.h1 && this.h2 == that.h2;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(h1 ^ h2);
        }

        @Override
        public String toString() {
            byte[] longBytes = new byte[17];
            getBytes(longBytes, 1);
            BigInteger bi = new BigInteger(longBytes);
            return "0x" + bi.toString(16);
        }
    }

    /** Index of the {@code h1} word within a murmur3-128 accumulator state. */
    public static final int STATE_H1 = 0;
    /** Index of the {@code h2} word within a murmur3-128 accumulator state. */
    public static final int STATE_H2 = 1;
    /** Number of {@code long}s occupied by one murmur3-128 accumulator state. */
    public static final int STATE_SIZE = 2;

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    public static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long mixK1(long k1) {
        k1 *= C1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= C2;
        return k1;
    }

    private static long mixK2(long k2) {
        k2 *= C2;
        k2 = Long.rotateLeft(k2, 33);
        k2 *= C1;
        return k2;
    }

    /**
     * The {@code h1} step of one complete 16-byte block.
     *
     * @param h2 the accumulator's {@code h2} from <i>before</i> this block
     */
    private static long nextH1(long h1, long h2, long k1) {
        h1 ^= mixK1(k1);
        h1 = Long.rotateLeft(h1, 27);
        h1 += h2;
        return h1 * 5 + 0x52dce729;
    }

    /**
     * The {@code h2} step of one complete 16-byte block.
     *
     * @param h1 the value {@link #nextH1} returned for this same block
     */
    private static long nextH2(long h2, long h1, long k2) {
        h2 ^= mixK2(k2);
        h2 = Long.rotateLeft(h2, 31);
        h2 += h1;
        return h2 * 5 + 0x38495ab5;
    }

    /** The length mix, fmix and cross-add that follow the tail. */
    private static Hash128 finish(Hash128 hash, int length, long h1, long h2) {
        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;

        hash.h1 = h1;
        hash.h2 = h2;
        return hash;
    }

    /**
     * Compute the hash of the MurmurHash3_x64_128 hashing function.
     *
     * Note, this hashing function might be used to persist hashes, so if the way hashes are computed
     * changes for some reason, it needs to be addressed (like in BloomFilter and MurmurHashField).
     */
    public static Hash128 hash128(byte[] key, int offset, int length, long seed, Hash128 hash) {
        long h1 = seed;
        long h2 = seed;

        // The block loop is written out here over scalar locals, and again in mixBlocks over a long[]
        // state, so that this one-shot API (the hottest hashing entry point in the codebase) stays
        // allocation-free rather than depending on escape analysis of a shared state array. Both
        // copies are two calls to the same nextH1/nextH2, so the arithmetic itself exists once.
        final int end = offset + (length & ~15);
        for (int i = offset; i < end; i += 16) {
            h1 = nextH1(h1, h2, ByteUtils.readLongLE(key, i));
            h2 = nextH2(h2, h1, ByteUtils.readLongLE(key, i + 8));
        }

        // `end`, not `offset`: the tail starts after the last complete block. When length < 16 no
        // block ran and end == offset, which is why no length guard is needed around the loop.
        return finalizeHash(hash, key, end, length, h1, h2);
    }

    /**
     * Initialises a murmur3-128 accumulator state held in {@code state[stateOffset]} ({@code h1}) and
     * {@code state[stateOffset + 1]} ({@code h2}). A zero-filled array is already correctly
     * initialised for seed 0, so this call can be skipped in that common case.
     */
    public static void initState(long[] state, int stateOffset, long seed) {
        state[stateOffset + STATE_H1] = seed;
        state[stateOffset + STATE_H2] = seed;
    }

    /**
     * Mixes one complete 16-byte block, supplied as its two little-endian words, into the
     * caller-owned accumulator state at {@code stateOffset}.
     *
     * <p>{@code k1} is the little-endian {@code long} formed by bytes 0..7 of the block and
     * {@code k2} the one formed by bytes 8..15, so a caller that already holds {@code long}s can feed
     * them directly instead of staging them through a byte buffer.
     *
     * <p>A stream folded with this method and completed with {@link #finalizeAlignedHash} or
     * {@link #finalizeHashWithLongTail} is bit-identical to hashing the equivalent byte array with
     * {@link #hash128}.
     */
    public static void mixBlock(long[] state, int stateOffset, long k1, long k2) {
        long h1 = state[stateOffset + STATE_H1];
        long h2 = state[stateOffset + STATE_H2];
        h1 = nextH1(h1, h2, k1);
        h2 = nextH2(h2, h1, k2);
        state[stateOffset + STATE_H1] = h1;
        state[stateOffset + STATE_H2] = h2;
    }

    /**
     * Mixes two consecutive complete blocks (32 bytes, four little-endian words) into the accumulator
     * state, loading and storing the state once. Identical to two {@link #mixBlock} calls.
     */
    public static void mixTwoBlocks(long[] state, int stateOffset, long k1, long k2, long k3, long k4) {
        long h1 = state[stateOffset + STATE_H1];
        long h2 = state[stateOffset + STATE_H2];
        h1 = nextH1(h1, h2, k1);
        h2 = nextH2(h2, h1, k2);
        h1 = nextH1(h1, h2, k3);
        h2 = nextH2(h2, h1, k4);
        state[stateOffset + STATE_H1] = h1;
        state[stateOffset + STATE_H2] = h2;
    }

    /**
     * Mixes the leading {@code length & ~15} bytes of {@code key} from {@code offset} into the
     * accumulator state at {@code stateOffset}.
     *
     * @return the offset just past the last complete block, i.e. where the unprocessed tail begins
     */
    public static int mixBlocks(byte[] key, int offset, int length, long[] state, int stateOffset) {
        long h1 = state[stateOffset + STATE_H1];
        long h2 = state[stateOffset + STATE_H2];
        final int end = offset + (length & ~15);
        for (int i = offset; i < end; i += 16) {
            h1 = nextH1(h1, h2, ByteUtils.readLongLE(key, i));
            h2 = nextH2(h2, h1, ByteUtils.readLongLE(key, i + 8));
        }
        state[stateOffset + STATE_H1] = h1;
        state[stateOffset + STATE_H2] = h2;
        return end;
    }

    /**
     * Finalises a hash whose input length is an exact multiple of 16, so every byte has already been
     * mixed as a complete block and there is no tail. Equivalent to
     * {@link #finalizeHash} with {@code length % 16 == 0}, where the tail switch matches no case and
     * the remainder array is never read.
     *
     * @param length total number of <i>bytes</i> hashed, not the number of blocks. It is mixed into
     *               the result, so a wrong value silently yields a different hash.
     */
    public static Hash128 finalizeAlignedHash(Hash128 hash, int length, long h1, long h2) {
        assert (length & 15) == 0 : "not block aligned: " + length;
        return finish(hash, length, h1, h2);
    }

    /** {@link #finalizeAlignedHash} reading {@code h1}/{@code h2} from an accumulator state. */
    public static Hash128 finalizeAlignedHash(Hash128 hash, int length, long[] state, int stateOffset) {
        return finalizeAlignedHash(hash, length, state[stateOffset + STATE_H1], state[stateOffset + STATE_H2]);
    }

    /**
     * Finalises a hash whose input is {@code length - 8} bytes already mixed as complete blocks plus
     * a trailing 8-byte partial block holding {@code tail} in little-endian order.
     *
     * <p>Equivalent to writing {@code tail} little-endian into an 8-byte remainder and calling
     * {@link #finalizeHash}: for {@code length & 15 == 8} the tail switch enters at case 8, and cases
     * 8..1 reassemble exactly that little-endian {@code long}, while {@code k2} stays zero so
     * {@code h2} is untouched by the tail. Note this is the {@code k1} tail mix, <b>not</b> a block
     * step — there is no rotate-add-multiply epilogue.
     *
     * @param length total number of bytes hashed; must satisfy {@code length & 15 == 8}
     */
    public static Hash128 finalizeHashWithLongTail(Hash128 hash, int length, long h1, long h2, long tail) {
        assert (length & 15) == 8 : "not an 8 byte tail: " + length;
        return finish(hash, length, h1 ^ mixK1(tail), h2);
    }

    /** {@link #finalizeHashWithLongTail} reading {@code h1}/{@code h2} from an accumulator state. */
    public static Hash128 finalizeHashWithLongTail(Hash128 hash, int length, long[] state, int stateOffset, long tail) {
        return finalizeHashWithLongTail(hash, length, state[stateOffset + STATE_H1], state[stateOffset + STATE_H2], tail);
    }

    /**
     * The {@code h1} word of the murmur3-128 hash (seed 0) of the eight little-endian bytes of
     * {@code value}. Allocation-free equivalent of hashing a single {@code long} through
     * {@code BufferedMurmur3Hasher}.
     *
     * @param scratch reusable output holder; its contents are overwritten
     */
    public static long hashLongToH1(long value, Hash128 scratch) {
        return finalizeHashWithLongTail(scratch, Long.BYTES, 0L, 0L, value).h1;
    }

    @SuppressWarnings("fallthrough") // Intentionally uses fallthrough to implement a well known hashing algorithm
    static Hash128 finalizeHash(Hash128 hash, byte[] remainder, int offset, int length, long h1, long h2) {
        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= (remainder[offset + 14] & 0xFFL) << 48;
            case 14:
                k2 ^= (remainder[offset + 13] & 0xFFL) << 40;
            case 13:
                k2 ^= (remainder[offset + 12] & 0xFFL) << 32;
            case 12:
                k2 ^= (remainder[offset + 11] & 0xFFL) << 24;
            case 11:
                k2 ^= (remainder[offset + 10] & 0xFFL) << 16;
            case 10:
                k2 ^= (remainder[offset + 9] & 0xFFL) << 8;
            case 9:
                k2 ^= (remainder[offset + 8] & 0xFFL) << 0;
                h2 ^= mixK2(k2);

            case 8:
                k1 ^= (remainder[offset + 7] & 0xFFL) << 56;
            case 7:
                k1 ^= (remainder[offset + 6] & 0xFFL) << 48;
            case 6:
                k1 ^= (remainder[offset + 5] & 0xFFL) << 40;
            case 5:
                k1 ^= (remainder[offset + 4] & 0xFFL) << 32;
            case 4:
                k1 ^= (remainder[offset + 3] & 0xFFL) << 24;
            case 3:
                k1 ^= (remainder[offset + 2] & 0xFFL) << 16;
            case 2:
                k1 ^= (remainder[offset + 1] & 0xFFL) << 8;
            case 1:
                k1 ^= (remainder[offset] & 0xFFL);
                h1 ^= mixK1(k1);
        }

        return finish(hash, length, h1, h2);
    }

    /**
     * A 64-bit variant which accepts a long to hash, and returns the 64bit long hash.
     * This is useful if the input is already in long (or smaller) format and you don't
     * need the full 128b width and flexibility of
     * {@link MurmurHash3#hash128(byte[], int, int, long, Hash128)}
     *
     * Given the limited nature of this variant, it should be faster than the 128b version
     * when you only need 128b (many fewer instructions)
     */
    public static long murmur64(long h) {
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

}
