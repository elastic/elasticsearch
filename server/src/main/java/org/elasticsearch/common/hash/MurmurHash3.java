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
import java.util.Objects;

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
            return Objects.equals(this.h1, that.h1) && Objects.equals(this.h2, that.h2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(h1, h2);
        }

        @Override
        public String toString() {
            byte[] longBytes = new byte[17];
            getBytes(longBytes, 1);
            BigInteger bi = new BigInteger(longBytes);
            return "0x" + bi.toString(16);
        }
    }

    static class IntermediateResult {
        int offset;
        long h1;
        long h2;

        IntermediateResult(int offset, long h1, long h2) {
            this.offset = offset;
            this.h1 = h1;
            this.h2 = h2;
        }
    }

    private static long C1 = 0x87c37b91114253d5L;
    private static long C2 = 0x4cf5ad432745937fL;

    public static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    /**
     * Compute the hash of the MurmurHash3_x64_128 hashing function.
     *
     * Note, this hashing function might be used to persist hashes, so if the way hashes are computed
     * changes for some reason, it needs to be addressed (like in BloomFilter and MurmurHashField).
     */
    @SuppressWarnings("fallthrough") // Intentionally uses fallthrough to implement a well known hashing algorithm
    public static Hash128 hash128(byte[] key, int offset, int length, long seed, Hash128 hash) {
        long h1 = seed;
        long h2 = seed;

        if (length >= 16) {
            IntermediateResult result = intermediateHash(key, offset, length, h1, h2);
            h1 = result.h1;
            h2 = result.h2;
            offset = result.offset;
        }

        return finalizeHash(hash, key, offset, length, h1, h2);
    }

    static IntermediateResult intermediateHash(byte[] key, int offset, int length, long h1, long h2) {
        final int len16 = length & 0xFFFFFFF0; // higher multiple of 16 that is lower than or equal to length
        final int end = offset + len16;
        for (int i = offset; i < end; i += 16) {
            long k1 = ByteUtils.readLongLE(key, i);
            long k2 = ByteUtils.readLongLE(key, i + 8);

            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        // Advance offset to the unprocessed tail of the data.
        offset = end;

        return new IntermediateResult(offset, h1, h2);
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
                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;

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
                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;
        }

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
