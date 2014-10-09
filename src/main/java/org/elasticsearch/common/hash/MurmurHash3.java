/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.hash;

import org.elasticsearch.common.util.ByteUtils;


/**
 * MurmurHash3 hashing functions.
 */
public enum MurmurHash3 {
    ;

    /**
     * A 128-bits hash.
     */
    public static class Hash128 {
        public long h1, h2;
    }

    private static long C1 = 0x87c37b91114253d5L;
    private static long C2 = 0x4cf5ad432745937fL;

    protected static long getblock(byte[] key, int offset, int index) {
        int i_8 = index << 3;
        int blockOffset = offset + i_8;
        return ByteUtils.readLongLE(key, blockOffset);
    }

    protected static long fmix(long k) {
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
    public static Hash128 hash128(byte[] key, int offset, int length, long seed, Hash128 hash) {
        long h1 = seed;
        long h2 = seed;

        if (length >= 16) {

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
        }

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= (key[offset + 14] & 0xFFL) << 48;
            case 14:
                k2 ^= (key[offset + 13] & 0xFFL) << 40;
            case 13:
                k2 ^= (key[offset + 12] & 0xFFL) << 32;
            case 12:
                k2 ^= (key[offset + 11] & 0xFFL) << 24;
            case 11:
                k2 ^= (key[offset + 10] & 0xFFL) << 16;
            case 10:
                k2 ^= (key[offset + 9] & 0xFFL) << 8;
            case 9:
                k2 ^= (key[offset + 8] & 0xFFL) << 0;
                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;

            case 8:
                k1 ^= (key[offset + 7] & 0xFFL) << 56;
            case 7:
                k1 ^= (key[offset + 6] & 0xFFL) << 48;
            case 6:
                k1 ^= (key[offset + 5] & 0xFFL) << 40;
            case 5:
                k1 ^= (key[offset + 4] & 0xFFL) << 32;
            case 4:
                k1 ^= (key[offset + 3] & 0xFFL) << 24;
            case 3:
                k1 ^= (key[offset + 2] & 0xFFL) << 16;
            case 2:
                k1 ^= (key[offset + 1] & 0xFFL) << 8;
            case 1:
                k1 ^= (key[offset] & 0xFFL);
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

}
