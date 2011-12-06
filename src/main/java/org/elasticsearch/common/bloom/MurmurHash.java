/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.bloom;

import java.nio.ByteBuffer;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * <p/>
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
public class MurmurHash {
    public static int hash32(ByteBuffer data, int offset, int length, int seed) {
        int m = 0x5bd1e995;
        int r = 24;

        int h = seed ^ length;

        int len_4 = length >> 2;

        for (int i = 0; i < len_4; i++) {
            int i_4 = i << 2;
            int k = data.get(offset + i_4 + 3);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 2) & 0xff);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 1) & 0xff);
            k = k << 8;
            k = k | (data.get(offset + i_4 + 0) & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // avoid calculating modulo
        int len_m = len_4 << 2;
        int left = length - len_m;

        if (left != 0) {
            if (left >= 3) {
                h ^= (int) data.get(offset + length - 3) << 16;
            }
            if (left >= 2) {
                h ^= (int) data.get(offset + length - 2) << 8;
            }
            if (left >= 1) {
                h ^= (int) data.get(offset + length - 1);
            }

            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static long hash64(ByteBuffer key, int offset, int length, long seed) {
        long m64 = 0xc6a4a7935bd1e995L;
        int r64 = 47;

        long h64 = (seed & 0xffffffffL) ^ (m64 * length);

        int lenLongs = length >> 3;

        for (int i = 0; i < lenLongs; ++i) {
            int i_8 = i << 3;

            long k64 = ((long) key.get(offset + i_8 + 0) & 0xff) + (((long) key.get(offset + i_8 + 1) & 0xff) << 8) +
                    (((long) key.get(offset + i_8 + 2) & 0xff) << 16) + (((long) key.get(offset + i_8 + 3) & 0xff) << 24) +
                    (((long) key.get(offset + i_8 + 4) & 0xff) << 32) + (((long) key.get(offset + i_8 + 5) & 0xff) << 40) +
                    (((long) key.get(offset + i_8 + 6) & 0xff) << 48) + (((long) key.get(offset + i_8 + 7) & 0xff) << 56);

            k64 *= m64;
            k64 ^= k64 >>> r64;
            k64 *= m64;

            h64 ^= k64;
            h64 *= m64;
        }

        int rem = length & 0x7;

        switch (rem) {
            case 0:
                break;
            case 7:
                h64 ^= (long) key.get(offset + length - rem + 6) << 48;
            case 6:
                h64 ^= (long) key.get(offset + length - rem + 5) << 40;
            case 5:
                h64 ^= (long) key.get(offset + length - rem + 4) << 32;
            case 4:
                h64 ^= (long) key.get(offset + length - rem + 3) << 24;
            case 3:
                h64 ^= (long) key.get(offset + length - rem + 2) << 16;
            case 2:
                h64 ^= (long) key.get(offset + length - rem + 1) << 8;
            case 1:
                h64 ^= (long) key.get(offset + length - rem);
                h64 *= m64;
        }

        h64 ^= h64 >>> r64;
        h64 *= m64;
        h64 ^= h64 >>> r64;

        return h64;
    }

    public static long hash64(byte[] key, int offset, int length, long seed) {
        long m64 = 0xc6a4a7935bd1e995L;
        int r64 = 47;

        long h64 = (seed & 0xffffffffL) ^ (m64 * length);

        int lenLongs = length >> 3;

        for (int i = 0; i < lenLongs; ++i) {
            int i_8 = i << 3;

            long k64 = ((long) key[offset + i_8 + 0] & 0xff) + (((long) key[offset + i_8 + 1] & 0xff) << 8) +
                    (((long) key[offset + i_8 + 2] & 0xff) << 16) + (((long) key[offset + i_8 + 3] & 0xff) << 24) +
                    (((long) key[offset + i_8 + 4] & 0xff) << 32) + (((long) key[offset + i_8 + 5] & 0xff) << 40) +
                    (((long) key[offset + i_8 + 6] & 0xff) << 48) + (((long) key[offset + i_8 + 7] & 0xff) << 56);

            k64 *= m64;
            k64 ^= k64 >>> r64;
            k64 *= m64;

            h64 ^= k64;
            h64 *= m64;
        }

        int rem = length & 0x7;

        switch (rem) {
            case 0:
                break;
            case 7:
                h64 ^= (long) key[offset + length - rem + 6] << 48;
            case 6:
                h64 ^= (long) key[offset + length - rem + 5] << 40;
            case 5:
                h64 ^= (long) key[offset + length - rem + 4] << 32;
            case 4:
                h64 ^= (long) key[offset + length - rem + 3] << 24;
            case 3:
                h64 ^= (long) key[offset + length - rem + 2] << 16;
            case 2:
                h64 ^= (long) key[offset + length - rem + 1] << 8;
            case 1:
                h64 ^= (long) key[offset + length - rem];
                h64 *= m64;
        }

        h64 ^= h64 >>> r64;
        h64 *= m64;
        h64 ^= h64 >>> r64;

        return h64;
    }
}
