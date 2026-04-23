/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.elasticsearch.common.util.ByteUtils;

public class BloomFilterHashFunctions {
    private BloomFilterHashFunctions() {}

    //
    // The following Murmur3 implementation is borrowed from commons-codec.
    //
    /**
     * Implementation of the MurmurHash3 128-bit hash functions.
     *
     * <p>
     * MurmurHash is a non-cryptographic hash function suitable for general hash-based lookup. The name comes from two basic
     * operations, multiply (MU) and rotate (R), used in its inner loop. Unlike cryptographic hash functions, it is not
     * specifically designed to be difficult to reverse by an adversary, making it unsuitable for cryptographic purposes.
     * </p>
     *
     * <p>
     * This contains a Java port of the 32-bit hash function {@code MurmurHash3_x86_32} and the 128-bit hash function
     * {@code MurmurHash3_x64_128} from Austin Appleby's original {@code c++} code in SMHasher.
     * </p>
     *
     * <p>
     * This is public domain code with no copyrights. From home page of
     * <a href="https://github.com/aappleby/smhasher">SMHasher</a>:
     * </p>
     *
     * <blockquote> "All MurmurHash versions are public domain software, and the author disclaims all copyright to their
     * code." </blockquote>
     *
     * <p>
     * Original adaption from Apache Hive. That adaption contains a {@code hash64} method that is not part of the original
     * MurmurHash3 code. It is not recommended to use these methods. They will be removed in a future release. To obtain a
     * 64-bit hash use half of the bits from the {@code hash128x64} methods using the input data converted to bytes.
     * </p>
     *
     * @see <a href="https://en.wikipedia.org/wiki/MurmurHash">MurmurHash</a>
     * @see <a href="https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp"> Original MurmurHash3 c++
     *      code</a>
     * @see <a href=
     *      "https://github.com/apache/hive/blob/master/storage-api/src/java/org/apache/hive/common/util/Murmur3.java">
     *      Apache Hive Murmer3</a>
     * @since 1.13
     */
    public static final class MurmurHash3 {
        /**
         * A default seed to use for the murmur hash algorithm.
         * Has the value {@code 104729}.
         */
        public static final int DEFAULT_SEED = 104729;

        // Constants for 128-bit variant
        private static final long C1 = 0x87c37b91114253d5L;
        private static final long C2 = 0x4cf5ad432745937fL;
        private static final int R1 = 31;
        private static final int R2 = 27;
        private static final int R3 = 33;
        private static final int M = 5;
        private static final int N1 = 0x52dce729;
        private static final int N2 = 0x38495ab5;

        /** No instance methods. */
        private MurmurHash3() {}

        /**
         * Generates 64-bit hash from the byte array with the given offset, length and seed by discarding the second value of the 128-bit
         * hash.
         *
         * This version uses the default seed.
         *
         * @param data The input byte array
         * @param offset The first element of array
         * @param length The length of array
         * @return The sum of the two 64-bit hashes that make up the hash128
         */
        @SuppressWarnings("fallthrough")
        public static long hash64(final byte[] data, final int offset, final int length) {
            long h1 = MurmurHash3.DEFAULT_SEED;
            long h2 = MurmurHash3.DEFAULT_SEED;
            final int nblocks = length >> 4;

            // body
            for (int i = 0; i < nblocks; i++) {
                final int index = offset + (i << 4);
                long k1 = ByteUtils.readLongLE(data, index);
                long k2 = ByteUtils.readLongLE(data, index + 8);

                // mix functions for k1
                k1 *= C1;
                k1 = Long.rotateLeft(k1, R1);
                k1 *= C2;
                h1 ^= k1;
                h1 = Long.rotateLeft(h1, R2);
                h1 += h2;
                h1 = h1 * M + N1;

                // mix functions for k2
                k2 *= C2;
                k2 = Long.rotateLeft(k2, R3);
                k2 *= C1;
                h2 ^= k2;
                h2 = Long.rotateLeft(h2, R1);
                h2 += h1;
                h2 = h2 * M + N2;
            }

            // tail
            long k1 = 0;
            long k2 = 0;
            final int index = offset + (nblocks << 4);
            switch (offset + length - index) {
                case 15:
                    k2 ^= ((long) data[index + 14] & 0xff) << 48;
                case 14:
                    k2 ^= ((long) data[index + 13] & 0xff) << 40;
                case 13:
                    k2 ^= ((long) data[index + 12] & 0xff) << 32;
                case 12:
                    k2 ^= ((long) data[index + 11] & 0xff) << 24;
                case 11:
                    k2 ^= ((long) data[index + 10] & 0xff) << 16;
                case 10:
                    k2 ^= ((long) data[index + 9] & 0xff) << 8;
                case 9:
                    k2 ^= data[index + 8] & 0xff;
                    k2 *= C2;
                    k2 = Long.rotateLeft(k2, R3);
                    k2 *= C1;
                    h2 ^= k2;

                case 8:
                    k1 ^= ((long) data[index + 7] & 0xff) << 56;
                case 7:
                    k1 ^= ((long) data[index + 6] & 0xff) << 48;
                case 6:
                    k1 ^= ((long) data[index + 5] & 0xff) << 40;
                case 5:
                    k1 ^= ((long) data[index + 4] & 0xff) << 32;
                case 4:
                    k1 ^= ((long) data[index + 3] & 0xff) << 24;
                case 3:
                    k1 ^= ((long) data[index + 2] & 0xff) << 16;
                case 2:
                    k1 ^= ((long) data[index + 1] & 0xff) << 8;
                case 1:
                    k1 ^= data[index] & 0xff;
                    k1 *= C1;
                    k1 = Long.rotateLeft(k1, R1);
                    k1 *= C2;
                    h1 ^= k1;
            }

            // finalization
            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix64(h1);
            h2 = fmix64(h2);

            h1 += h2;

            return h1;
        }

        /**
         * Performs the final avalanche mix step of the 64-bit hash function {@code MurmurHash3_x64_128}.
         *
         * @param hash The current hash
         * @return The final hash
         */
        private static long fmix64(long hash) {
            hash ^= (hash >>> 33);
            hash *= 0xff51afd7ed558ccdL;
            hash ^= (hash >>> 33);
            hash *= 0xc4ceb9fe1a85ec53L;
            hash ^= (hash >>> 33);
            return hash;
        }
    }
}
