/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.common.hash;

import java.nio.charset.StandardCharsets;

/**
 * Provides hashing function using FNV1a hash function. @see <a href="http://www.isthe.com/chongo/tech/comp/fnv/#FNV-1">FNV author's website</a>.
 * 32 bit Java port of http://www.isthe.com/chongo/src/fnv/hash_32a.c
 * 64 bit Java port of http://www.isthe.com/chongo/src/fnv/hash_64a.c
 *
 * @opensearch.internal
 */
public class FNV1a {
    private static final long FNV_OFFSET_BASIS_32 = 0x811c9dc5L;
    private static final long FNV_PRIME_32 = 0x01000193L;

    private static final long FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325L;
    private static final long FNV_PRIME_64 = 0x100000001b3L;

    // FNV-1a hash computation for 32-bit hash
    public static long hash32(String input) {
        long hash = FNV_OFFSET_BASIS_32;
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        for (byte b : bytes) {
            hash ^= (b & 0xFF);
            hash *= FNV_PRIME_32;
        }
        return hash;
    }

    // FNV-1a hash computation for 64-bit hash
    public static long hash64(String input) {
        long hash = FNV_OFFSET_BASIS_64;
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        for (byte b : bytes) {
            hash ^= (b & 0xFF);
            hash *= FNV_PRIME_64;
        }
        return hash;
    }
}
