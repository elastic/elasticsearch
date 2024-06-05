/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.geometry.utils;

/**
 * Utilities for common Bit twiddling methods. Borrowed heavily from Lucene (org.apache.lucene.util.BitUtil).
 */
public class BitUtil {  // magic numbers for bit interleaving

    private BitUtil() {}

    private static final long MAGIC[] = {
        0x5555555555555555L,
        0x3333333333333333L,
        0x0F0F0F0F0F0F0F0FL,
        0x00FF00FF00FF00FFL,
        0x0000FFFF0000FFFFL,
        0x00000000FFFFFFFFL,
        0xAAAAAAAAAAAAAAAAL };
    // shift values for bit interleaving
    private static final short SHIFT[] = { 1, 2, 4, 8, 16 };

    /**
     * Interleaves the first 32 bits of each long value
     *
     * Adapted from: http://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
     */
    public static long interleave(int even, int odd) {
        long v1 = 0x00000000FFFFFFFFL & even;
        long v2 = 0x00000000FFFFFFFFL & odd;
        v1 = (v1 | (v1 << SHIFT[4])) & MAGIC[4];
        v1 = (v1 | (v1 << SHIFT[3])) & MAGIC[3];
        v1 = (v1 | (v1 << SHIFT[2])) & MAGIC[2];
        v1 = (v1 | (v1 << SHIFT[1])) & MAGIC[1];
        v1 = (v1 | (v1 << SHIFT[0])) & MAGIC[0];
        v2 = (v2 | (v2 << SHIFT[4])) & MAGIC[4];
        v2 = (v2 | (v2 << SHIFT[3])) & MAGIC[3];
        v2 = (v2 | (v2 << SHIFT[2])) & MAGIC[2];
        v2 = (v2 | (v2 << SHIFT[1])) & MAGIC[1];
        v2 = (v2 | (v2 << SHIFT[0])) & MAGIC[0];

        return (v2 << 1) | v1;
    }

    /**
     * Extract just the even-bits value as a long from the bit-interleaved value
     */
    public static long deinterleave(long b) {
        b &= MAGIC[0];
        b = (b ^ (b >>> SHIFT[0])) & MAGIC[1];
        b = (b ^ (b >>> SHIFT[1])) & MAGIC[2];
        b = (b ^ (b >>> SHIFT[2])) & MAGIC[3];
        b = (b ^ (b >>> SHIFT[3])) & MAGIC[4];
        b = (b ^ (b >>> SHIFT[4])) & MAGIC[5];
        return b;
    }

    /**
     * flip flops odd with even bits
     */
    public static final long flipFlop(final long b) {
        return ((b & MAGIC[6]) >>> 1) | ((b & MAGIC[0]) << 1);
    }
}
