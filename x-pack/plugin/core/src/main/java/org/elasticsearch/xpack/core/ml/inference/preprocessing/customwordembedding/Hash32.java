/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 * This Java port of CLD3 was derived from Google's CLD3 project at https://github.com/google/cld3
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import java.nio.charset.StandardCharsets;

/**
 * Custom Hash class necessary for hashing nGrams
 */
final class Hash32 {

    private static final int DEFAULT_SEED = 0xBEEF;

    private final int seed;

    Hash32(int seed) {
        this.seed = seed;
    }

    Hash32() {
        this(DEFAULT_SEED);
    }

    public long hash(String input) {
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        return Integer.toUnsignedLong(hash32(bytes));
    }

    /**
     * Derived from https://github.com/google/cld3/blob/06f695f1c8ee530104416aab5dcf2d6a1414a56a/src/utils.cc#L137
     *
     * It is critical that we utilize this hash as it determines which weight and quantile column/row we choose
     * when building the feature array.
     */
    private int hash32(byte[] data) {
        int n = data.length;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        int m = 0x5bd1e995;
        int r = 24;

        // Initialize the hash to a 'random' value
        int h = (seed ^ n);

        // Mix 4 bytes at a time into the hash
        int i = 0;
        while (n >= 4) {
            int k = decodeFixed32(data, i);
            k *= m;
            k ^= k >>> r; // use unsigned shift
            k *= m;
            h *= m;
            h ^= k;
            i += 4;
            n -= 4;
        }

        // Handle the last few bytes of the input array
        if (n == 3) {
            h ^= Byte.toUnsignedInt(data[i + 2]) << 16;
        }
        if (n >= 2) {
            h ^= Byte.toUnsignedInt(data[i + 1]) << 8;
        }
        if (n >= 1) {
            h ^= Byte.toUnsignedInt(data[i]);
            h *= m;
        }

        // Do a few final mixes of the hash to ensure the last few
        // bytes are well-incorporated.
        h ^= h >>> 13; // use unsigned shift
        h *= m;
        h ^= h >>> 15; // use unsigned shift
        return h;
    }

    private static int decodeFixed32(byte[] ptr, int offset) {
        return Byte.toUnsignedInt(ptr[offset]) | Byte.toUnsignedInt(ptr[offset + 1]) << 8 | Byte.toUnsignedInt(ptr[offset + 2]) << 16 | Byte
            .toUnsignedInt(ptr[offset + 3]) << 24;
    }

}
