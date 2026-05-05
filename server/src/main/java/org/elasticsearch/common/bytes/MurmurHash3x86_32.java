/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

/**
 * Streaming MurmurHash3 x86/32 implementation that can hash data arriving in
 * multiple full pages followed by one final (possibly short) page. The result
 * matches Lucene's {@link StringHelper#murmurhash3_x86_32} seeded with
 * {@link StringHelper#GOOD_FAST_HASH_SEED} — and therefore matches
 * {@link BytesRef#hashCode()}.
 * <p>
 *     Pages fed via {@link #fullPage} must each be exactly divisible by 4 bytes.
 *     The final page is fed via {@link #lastPage} and may be any length.
 * </p>
 */
class MurmurHash3x86_32 {
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private int h1 = StringHelper.GOOD_FAST_HASH_SEED;
    private int totalLength;

    /**
     * Mix a full page of bytes. The page length must be a multiple of 4.
     */
    void fullPage(byte[] page) {
        assert page.length % 4 == 0 : "only valid for pages with length a multiple of 4";
        for (int i = 0; i < page.length; i += 4) {
            int k1 = (page[i] & 0xff) | ((page[i + 1] & 0xff) << 8) | ((page[i + 2] & 0xff) << 16) | ((page[i + 3] & 0xff) << 24);
            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
        totalLength += page.length;
    }

    /**
     * Mix the final page (of any length ≥ 0) and return the finished hash.
     */
    int lastPage(byte[] page, int length) {
        totalLength += length;
        int roundedEnd = length & ~3;

        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (page[i] & 0xff) | ((page[i + 1] & 0xff) << 8) | ((page[i + 2] & 0xff) << 16) | ((page[i + 3] & 0xff) << 24);
            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int tailLen = length & 3;
        if (tailLen > 0) {
            int k1 = switch (tailLen) {
                case 3 -> ((page[roundedEnd + 2] & 0xff) << 16) | ((page[roundedEnd + 1] & 0xff) << 8) | (page[roundedEnd] & 0xff);
                case 2 -> ((page[roundedEnd + 1] & 0xff) << 8) | (page[roundedEnd] & 0xff);
                default -> page[roundedEnd] & 0xff;
            };
            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;
            h1 ^= k1;
        }

        h1 ^= totalLength;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }
}
