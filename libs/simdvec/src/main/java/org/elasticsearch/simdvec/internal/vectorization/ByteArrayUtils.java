/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import static org.apache.lucene.util.BitUtil.VH_LE_LONG;

/** Byte array utilities. */
final class ByteArrayUtils {

    /**
     * Implementation of {@link ESVectorUtilSupport#indexOf(byte[], int, int, byte)} using fast
     * SWAR (SIMD Within A Register) loop.
     */
    static int indexOf(final byte[] bytes, final int offset, final int len, final byte marker) {
        final int end = offset + len;
        int i = offset;

        // First, try to find the marker in the first few bytes, so we can enter the faster 8-byte aligned loop below.
        // The idea for this logic is taken from Netty's io.netty.buffer.ByteBufUtil.firstIndexOf and optimized for little endian hardware.
        // See e.g. https://richardstartin.github.io/posts/finding-bytes for the idea behind this optimization.
        final int byteCount = len & 7;
        if (byteCount > 0) {
            final int index = unrolledFirstIndexOf(bytes, i, byteCount, marker);
            if (index != -1) {
                return index - offset;
            }
            i += byteCount;
            if (i == end) {
                return -1;
            }
        }
        final int longCount = len >>> 3;
        // faster SWAR (SIMD Within A Register) loop
        final long pattern = compilePattern(marker);
        for (int j = 0; j < longCount; j++) {
            int index = findInLong(readLongLE(bytes, i), pattern);
            if (index < Long.BYTES) {
                return i + index - offset;
            }
            i += Long.BYTES;
        }
        return -1;
    }

    private static long readLongLE(byte[] arr, int offset) {
        return (long) VH_LE_LONG.get(arr, offset);
    }

    private static long compilePattern(byte byteToFind) {
        return (byteToFind & 0xFFL) * 0x101010101010101L;
    }

    private static int findInLong(long word, long pattern) {
        long input = word ^ pattern;
        long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        tmp = ~(tmp | input | 0x7F7F7F7F7F7F7F7FL);
        final int binaryPosition = Long.numberOfTrailingZeros(tmp);
        return binaryPosition >>> 3;
    }

    private static int unrolledFirstIndexOf(byte[] buffer, int fromIndex, int byteCount, byte value) {
        if (buffer[fromIndex] == value) {
            return fromIndex;
        }
        if (byteCount == 1) {
            return -1;
        }
        if (buffer[fromIndex + 1] == value) {
            return fromIndex + 1;
        }
        if (byteCount == 2) {
            return -1;
        }
        if (buffer[fromIndex + 2] == value) {
            return fromIndex + 2;
        }
        if (byteCount == 3) {
            return -1;
        }
        if (buffer[fromIndex + 3] == value) {
            return fromIndex + 3;
        }
        if (byteCount == 4) {
            return -1;
        }
        if (buffer[fromIndex + 4] == value) {
            return fromIndex + 4;
        }
        if (byteCount == 5) {
            return -1;
        }
        if (buffer[fromIndex + 5] == value) {
            return fromIndex + 5;
        }
        if (byteCount == 6) {
            return -1;
        }
        if (buffer[fromIndex + 6] == value) {
            return fromIndex + 6;
        }
        return -1;
    }
}
