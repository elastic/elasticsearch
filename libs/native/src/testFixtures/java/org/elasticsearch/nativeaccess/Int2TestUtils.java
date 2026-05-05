/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

/**
 * Shared test utilities for Int2 packed-quad vector operations.
 *
 * <p>Int2 vectors use two representations:
 * <ul>
 *   <li><b>Unpacked</b>: {@code 4 * packedLen} bytes. Each byte stores a single 2-bit value
 *       (range 0-3). Indices are split into four equal stripes:
 *       <ul>
 *         <li>{@code [0..packedLen)}            — stripe 0 (lands in bits 7:6 of packed bytes)</li>
 *         <li>{@code [packedLen..2*packedLen)}  — stripe 1 (bits 5:4)</li>
 *         <li>{@code [2*packedLen..3*packedLen)} — stripe 2 (bits 3:2)</li>
 *         <li>{@code [3*packedLen..4*packedLen)} — stripe 3 (bits 1:0)</li>
 *       </ul>
 *   </li>
 *   <li><b>Packed</b>: {@code packedLen} bytes. Each byte holds four 2-bit values:
 *       stripe 0 in bits 7:6, stripe 1 in bits 5:4, stripe 2 in bits 3:2, stripe 3 in bits 1:0.</li>
 * </ul>
 * <p>
 * The packed format pairs elements that are {@code packedLen} apart. For example, with dims=8,
 * unpacked.length is 8, and packedLen is 2:
 *   - {@code packed[0] = (v0 << 6) | (v2 << 4) | (v4 << 2) | v6}
 *   - {@code packed[1] = (v1 << 6) | (v3 << 4) | (v5 << 2) | v7}
 * <p>
 * Or, visually,
 * UNPACKED (8 bytes, one 2-bit value per byte):
 *   index:   0     1     2     3     4     5     6     7
 *          [v0]  [v1]  [v2]  [v3]  [v4]  [v5]  [v6]  [v7]
 *           \stripe 0/  \stripe 1/  \stripe 2/  \stripe 3/
 *   PACKED (2 bytes, four 2-bit values per byte):
 *   index:           0                       1
 *          [v0 | v2 | v4 | v6]   [v1 | v3 | v5 | v7]
 *           7:6  5:4  3:2  1:0    7:6  5:4  3:2  1:0
 */
public final class Int2TestUtils {

    private Int2TestUtils() {}

    /**
     * Packs unpacked int2 values (one value per byte) into the four-stripe packed format
     * (four values per byte).
     * The input layout is {@code [stripe0, stripe1, stripe2, stripe3]} with length {@code 4 * packedLen}.
     */
    public static byte[] packQuads(byte[] unpacked) {
        int packedLength = unpacked.length / 4;
        byte[] packed = new byte[packedLength];
        for (int i = 0; i < packedLength; i++) {
            int s0 = unpacked[i] & 0x03;
            int s1 = unpacked[i + packedLength] & 0x03;
            int s2 = unpacked[i + 2 * packedLength] & 0x03;
            int s3 = unpacked[i + 3 * packedLength] & 0x03;
            packed[i] = (byte) ((s0 << 6) | (s1 << 4) | (s2 << 2) | s3);
        }
        return packed;
    }

    /**
     * Unpacks four-stripe packed int2 values into the unpacked format (one value per byte).
     * @param packed the packed bytes (each holding four 2-bit values)
     * @param dims the total number of 2-bit elements ({@code 4 * packed.length})
     */
    public static byte[] unpackQuads(byte[] packed, int dims) {
        byte[] unpacked = new byte[dims];
        int packedLen = packed.length;
        for (int i = 0; i < packedLen; i++) {
            int b = packed[i] & 0xFF;
            unpacked[i] = (byte) ((b >>> 6) & 0x03);
            unpacked[i + packedLen] = (byte) ((b >>> 4) & 0x03);
            unpacked[i + 2 * packedLen] = (byte) ((b >>> 2) & 0x03);
            unpacked[i + 3 * packedLen] = (byte) (b & 0x03);
        }
        return unpacked;
    }

    /**
     * Computes the dot product between an unpacked query vector and a packed document vector,
     * matching the native {@code doti2_inner} implementation.
     */
    public static int dotProductI2SinglePacked(byte[] unpacked, byte[] packed) {
        int total = 0;
        int packedLen = packed.length;
        for (int i = 0; i < packedLen; i++) {
            int b = packed[i] & 0xFF;
            total += ((b >>> 6) & 0x03) * unpacked[i];
            total += ((b >>> 4) & 0x03) * unpacked[i + packedLen];
            total += ((b >>> 2) & 0x03) * unpacked[i + 2 * packedLen];
            total += (b & 0x03) * unpacked[i + 3 * packedLen];
        }
        return total;
    }
}
