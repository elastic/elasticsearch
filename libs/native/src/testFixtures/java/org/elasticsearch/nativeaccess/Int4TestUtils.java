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
 * Shared test utilities for Int4 packed-nibble vector operations.
 *
 * <p>Int4 vectors use two representations:
 * <ul>
 *   <li><b>Unpacked</b>: {@code 2 * packedLen} bytes. Indices {@code [0..packedLen)} hold the
 *       high-nibble values; indices {@code [packedLen..2*packedLen)} hold the low-nibble values.
 *       Each byte stores a single 4-bit value (range 0-15).</li>
 *   <li><b>Packed</b>: {@code packedLen} bytes. Each byte holds two 4-bit values: high nibble
 *       in bits 7-4, low nibble in bits 3-0.</li>
 * </ul>
 * <p>
 * The unpacked input comes from {@code OptimizedScalarQuantizer#scalarQuantize}, which quantizes a float
 * vector into one byte per element in natural order: unpacked = [v0, v1, v2, ..., v_{N-1}] where N = dims.
 * <p>
 * The packed format pairs elements that are packedLength ({@param unpacked} length / 2) apart. For example,
 * with dims=8, unpacked.length is 8, and packedLength is 4:
 *   - {@code packed[0] = (v0 << 4) | v4}
 *   - {@code packed[1] = (v1 << 4) | v5}
 *   - {@code packed[2] = (v2 << 4) | v6}
 *   - {@code packed[3] = (v3 << 4) | v7}
 * <p>
 * Or, visually,
 * UNPACKED (8 bytes, natural vector order, one 4-bit value per byte):
 *   index:   0     1     2     3     4     5     6     7
 *          [v0]  [v1]  [v2]  [v3]  [v4]  [v5]  [v6]  [v7]
 *   PACKED (4 bytes, on disk, two 4-bit values per byte):
 *   index:      0          1          2          3
 *          [v0  | v4]  [v1 | v5]  [v2 | v6]  [v3 | v7]
 *           hi    lo    hi   lo    hi   lo    hi   lo
 *          7..4  3..0  7..4 3..0  7..4 3..0  7..4 3..0
 */
public final class Int4TestUtils {

    private Int4TestUtils() {}

    /**
     * Packs unpacked int4 values (one value per byte) into Lucene nibble-packed format (two values per byte)
     * written by {@code Lucene104ScalarQuantizedVectorsWriter} (ScalarEncoding#PACKED_NIBBLE format).
     * The input layout is {@code [high0, high1, ..., low0, low1, ...]} with length {@code 2 * packedLen}.
     */
    public static byte[] packNibbles(byte[] unpacked) {
        int packedLength = unpacked.length / 2;
        byte[] packed = new byte[packedLength];
        for (int i = 0; i < packedLength; i++) {
            packed[i] = (byte) ((unpacked[i] << 4) | (unpacked[i + packedLength] & 0x0F));
        }
        return packed;
    }

    /**
     * Unpacks "nibble-packed" int4 values (two values per byte) into the unpacked int4 format (byte[], one value per byte).
     * @param packed the packed bytes (each holding two 4-bit values)
     * @param dims the total number of 4-bit elements ({@code 2 * packed.length})
     */
    public static byte[] unpackNibbles(byte[] packed, int dims) {
        byte[] unpacked = new byte[dims];
        int packedLen = packed.length;
        for (int i = 0; i < packedLen; i++) {
            unpacked[i] = (byte) ((packed[i] & 0xFF) >>> 4);
            unpacked[i + packedLen] = (byte) (packed[i] & 0x0F);
        }
        return unpacked;
    }

    /**
     * Computes the dot product between an unpacked query vector and a packed document vector,
     * matching the native {@code doti4_inner} implementation.
     */
    public static int dotProductI4SinglePacked(byte[] unpacked, byte[] packed) {
        int total = 0;
        for (int i = 0; i < packed.length; i++) {
            byte packedByte = packed[i];
            total += ((packedByte & 0xFF) >> 4) * unpacked[i];
            total += (packedByte & 0x0F) * unpacked[i + packed.length];
        }
        return total;
    }
}
