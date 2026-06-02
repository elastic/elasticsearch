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
 * Shared test utilities for BBQ vector operations in the {@code STRIPED} (bit-plane) layout —
 * the packing convention consumed by {@code vec_dotdNqM} kernels and produced on disk by the
 * existing OSQ writers via {@code ESVectorUtil.transposeHalfByte}, {@code packDibit}, and
 * {@code packAsBinary}.
 *
 * <p>BBQ striped vectors use two representations:
 * <ul>
 *   <li><b>Unpacked</b>: {@code dims} bytes. One value per byte, range {@code [0, 2^elementBits - 1]}.</li>
 *   <li><b>Packed (striped)</b>: {@code elementBits * dims / 8} bytes — {@code elementBits}
 *       contiguous bit-planes of {@code dims/8} bytes each. Plane {@code j} carries bit {@code j}
 *       (LSB-first) of every unpacked value, packed 8-per-byte with the bit at index {@code i}
 *       landing in byte {@code i/8}, bit position {@code 7 - (i % 8)} (MSB-first within byte).</li>
 * </ul>
 * Plane {@code 0} is at offset {@code 0} of the packed buffer; plane {@code j} is at offset
 * {@code j * dims/8}. This is the layout the {@code dotd1q4_inner} kernel reads from each query
 * stripe and from each consecutive doc plane.
 */
public final class BBQTestUtils {

    private BBQTestUtils() {}

    /**
     * Packs unpacked BBQ values (one value per byte) into the bit-plane striped format.
     * Output length is {@code elementBits * unpacked.length / 8} bytes.
     *
     * @param unpacked one value per byte, range {@code [0, 2^elementBits - 1]}; length must be a multiple of 8
     * @param elementBits number of bits per value (e.g. 1, 2, 4, 7)
     */
    public static byte[] packStriped(byte[] unpacked, int elementBits) {
        assert unpacked.length % 8 == 0 : "unpacked length must be a multiple of 8: " + unpacked.length;
        int planeBytes = unpacked.length / 8;
        byte[] packed = new byte[elementBits * planeBytes];
        for (int i = 0; i < unpacked.length; i++) {
            byte value = unpacked[i];
            int byteIdx = i / 8;
            int bitPos = 7 - (i % 8);
            for (int j = 0; j < elementBits; j++) {
                int v = value & 0x1;
                packed[byteIdx + j * planeBytes] |= (byte) (v << bitPos);
                value >>= 1;
            }
        }
        return packed;
    }

    /**
     * Inverse of {@link #packStriped}. Unpacks a bit-plane striped buffer into one value per byte.
     *
     * @param packed bit-plane striped buffer, length {@code elementBits * dims / 8}
     * @param dims number of unpacked values
     * @param elementBits number of bits per value
     */
    public static byte[] unpackStriped(byte[] packed, int dims, int elementBits) {
        assert dims % 8 == 0 : "dims must be a multiple of 8: " + dims;
        assert packed.length == elementBits * dims / 8 : "packed length " + packed.length + " != elementBits * dims / 8";
        byte[] unpacked = new byte[dims];
        int planeBytes = dims / 8;
        for (int i = 0; i < dims; i++) {
            int byteIdx = i / 8;
            int bitPos = 7 - (i % 8);
            byte value = 0;
            for (int j = 0; j < elementBits; j++) {
                int bit = (packed[byteIdx + j * planeBytes] >>> bitPos) & 0x1;
                value |= (byte) (bit << j);
            }
            unpacked[i] = value;
        }
        return unpacked;
    }

    /** Bytes required to hold {@code dimensions} values quantized to {@code bits} bits each, packed striped. */
    public static int numBytes(int dimensions, int bits) {
        assert dimensions % 8 == 0 : "dimensions must be a multiple of 8: " + dimensions;
        return dimensions / (8 / bits);
    }

    /**
     * Packs unpacked 2-bit values (one value per byte, range 0..3) into the packed-quad
     * layout: each output byte holds four 2-bit values
     * {@code [v0:7..6 | v1:5..4 | v2:3..2 | v3:1..0]}, where the four values pulled into
     * a single byte are sourced {@code packedLen = unpacked.length / 4} apart in the input.
     *
     * <p>Example with {@code unpacked.length = 8}, {@code packedLen = 2}:
     * <pre>
     *   packed[0] = (v0 &lt;&lt; 6) | (v2 &lt;&lt; 4) | (v4 &lt;&lt; 2) | v6
     *   packed[1] = (v1 &lt;&lt; 6) | (v3 &lt;&lt; 4) | (v5 &lt;&lt; 2) | v7
     * </pre>
     */
    public static byte[] packQuads(byte[] unpacked) {
        assert unpacked.length % 4 == 0 : "unpacked length must be a multiple of 4: " + unpacked.length;
        int packedLen = unpacked.length / 4;
        byte[] packed = new byte[packedLen];
        for (int i = 0; i < packedLen; i++) {
            int s0 = unpacked[i] & 0x03;
            int s1 = unpacked[i + packedLen] & 0x03;
            int s2 = unpacked[i + 2 * packedLen] & 0x03;
            int s3 = unpacked[i + 3 * packedLen] & 0x03;
            packed[i] = (byte) ((s0 << 6) | (s1 << 4) | (s2 << 2) | s3);
        }
        return packed;
    }

    /**
     * Inverse of {@link #packQuads}.
     *
     * @param packed packed-quad buffer (each byte holds four 2-bit values)
     * @param dims total number of 2-bit elements ({@code 4 * packed.length})
     */
    public static byte[] unpackQuads(byte[] packed, int dims) {
        assert dims == 4 * packed.length : "dims " + dims + " != 4 * packed.length " + packed.length;
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
}
