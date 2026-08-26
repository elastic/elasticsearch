/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

/**
 * Utilities for packing ASH (Asymmetric Scalar Hashing) quantized codes into
 * bit-plane format for on-disk storage.
 */
public final class AshPackingUtils {

    private AshPackingUtils() {}

    /**
     * Returns the number of bytes needed to store nDims dimensions at the given bits per dimension.
     * For bitsPerDim=2: 2*ceil(nDims/8) (low + high bit planes).
     *
     * @param nDims number of projected dimensions
     * @param bitsPerDim bits per dimension
     * @return number of bytes needed
     */
    public static int packedLength(int nDims, int bitsPerDim) {
        return bitsPerDim * ((nDims + 7) >>> 3);
    }

    /**
     * Packs multi-bit quantized codes into a byte array using bit-plane layout.
     * The input codes come from {@code AshSphericalScalarQuantizer} and have values
     * sign * (0.5 + idx) for idx in [0, numAbsLevels-1] where numAbsLevels = 2^(bitsPerDim-1).
     * The full level set is centered at 0 with spacing 1.
     * <p>
     * We map to unsigned levels [0, 2^bitsPerDim - 1] by adding (numLevels-1)/2.0 and rounding,
     * then split into bit planes (LSB first), each packed MSB-first.
     * Layout: [plane0: ceil(nDims/8) bytes][plane1: ceil(nDims/8) bytes]...[plane_{b-1}: ...]
     *
     * @param codes float array of quantized levels from AshSphericalScalarQuantizer
     * @param bitsPerDim number of bits per dimension
     * @return packed bytes, length bitsPerDim * ceil(nDims/8)
     */
    public static byte[] pack(float[] codes, int bitsPerDim) {
        int nDims = codes.length;
        int planeBytes = (nDims + 7) >>> 3;
        int numLevels = 1 << bitsPerDim;
        float offset = (numLevels - 1) / 2.0f;

        int[] rounded = new int[nDims];
        for (int i = 0; i < nDims; i++) {
            rounded[i] = Math.clamp(Math.round(codes[i] + offset), 0, numLevels - 1);
        }

        byte[] packed = new byte[bitsPerDim * planeBytes];
        switch (bitsPerDim) {
            case 1 -> ESVectorUtil.pack1BitValues(rounded, packed);
            case 2 -> ESVectorUtil.stride2BitValues(rounded, packed);
            case 4 -> ESVectorUtil.stride4BitValues(rounded, packed);
            case 3, 8 -> {
                // TODO: optimized implementations
                for (int j = 0; j < nDims; j++) {
                    int byteIdx = j >>> 3;
                    int bitIdx = 7 - (j & 7); // MSB-first
                    for (int p = 0; p < bitsPerDim; p++) {
                        if ((rounded[j] & (1 << p)) != 0) {
                            packed[p * planeBytes + byteIdx] |= (byte) (1 << bitIdx);
                        }
                    }
                }
            }
            default -> throw new IllegalArgumentException("Unsupported bitsPerDim: " + bitsPerDim);
        }

        return packed;
    }
}
