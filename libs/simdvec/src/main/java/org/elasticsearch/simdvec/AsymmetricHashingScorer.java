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
 * Scalar implementation of asymmetric hashing scoring.
 * <p>
 * Computes approximate dot products against encoded database vectors using the asymmetric
 * scheme: the query is projected (not quantized) while database vectors are stored in
 * quantized form.
 * <p>
 * Per-vector scoring formula (Eq. 18 from the ASH paper):
 * <pre>
 *   queryTransformed = query @ W    (raw projection, NOT centered)
 *   dotProduct = queryTransformed . code * scale + query . centroid + offset
 * </pre>
 * The centroid contribution is accounted for in the stored offset via the cross-term
 * (see Eq. 19), so the scorer only needs the raw projected query.
 */
public final class AsymmetricHashingScorer {

    private AsymmetricHashingScorer() {}

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
        byte[] packed = new byte[bitsPerDim * planeBytes];
        int numLevels = 1 << bitsPerDim;
        float offset = (numLevels - 1) / 2.0f;
        for (int j = 0; j < nDims; j++) {
            int unsigned = Math.round(codes[j] + offset);
            if (unsigned < 0) unsigned = 0;
            if (unsigned >= numLevels) unsigned = numLevels - 1;
            int byteIdx = j >>> 3;
            int bitIdx = 7 - (j & 7); // MSB-first
            for (int p = 0; p < bitsPerDim; p++) {
                if ((unsigned & (1 << p)) != 0) {
                    packed[p * planeBytes + byteIdx] |= (byte) (1 << bitIdx);
                }
            }
        }
        return packed;
    }

    /**
     * Scores a single database vector from its packed bit-plane representation against a
     * precomputed transformed query. This is the inner-loop method for posting list scoring.
     * <p>
     * The packed format is bitsPerDim bit-planes, each ceil(nDims/8) bytes, MSB-first.
     * Codes represent centered levels. The unsigned code value is reconstructed from bit planes,
     * then shifted back to centered by subtracting (numLevels-1)/2.
     * dot = sum_j queryTransformed[j] * centeredCode[j]
     *     = sum over planes of (2^p * sum_of_qt_where_bit_p_set) - centerOffset * sumAll
     *
     * @param queryTransformed precomputed query @ W (raw projection, not centered)
     * @param queryDotCentroid precomputed query . centroid for this cluster
     * @param packedCodes bit-plane packed codes
     * @param nDims number of projected dimensions
     * @param bitsPerDim bits per dimension
     * @param scale the scale factor for this vector
     * @param offset the offset correction for this vector (includes cross-term per Eq. 19)
     * @return approximate dot product
     */
    public static float score(
        float[] queryTransformed,
        float queryDotCentroid,
        byte[] packedCodes,
        int nDims,
        int bitsPerDim,
        float scale,
        float offset
    ) {
        int planeBytes = (nDims + 7) >>> 3;
        int numLevels = 1 << bitsPerDim;
        double centerOffset = (numLevels - 1) / 2.0;

        double sumAll = 0;
        double[] planeSums = new double[bitsPerDim];

        for (int j = 0; j < nDims; j++) {
            float qt = queryTransformed[j];
            sumAll += qt;
            int byteIdx = j >>> 3;
            int bitIdx = 7 - (j & 7);
            for (int p = 0; p < bitsPerDim; p++) {
                if ((packedCodes[p * planeBytes + byteIdx] & (1 << bitIdx)) != 0) {
                    planeSums[p] += qt;
                }
            }
        }

        // unsigned dot = sum_p (2^p * planeSums[p]); centered dot = unsigned dot - centerOffset * sumAll
        double dot = -centerOffset * sumAll;
        for (int p = 0; p < bitsPerDim; p++) {
            dot += (1 << p) * planeSums[p];
        }
        return (float) dot * scale + queryDotCentroid + offset;
    }
}
