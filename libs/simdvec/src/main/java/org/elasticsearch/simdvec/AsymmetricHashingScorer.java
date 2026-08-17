/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.util.BitUtil;

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
     * @param queryConstants per-cluster query constants: [queryDotCentroid, ...]
     * @param packedCodes byte buffer containing packed codes (may contain multiple vectors)
     * @param codeOffset starting byte offset for this vector's codes within the buffer
     * @param nDims number of projected dimensions
     * @param bitsPerDim bits per dimension
     * @param corrections per-vector corrections buffer (AoS layout: [scale, offset, docSum] per vector)
     * @param correctionOffset byte offset into corrections for this vector
     * @return approximate dot product
     */
    public static float score(
        float[] queryTransformed,
        float[] queryConstants,
        byte[] packedCodes,
        int codeOffset,
        int nDims,
        int bitsPerDim,
        byte[] corrections,
        int correctionOffset
    ) {
        float scale = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corrections, correctionOffset + CORR_SCALE));
        float offset = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corrections, correctionOffset + CORR_OFFSET));

        int planeBytes = (nDims + 7) >>> 3;
        int numLevels = 1 << bitsPerDim;
        double centerOffset = (numLevels - 1) / 2.0;

        double sumAll = 0;
        double[] planeSums = new double[bitsPerDim];

        for (int j = 0; j < nDims; j++) {
            float qt = queryTransformed[j];
            sumAll += qt;
            // TODO: this is a more general form of ESVectorUtil.ipFloatBit
            int byteIdx = j >>> 3;
            int bitIdx = 7 - (j & 7);
            for (int p = 0; p < bitsPerDim; p++) {
                if ((packedCodes[codeOffset + p * planeBytes + byteIdx] & (1 << bitIdx)) != 0) {
                    planeSums[p] += qt;
                }
            }
        }

        double dot = -centerOffset * sumAll;
        for (int p = 0; p < bitsPerDim; p++) {
            dot = Math.fma(1 << p, planeSums[p], dot);
        }
        return (float) dot * scale + queryConstants[QC_QUERY_DOT_CENTROID] + offset;
    }

    // --- queryConstants indices for scoreInteger ---
    /** Index of queryDotCentroid in queryConstants array. */
    public static final int QC_QUERY_DOT_CENTROID = 0;
    /** Index of invQScale in queryConstants array. */
    public static final int QC_INV_Q_SCALE = 1;
    /** Index of qOffset in queryConstants array. */
    public static final int QC_Q_OFFSET = 2;
    /** Index of constantCorrection in queryConstants array. */
    public static final int QC_CONSTANT_CORRECTION = 3;
    /** Length of the queryConstants array. */
    public static final int QC_LENGTH = 4;

    // --- Per-vector correction layout (AoS: all fields interleaved per vector) ---
    /** Byte offset of scale (float32) within a correction entry. */
    public static final int CORR_SCALE = 0;
    /** Byte offset of offset (float32) within a correction entry. */
    public static final int CORR_OFFSET = Float.BYTES;
    /** Byte offset of docSum (int32) within a correction entry. */
    public static final int CORR_DOC_SUM = 2 * Float.BYTES;
    /** Byte offset of ⟨μ*,x⟩ (float32) within a correction entry (EUCLIDEAN; 0 otherwise). */
    public static final int CORR_VEC_CENTROID_DOT = 3 * Float.BYTES;
    /** Byte offset of ‖x-μ*‖² (float32) within a correction entry (EUCLIDEAN; 0 otherwise). */
    public static final int CORR_VEC_CENTROID_SQ_DIST = 4 * Float.BYTES;
    /** Total bytes per correction entry. */
    public static final int CORRECTION_BYTES = 5 * Float.BYTES;

    /**
     * Scores a single database vector using integer arithmetic with a quantized query.
     * The query is quantized to {@code queryBitsPerDim} bits and scoring uses AND+popcount
     * between query and document bit planes, with per-vector correction via stored docSum.
     * <p>
     * This generalizes the D2Q4 (document 2-bit, query 4-bit) pattern to arbitrary bit widths.
     * <p>
     * Derivation: the float scorer computes {@code dot(qt_float, centeredCode) * scale + qdc + offset}.
     * We approximate {@code qt_float[j] ≈ invQScale * qt_quantized[j] + qOffset}, so:
     * <pre>
     *   dot(qt_float, centeredCode)
     *     = dot(qt_float, unsignedCode) - centerOffset * sum(qt_float)
     *     ≈ invQScale * dot(qt_q, unsignedCode) + qOffset * sum(unsignedCode)
     *       - centerOffset * (invQScale * sum(qt_q) + qOffset * nDims)
     *     = invQScale * rawDot + qOffset * docSum - constantCorrection
     * </pre>
     * where {@code rawDot = dot(qt_q, unsignedCode)} via AND+popcount, {@code docSum = sum(unsignedCode)}
     * is precomputed at index time, and {@code constantCorrection} is precomputed per query.
     *
     * @param queryQuantized quantized query in bit-plane format (queryBitsPerDim × planeBytes)
     * @param queryBitsPerDim bits per dimension for the quantized query
     * @param queryConstants per-cluster query constants: [queryDotCentroid, invQScale, qOffset, constantCorrection]
     * @param packedCodes byte buffer containing packed document codes
     * @param codeOffset starting byte offset for this vector's codes within the buffer
     * @param bitsPerDim bits per dimension for document codes
     * @param planeBytes bytes per single bit-plane (ceil(nDims/8))
     * @param corrections per-vector corrections buffer (AoS layout: [scale, offset, docSum] per vector)
     * @param correctionOffset byte offset into corrections for this vector
     * @return approximate dot product
     */
    public static float scoreInteger(
        byte[] queryQuantized,
        int queryBitsPerDim,
        float[] queryConstants,
        byte[] packedCodes,
        int codeOffset,
        int bitsPerDim,
        int planeBytes,
        byte[] corrections,
        int correctionOffset
    ) {
        float invQScale = queryConstants[QC_INV_Q_SCALE];
        float qOffset = queryConstants[QC_Q_OFFSET];
        float constantCorrection = queryConstants[QC_CONSTANT_CORRECTION];
        float scale = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corrections, correctionOffset + CORR_SCALE));
        float offset = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(corrections, correctionOffset + CORR_OFFSET));
        float docSum = (int) BitUtil.VH_LE_INT.get(corrections, correctionOffset + CORR_DOC_SUM);

        int rawDot = 0;
        for (int qp = 0; qp < queryBitsPerDim; qp++) {
            for (int dp = 0; dp < bitsPerDim; dp++) {
                int weight = (1 << qp) * (1 << dp);
                int pc = 0;
                for (int b = 0; b < planeBytes; b++) {
                    pc += Integer.bitCount((queryQuantized[qp * planeBytes + b] & packedCodes[codeOffset + dp * planeBytes + b]) & 0xFF);
                }
                rawDot += weight * pc;
            }
        }
        float floatDot = Math.fma(invQScale, rawDot, Math.fma(qOffset, docSum, -constantCorrection));
        return Math.fma(floatDot, scale, queryConstants[QC_QUERY_DOT_CENTROID] + offset);
    }
}
