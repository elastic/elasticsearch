/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

/**
 * Scalar implementation of asymmetric hashing scoring.
 * <p>
 * For each query, computes approximate dot products against all encoded database vectors
 * using the asymmetric scheme: the query is projected (not quantized) while database
 * vectors are stored in quantized form.
 * <p>
 * Per-cluster scoring formula:
 * <pre>
 *   queryTransformed = (query - centroid_k) @ W
 *   dotProduct[i] = queryTransformed . encodedVector[i] * scale[i] + query . centroid_k + offset[i]
 * </pre>
 */
public final class AsymmetricHashingScorer {

    private AsymmetricHashingScorer() {}

    /**
     * Computes approximate dot products between a single query and all encoded database vectors.
     *
     * @param query the query vector, length originalDim
     * @param w projection matrix, shape (originalDim, nDims)
     * @param centroids cluster centroids, shape (nClusters, originalDim)
     * @param clusterAssignments cluster assignment per database vector
     * @param encodedVectors quantized codes, shape (nVectors, nDims)
     * @param scales per-vector scale factors
     * @param offsets per-vector offset corrections
     * @return approximate dot products, length nVectors
     */
    public static float[] score(
        float[] query,
        float[][] w,
        float[][] centroids,
        int[] clusterAssignments,
        float[][] encodedVectors,
        float[] scales,
        float[] offsets
    ) {
        int nVectors = encodedVectors.length;
        int originalDim = query.length;
        int nDims = w[0].length;
        int nClusters = centroids.length;

        // Precompute query dot centroid and transformed query per cluster
        float[] queryDotCentroid = new float[nClusters];
        float[][] queryTransformed = new float[nClusters][nDims];

        for (int k = 0; k < nClusters; k++) {
            float[] centroid = centroids[k];
            double dotQC = 0;
            for (int d = 0; d < originalDim; d++) {
                dotQC += (double) query[d] * centroid[d];
            }
            queryDotCentroid[k] = (float) dotQC;

            // queryTransformed[k] = (query - centroid_k) @ W
            for (int j = 0; j < nDims; j++) {
                double sum = 0;
                for (int d = 0; d < originalDim; d++) {
                    sum += (double) (query[d] - centroid[d]) * w[d][j];
                }
                queryTransformed[k][j] = (float) sum;
            }
        }

        // Score each database vector
        float[] results = new float[nVectors];
        for (int i = 0; i < nVectors; i++) {
            int k = clusterAssignments[i];
            float[] qt = queryTransformed[k];
            float[] enc = encodedVectors[i];

            // dot product in latent space
            double dot = 0;
            for (int j = 0; j < nDims; j++) {
                dot += (double) qt[j] * enc[j];
            }

            results[i] = (float) dot * scales[i] + queryDotCentroid[k] + offsets[i];
        }

        return results;
    }

    /**
     * Scores a single database vector against a precomputed transformed query for a specific cluster.
     * This is the inner-loop method for integration with the posting list reader.
     *
     * @param queryTransformedForCluster precomputed (query - centroid) @ W for this cluster
     * @param queryDotCentroid precomputed query . centroid for this cluster
     * @param encodedVector the quantized code for the database vector
     * @param scale the scale factor for this vector
     * @param offset the offset correction for this vector
     * @return approximate dot product
     */
    public static float scoreOneVector(
        float[] queryTransformedForCluster,
        float queryDotCentroid,
        float[] encodedVector,
        float scale,
        float offset
    ) {
        int nDims = queryTransformedForCluster.length;
        double dot = 0;
        for (int j = 0; j < nDims; j++) {
            dot += (double) queryTransformedForCluster[j] * encodedVector[j];
        }
        return (float) dot * scale + queryDotCentroid + offset;
    }

    /**
     * Scores a single binary (1-bit) encoded database vector from packed byte representation.
     * Each bit in {@code packedCodes} represents a sign: 1 = +1, 0 = -1.
     * Bits are packed MSB-first within each byte.
     *
     * @param queryTransformedForCluster precomputed (query - centroid) @ W for this cluster
     * @param queryDotCentroid precomputed query . centroid for this cluster
     * @param packedCodes bit-packed codes, length ceil(nDims/8)
     * @param nDims number of projected dimensions
     * @param scale the scale factor for this vector
     * @param offset the offset correction for this vector
     * @return approximate dot product
     */
    public static float scoreOneVectorBinary(
        float[] queryTransformedForCluster,
        float queryDotCentroid,
        byte[] packedCodes,
        int nDims,
        float scale,
        float offset
    ) {
        // dot = sum_j queryTransformed[j] * sign[j]
        // where sign[j] = +1 if bit set, -1 if not
        // = sum_j queryTransformed[j] * (2*bit[j] - 1)
        // = 2 * sum_positive(queryTransformed[j]) - sum_all(queryTransformed[j])
        double sumAll = 0;
        double sumPositive = 0;
        for (int j = 0; j < nDims; j++) {
            float qt = queryTransformedForCluster[j];
            sumAll += qt;
            int byteIdx = j >>> 3;
            int bitIdx = 7 - (j & 7); // MSB-first
            if ((packedCodes[byteIdx] & (1 << bitIdx)) != 0) {
                sumPositive += qt;
            }
        }
        double dot = 2.0 * sumPositive - sumAll;
        return (float) dot * scale + queryDotCentroid + offset;
    }

    /**
     * Packs binary (1-bit sign) codes into bytes, MSB-first.
     * Input codes are expected to be +1 or -1.
     *
     * @param codes float array of {+1, -1} values, length nDims
     * @return packed bytes, length ceil(nDims/8)
     */
    public static byte[] packBinaryCodes(float[] codes) {
        int nDims = codes.length;
        int nBytes = (nDims + 7) >>> 3;
        byte[] packed = new byte[nBytes];
        for (int j = 0; j < nDims; j++) {
            if (codes[j] > 0) {
                int byteIdx = j >>> 3;
                int bitIdx = 7 - (j & 7);
                packed[byteIdx] |= (byte) (1 << bitIdx);
            }
        }
        return packed;
    }

    /**
     * Returns the number of bytes needed to store nDims binary-quantized dimensions.
     *
     * @param nDims number of projected dimensions
     * @return number of bytes needed
     */
    public static int packedByteLength(int nDims) {
        return (nDims + 7) >>> 3;
    }

    /**
     * Returns the number of bytes needed to store nDims dimensions at the given bits per dimension.
     * For bitsPerDim=1: ceil(nDims/8). For bitsPerDim=2: 2*ceil(nDims/8) (low + high bit planes).
     *
     * @param nDims number of projected dimensions
     * @param bitsPerDim bits per dimension
     * @return number of bytes needed
     */
    public static int packedByteLength(int nDims, int bitsPerDim) {
        return bitsPerDim * ((nDims + 7) >>> 3);
    }

    /**
     * Packs multi-bit quantized codes into a byte array using bit-plane layout.
     * The input codes come from {@link AshSphericalScalarQuantizer} and have values
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
    public static byte[] packMultiBitCodes(float[] codes, int bitsPerDim) {
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
     * Scores a single multi-bit encoded database vector from packed bit-plane representation.
     * The packed format is bitsPerDim bit-planes, each ceil(nDims/8) bytes, MSB-first.
     * <p>
     * Codes represent centered levels. The unsigned code value is reconstructed from bit planes,
     * then shifted back to centered by subtracting (numLevels-1)/2.
     * dot = sum_j queryTransformed[j] * centeredCode[j]
     *     = sum over planes of (2^p * sum_of_qt_where_bit_p_set) - centerOffset * sumAll
     *
     * @param queryTransformedForCluster precomputed (query - centroid) @ W for this cluster
     * @param queryDotCentroid precomputed query . centroid for this cluster
     * @param packedCodes bit-plane packed codes
     * @param nDims number of projected dimensions
     * @param bitsPerDim bits per dimension
     * @param scale the scale factor for this vector
     * @param offset the offset correction for this vector
     * @return approximate dot product
     */
    public static float scoreOneVectorMultiBit(
        float[] queryTransformedForCluster,
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
            float qt = queryTransformedForCluster[j];
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
