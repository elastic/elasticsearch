/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.test.ESTestCase;

/**
 * Tests for {@link AsymmetricHashingScorer}.
 */
public class AsymmetricHashingScorerTests extends ESTestCase {

    public void testScoreOneVectorBasic() {
        // queryTransformed = [2, 3], encodedVector = [1, -1]
        // dot = 2*1 + 3*(-1) = -1
        // result = -1 * scale + qdc + offset = -1 * 1.0 + 0.0 + 0.0 = -1.0
        float score = AsymmetricHashingScorer.scoreOneVector(new float[] { 2.0f, 3.0f }, 0.0f, new float[] { 1.0f, -1.0f }, 1.0f, 0.0f);
        assertEquals(-1.0f, score, 1e-6f);
    }

    public void testScaleAndOffsetApplied() {
        // dot = 2*1 + 3*1 = 5
        // result = 5 * 2.0 + 1.5 + 0.3 = 11.8
        float score = AsymmetricHashingScorer.scoreOneVector(new float[] { 2.0f, 3.0f }, 1.5f, new float[] { 1.0f, 1.0f }, 2.0f, 0.3f);
        assertEquals(11.8f, score, 1e-5f);
    }

    public void testScoreOneVectorMultiBitEqualsFloat() {
        // Multi-bit scorer should produce same result as float scorer
        for (int bitsPerDim = 2; bitsPerDim <= 4; bitsPerDim++) {
            int numAbsLevels = 1 << (bitsPerDim - 1);
            for (int iter = 0; iter < 20; iter++) {
                int nDims = randomIntBetween(4, 100);
                // Generate valid multi-bit codes: sign * (0.5 + level) for level in [0, numAbsLevels-1]
                float[] codes = new float[nDims];
                for (int j = 0; j < nDims; j++) {
                    float sign = randomBoolean() ? 1.0f : -1.0f;
                    int level = randomIntBetween(0, numAbsLevels - 1);
                    codes[j] = sign * (0.5f + level);
                }
                byte[] packed = AsymmetricHashingScorer.packMultiBitCodes(codes, bitsPerDim);

                float[] qt = new float[nDims];
                for (int j = 0; j < nDims; j++) {
                    qt[j] = (float) random().nextGaussian();
                }
                float scale = randomFloat() * 3;
                float offset = (float) random().nextGaussian();
                float qdc = (float) random().nextGaussian();

                float floatScore = AsymmetricHashingScorer.scoreOneVector(qt, qdc, codes, scale, offset);
                float multiBitScore = AsymmetricHashingScorer.scoreOneVectorMultiBit(qt, qdc, packed, nDims, bitsPerDim, scale, offset);
                assertEquals("Mismatch at bits=" + bitsPerDim + " iter=" + iter, floatScore, multiBitScore, 1e-3f);
            }
        }
    }

    public void testPackMultiBitCodesRoundtrip() {
        int bitsPerDim = 2;
        int nDims = 13;
        int numLevels = 1 << bitsPerDim;
        float centerOffset = (numLevels - 1) / 2.0f;

        // Create codes with known levels
        float[] codes = new float[nDims];
        int[] expectedUnsigned = new int[nDims];
        for (int j = 0; j < nDims; j++) {
            // Random level from valid set
            expectedUnsigned[j] = randomIntBetween(0, numLevels - 1);
            codes[j] = expectedUnsigned[j] - centerOffset;
        }

        byte[] packed = AsymmetricHashingScorer.packMultiBitCodes(codes, bitsPerDim);
        int planeBytes = (nDims + 7) >>> 3;
        assertEquals(bitsPerDim * planeBytes, packed.length);

        // Decode and verify
        for (int j = 0; j < nDims; j++) {
            int byteIdx = j >>> 3;
            int bitIdx = 7 - (j & 7);
            int decoded = 0;
            for (int p = 0; p < bitsPerDim; p++) {
                if ((packed[p * planeBytes + byteIdx] & (1 << bitIdx)) != 0) {
                    decoded |= (1 << p);
                }
            }
            assertEquals("Mismatch at dim " + j, expectedUnsigned[j], decoded);
        }
    }

    public void testPackedByteLength() {
        assertEquals(2, AsymmetricHashingScorer.packedByteLength(8, 2));
        assertEquals(4, AsymmetricHashingScorer.packedByteLength(9, 2));
        assertEquals(36, AsymmetricHashingScorer.packedByteLength(96, 3));
        assertEquals(12, AsymmetricHashingScorer.packedByteLength(96, 1));
    }

    public void testBatchScoreConsistency() {
        int nVectors = 20;
        int originalDim = 8;
        int nDims = 4;

        float[] query = new float[originalDim];
        for (int d = 0; d < originalDim; d++) {
            query[d] = (float) random().nextGaussian();
        }

        // Simple W: project first 4 dims
        float[][] w = new float[originalDim][nDims];
        for (int i = 0; i < nDims; i++) {
            w[i][i] = 1.0f;
        }

        float[][] centroids = { new float[originalDim] }; // zero centroid
        int[] assignments = new int[nVectors];

        float[][] encodedVectors = new float[nVectors][nDims];
        float[] scales = new float[nVectors];
        float[] offsets = new float[nVectors];
        for (int i = 0; i < nVectors; i++) {
            for (int j = 0; j < nDims; j++) {
                encodedVectors[i][j] = (float) random().nextGaussian();
            }
            scales[i] = 0.5f + randomFloat();
            offsets[i] = (float) random().nextGaussian() * 0.1f;
        }

        float[] batchScores = AsymmetricHashingScorer.score(query, w, centroids, assignments, encodedVectors, scales, offsets);

        // queryTransformed = (query - 0) @ W = first 4 dims of query
        float[] qt = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            qt[j] = query[j];
        }
        float qdc = 0f; // centroid is zero

        for (int i = 0; i < nVectors; i++) {
            float singleScore = AsymmetricHashingScorer.scoreOneVector(qt, qdc, encodedVectors[i], scales[i], offsets[i]);
            assertEquals("Mismatch at vector " + i, batchScores[i], singleScore, 1e-5f);
        }
    }

    public void testZeroDimensionScoring() {
        // Edge case: 0-dim vectors
        float score = AsymmetricHashingScorer.scoreOneVector(new float[0], 1.5f, new float[0], 2.0f, 0.3f);
        // dot = 0, result = 0 * 2.0 + 1.5 + 0.3 = 1.8
        assertEquals(1.8f, score, 1e-6f);
    }

    public void testReconstructedDotProductApproximatesTrueDotProduct() {
        // With an identity projection (nDims == originalDim, W = I) and an *unquantized* encoded
        // vector (scale = 1, encodedVector = the exact residual), the scorer's formula is an exact
        // algebraic identity:
        //
        // dot(query, vector) = dot(query - centroid, vector - centroid) + dot(query, centroid) + offset
        //
        // where offset = dot(vector, centroid) - dot(centroid, centroid). This verifies the scorer
        // reconstructs the true dot product between the original (un-centered, un-projected)
        // vectors, up to floating-point rounding.
        //
        // Note: we deliberately don't layer real (lossy) quantization on top here. Once the
        // residual is quantized down to a handful of bits per dimension, the worst-case
        // reconstruction error grows linearly with the dimension and can dwarf the true dot
        // product whenever it happens to be small -- the same reason the full-pipeline test in
        // AsymmetricHashingQuantizerTests only checks rank correlation, not closeness, once real
        // quantization is involved.
        for (int iter = 0; iter < 50; iter++) {
            int dim = randomIntBetween(1, 64);

            float[] query = new float[dim];
            float[] vector = new float[dim];
            float[] centroid = new float[dim];
            for (int d = 0; d < dim; d++) {
                query[d] = (float) random().nextGaussian();
                vector[d] = (float) random().nextGaussian();
                centroid[d] = (float) random().nextGaussian();
            }

            float trueDot = ESVectorUtil.dotProduct(query, vector, dim);

            float[] queryTransformed = new float[dim];
            float[] encodedVector = new float[dim];
            for (int d = 0; d < dim; d++) {
                queryTransformed[d] = query[d] - centroid[d];
                encodedVector[d] = vector[d] - centroid[d];
            }
            float queryDotCentroid = ESVectorUtil.dotProduct(query, centroid, dim);
            float offset = ESVectorUtil.dotProduct(vector, centroid, dim) - ESVectorUtil.dotProduct(centroid, centroid, dim);

            float reconstructed = AsymmetricHashingScorer.scoreOneVector(queryTransformed, queryDotCentroid, encodedVector, 1.0f, offset);

            assertEquals(
                "Mismatch at iter " + iter + " dim=" + dim,
                trueDot,
                reconstructed,
                Math.max(1e-3f, Math.abs(trueDot) * 1e-4f)
            );
        }
    }
}
