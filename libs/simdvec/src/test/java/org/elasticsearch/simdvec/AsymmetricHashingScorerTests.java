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

    public void testScoreBasic() {
        // qt = [2, 3], valid 2-bit codes: [0.5, -0.5]
        // dot = 2*0.5 + 3*(-0.5) = -0.5
        // result = -0.5 * 1.0 + 0.0 + 0.0 = -0.5
        float[] codes = { 0.5f, -0.5f };
        byte[] packed = AsymmetricHashingScorer.pack(codes, 2);
        float score = AsymmetricHashingScorer.score(new float[] { 2.0f, 3.0f }, 0.0f, packed, 0, 2, 2, 1.0f, 0.0f);
        float expected = referenceScore(new float[] { 2.0f, 3.0f }, 0.0f, codes, 1.0f, 0.0f);
        assertEquals(expected, score, 1e-4f);
    }

    public void testScaleAndOffsetApplied() {
        // codes = [0.5, 0.5] (valid 2-bit level)
        // dot = 2*0.5 + 3*0.5 = 2.5
        // result = 2.5 * 2.0 + 1.5 + 0.3 = 6.8
        float[] codes = { 0.5f, 0.5f };
        byte[] packed = AsymmetricHashingScorer.pack(codes, 2);
        float score = AsymmetricHashingScorer.score(new float[] { 2.0f, 3.0f }, 1.5f, packed, 0, 2, 2, 2.0f, 0.3f);
        float expected = referenceScore(new float[] { 2.0f, 3.0f }, 1.5f, codes, 2.0f, 0.3f);
        assertEquals(expected, score, 1e-4f);
    }

    public void testScoreMatchesReferenceDotProduct() {
        // Verify packed scoring matches a reference float dot product computation
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
                byte[] packed = AsymmetricHashingScorer.pack(codes, bitsPerDim);

                float[] qt = new float[nDims];
                for (int j = 0; j < nDims; j++) {
                    qt[j] = (float) random().nextGaussian();
                }
                float scale = randomFloat() * 3;
                float offset = (float) random().nextGaussian();
                float qdc = (float) random().nextGaussian();

                float expected = referenceScore(qt, qdc, codes, scale, offset);
                float actual = AsymmetricHashingScorer.score(qt, qdc, packed, 0, nDims, bitsPerDim, scale, offset);
                assertEquals("Mismatch at bits=" + bitsPerDim + " iter=" + iter, expected, actual, 1e-3f);
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

        byte[] packed = AsymmetricHashingScorer.pack(codes, bitsPerDim);
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
        assertEquals(2, AsymmetricHashingScorer.packedLength(8, 2));
        assertEquals(4, AsymmetricHashingScorer.packedLength(9, 2));
        assertEquals(36, AsymmetricHashingScorer.packedLength(96, 3));
        assertEquals(12, AsymmetricHashingScorer.packedLength(96, 1));
    }

    public void testZeroDimensionScoring() {
        // Edge case: 0-dim vectors; dot = 0, result = 0 * 2.0 + 1.5 + 0.3 = 1.8
        byte[] packed = AsymmetricHashingScorer.pack(new float[0], 2);
        float score = AsymmetricHashingScorer.score(new float[0], 1.5f, packed, 0, 0, 2, 2.0f, 0.3f);
        assertEquals(1.8f, score, 1e-6f);
    }

    public void testNonZeroCodeOffset() {
        // Pack multiple vectors into a single contiguous buffer and score each at its offset,
        // verifying the same result as scoring standalone per-vector arrays at offset 0.
        int bitsPerDim = 2;
        int numAbsLevels = 1 << (bitsPerDim - 1);
        int nVectors = 5;
        int nDims = 32;
        int packedLen = AsymmetricHashingScorer.packedLength(nDims, bitsPerDim);

        // Generate random codes and pack each vector
        float[][] allCodes = new float[nVectors][nDims];
        byte[][] individualPacked = new byte[nVectors][];
        for (int v = 0; v < nVectors; v++) {
            for (int j = 0; j < nDims; j++) {
                float sign = randomBoolean() ? 1.0f : -1.0f;
                int level = randomIntBetween(0, numAbsLevels - 1);
                allCodes[v][j] = sign * (0.5f + level);
            }
            individualPacked[v] = AsymmetricHashingScorer.pack(allCodes[v], bitsPerDim);
        }

        // Build a contiguous bulk buffer (like AshPostingsVisitor reads from disk)
        byte[] bulkBuffer = new byte[nVectors * packedLen];
        for (int v = 0; v < nVectors; v++) {
            System.arraycopy(individualPacked[v], 0, bulkBuffer, v * packedLen, packedLen);
        }

        // Random query and scoring params
        float[] qt = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            qt[j] = (float) random().nextGaussian();
        }
        float qdc = (float) random().nextGaussian();
        float scale = randomFloat() * 3;
        float offset = (float) random().nextGaussian();

        // Score each vector from the bulk buffer at its offset and compare to standalone scoring
        for (int v = 0; v < nVectors; v++) {
            float standaloneScore = AsymmetricHashingScorer.score(qt, qdc, individualPacked[v], 0, nDims, bitsPerDim, scale, offset);
            float bulkScore = AsymmetricHashingScorer.score(qt, qdc, bulkBuffer, v * packedLen, nDims, bitsPerDim, scale, offset);
            assertEquals("Mismatch at vector " + v, standaloneScore, bulkScore, 0f);
        }
    }

    public void testScoreIntegerApproximatesFloat() {
        // Verify integer scoring with quantized query produces correlated results with float scoring.
        // The integer path trades precision for throughput; we verify rank correlation over many vectors
        // rather than per-vector closeness, since quantization error accumulates across dimensions.
        int bitsPerDim = 2;
        int numAbsLevels = 1 << (bitsPerDim - 1);
        int nDims = 32;
        int planeBytes = (nDims + 7) >>> 3;
        int nVectors = 100;

        for (int queryBits : new int[] { 4, 8 }) {
            // Random query transform
            float[] qt = new float[nDims];
            for (int j = 0; j < nDims; j++) {
                qt[j] = (float) random().nextGaussian();
            }
            float qdc = (float) random().nextGaussian();

            // Quantize query
            float qMin = Float.MAX_VALUE, qMax = -Float.MAX_VALUE;
            for (int j = 0; j < nDims; j++) {
                qMin = Math.min(qMin, qt[j]);
                qMax = Math.max(qMax, qt[j]);
            }
            float range = qMax - qMin;
            int numQueryLevels = 1 << queryBits;
            float qScale = range > 0 ? (numQueryLevels - 1) / range : 1.0f;
            float invQScale = range > 0 ? range / (numQueryLevels - 1) : 0f;
            float centerOff = ((1 << bitsPerDim) - 1) / 2.0f;

            byte[] queryQuantized = new byte[queryBits * planeBytes];
            int unsignedQuerySum = 0;
            for (int j = 0; j < nDims; j++) {
                int qlevel = Math.clamp(Math.round((qt[j] - qMin) * qScale), 0, numQueryLevels - 1);
                unsignedQuerySum += qlevel;
                int byteIdx = j >>> 3;
                int bitIdx = 7 - (j & 7);
                for (int p = 0; p < queryBits; p++) {
                    if (((qlevel >> p) & 1) != 0) {
                        queryQuantized[p * planeBytes + byteIdx] |= (byte) (1 << bitIdx);
                    }
                }
            }
            float constantCorrection = centerOff * (unsignedQuerySum * invQScale + qMin * nDims);

            // Score many vectors with both paths and check rank correlation
            float[] floatScores = new float[nVectors];
            float[] intScores = new float[nVectors];
            for (int v = 0; v < nVectors; v++) {
                float[] codes = new float[nDims];
                for (int j = 0; j < nDims; j++) {
                    float sign = randomBoolean() ? 1.0f : -1.0f;
                    int level = randomIntBetween(0, numAbsLevels - 1);
                    codes[j] = sign * (0.5f + level);
                }
                byte[] packed = AsymmetricHashingScorer.pack(codes, bitsPerDim);
                float scale = 0.5f + randomFloat() * 2;
                float offset = (float) random().nextGaussian() * 0.5f;
                int docSum = 0;
                for (int j = 0; j < nDims; j++) {
                    docSum += Math.round(codes[j] + centerOff);
                }

                floatScores[v] = AsymmetricHashingScorer.score(qt, qdc, packed, 0, nDims, bitsPerDim, scale, offset);
                intScores[v] = AsymmetricHashingScorer.scoreInteger(
                    queryQuantized,
                    queryBits,
                    qdc,
                    packed,
                    0,
                    bitsPerDim,
                    planeBytes,
                    scale,
                    offset,
                    (short) docSum,
                    invQScale,
                    qMin,
                    constantCorrection
                );
            }

            // Verify Pearson correlation > threshold (rank preservation)
            double sumF = 0, sumI = 0, sumFF = 0, sumII = 0, sumFI = 0;
            for (int v = 0; v < nVectors; v++) {
                sumF += floatScores[v];
                sumI += intScores[v];
                sumFF += (double) floatScores[v] * floatScores[v];
                sumII += (double) intScores[v] * intScores[v];
                sumFI += (double) floatScores[v] * intScores[v];
            }
            double meanF = sumF / nVectors, meanI = sumI / nVectors;
            double varF = sumFF / nVectors - meanF * meanF;
            double varI = sumII / nVectors - meanI * meanI;
            double covFI = sumFI / nVectors - meanF * meanI;
            double pearson = covFI / Math.sqrt(varF * varI);

            double threshold = queryBits >= 8 ? 0.99 : 0.85;
            assertTrue("queryBits=" + queryBits + " Pearson correlation " + pearson + " below " + threshold, pearson > threshold);
        }
    }

    /** Reference scorer: computes dot(qt, codes) * scale + qdc + offset using plain float arithmetic. */
    private static float referenceScore(float[] qt, float qdc, float[] codes, float scale, float offset) {
        double dot = 0;
        for (int j = 0; j < qt.length; j++) {
            dot += (double) qt[j] * codes[j];
        }
        return (float) dot * scale + qdc + offset;
    }
}
