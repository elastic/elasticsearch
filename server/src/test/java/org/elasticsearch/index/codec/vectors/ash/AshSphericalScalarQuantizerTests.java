/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.oneOf;

/**
 * Tests for {@link AshSphericalScalarQuantizer}.
 */
public class AshSphericalScalarQuantizerTests extends ESTestCase {

    public void testInvalidBitsPerDimThrows() {
        expectThrows(IllegalArgumentException.class, () -> new AshSphericalScalarQuantizer(0));
        expectThrows(IllegalArgumentException.class, () -> new AshSphericalScalarQuantizer(-1));
    }

    public void testBitsPerDimension() {
        assertEquals(1, new AshSphericalScalarQuantizer(1).bitsPerDimension());
        assertEquals(2, new AshSphericalScalarQuantizer(2).bitsPerDimension());
        assertEquals(3, new AshSphericalScalarQuantizer(3).bitsPerDimension());
        assertEquals(4, new AshSphericalScalarQuantizer(4).bitsPerDimension());
    }

    public void test2BitMagnitudes() {
        // 2-bit: magnitudes must be 0.5 or 1.5
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        for (int iter = 0; iter < 20; iter++) {
            int d = randomIntBetween(4, 200);
            float[] input = randomGaussianVector(d);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
            for (int j = 0; j < d; j++) {
                float mag = Math.abs(result.centeredCode()[j]);
                assertThat(mag, oneOf(0.5f, 1.5f));
            }
        }
    }

    public void test2BitSignPreservation() {
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        int d = 50;
        float[] input = randomGaussianVector(d);
        AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
        for (int j = 0; j < d; j++) {
            if (input[j] >= 0) {
                assertThat("Expected positive code for positive input at dim " + j, result.centeredCode()[j], greaterThan(0f));
            } else {
                assertThat("Expected negative code for negative input at dim " + j, result.centeredCode()[j], lessThan(0f));
            }
        }
    }

    public void test3BitMagnitudes() {
        // 3-bit: numAbsLevels=4, magnitudes in {0.5, 1.5, 2.5, 3.5}
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(3);
        for (int iter = 0; iter < 20; iter++) {
            int d = randomIntBetween(4, 100);
            float[] input = randomGaussianVector(d);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
            for (int j = 0; j < d; j++) {
                float mag = Math.abs(result.centeredCode()[j]);
                assertThat(mag, oneOf(0.5f, 1.5f, 2.5f, 3.5f));
            }
        }
    }

    public void test4BitMagnitudes() {
        // 4-bit: numAbsLevels=8, magnitudes in {0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5}
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(4);
        for (int iter = 0; iter < 10; iter++) {
            int d = randomIntBetween(4, 100);
            float[] input = randomGaussianVector(d);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
            for (int j = 0; j < d; j++) {
                float mag = Math.abs(result.centeredCode()[j]);
                assertThat(mag, oneOf(0.5f, 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f));
            }
        }
    }

    public void testNormPositive() {
        for (int bits = 2; bits <= 4; bits++) {
            AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(bits);
            int d = randomIntBetween(4, 200);
            float[] input = randomGaussianVector(d);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
            assertThat(result.codeNorm(), greaterThan(0f));
        }
    }

    /**
     * Every row of a batch must quantize identically to the same vector passed to
     * {@link AshSphericalScalarQuantizer#encodeOne}. Uses several rows so the batch path is
     * exercised at non-zero offsets into the flat input and output arrays.
     */
    public void testEncodeOneMatchesBatch() {
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(randomIntBetween(1, 4));
        int d = 16;
        int n = randomIntBetween(2, 5);
        float[] batchInput = randomGaussianVector(n * d);

        AshSphericalScalarQuantizer.QuantizeResult batch = ssq.encode(batchInput, n, d);
        assertEquals(n * d, batch.centeredCodes().length);
        assertEquals(n, batch.codeNorms().length);

        for (int i = 0; i < n; i++) {
            int rowIdx = i * d;
            float[] row = ArrayUtil.copyOfSubArray(batchInput, rowIdx, rowIdx + d);
            AshSphericalScalarQuantizer.SingleQuantizeResult single = ssq.encodeOne(row);
            assertArrayEquals("row " + i, single.centeredCode(), ArrayUtil.copyOfSubArray(batch.centeredCodes(), rowIdx, rowIdx + d), 0f);
            assertEquals("row " + i, single.codeNorm(), batch.codeNorms()[i], 0f);
        }
    }

    public void testEmptyInput() {
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        AshSphericalScalarQuantizer.QuantizeResult result = ssq.encode(new float[0], 0, 16);
        assertEquals(0, result.centeredCodes().length);
        assertEquals(0, result.codeNorms().length);
    }

    public void test2BitOptimalAssignment() {
        // One very large dimension, rest small — the large dim should get 1.5, others 0.5
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        int d = 10;
        float[] input = new float[d];
        input[0] = 10.0f;  // much larger than the rest
        for (int j = 1; j < d; j++) {
            input[j] = 0.01f;
        }
        AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
        // The large dimension should be at level 1.5
        assertEquals(1.5f, Math.abs(result.centeredCode()[0]), 0f);
    }

    public void testInnerProductPreservation() {
        // Quantized dot product should positively correlate with true dot product
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        int d = 64;
        int n = 200;
        float[][] vectors = new float[n][d];
        for (int i = 0; i < n; i++) {
            vectors[i] = randomGaussianVector(d);
        }
        float[] query = randomGaussianVector(d);

        // True dot products vs quantized dot products
        double sumProduct = 0;
        double sumTrue2 = 0;
        double sumQuant2 = 0;
        for (int i = 0; i < n; i++) {
            AshSphericalScalarQuantizer.SingleQuantizeResult enc = ssq.encodeOne(vectors[i]);
            AshSphericalScalarQuantizer.SingleQuantizeResult qEnc = ssq.encodeOne(query);
            double trueDot = ESVectorUtil.dotProduct(vectors[i], query);
            double quantDot = ESVectorUtil.dotProduct(enc.centeredCode(), qEnc.centeredCode());
            sumProduct += trueDot * quantDot;
            sumTrue2 += trueDot * trueDot;
            sumQuant2 += quantDot * quantDot;
        }
        double correlation = sumProduct / Math.sqrt(sumTrue2 * sumQuant2);
        assertThat("Expected positive correlation", correlation, greaterThan(0.3));
    }

    public void testGeneralPathMatchesBruteForceOptimum() {
        // Regression test for a bug where the general (bitsPerDim >= 3) quantization path relied on
        // a mis-sorted event order and could land on a strictly suboptimal level assignment -- e.g.
        // it once returned all-base-level codes even though better assignments were reachable within
        // the same level budget. Brute force is only tractable for small d and bitsPerDim, so we
        // restrict this test to those.
        for (int bitsPerDim = 3; bitsPerDim <= 4; bitsPerDim++) {
            int numAbsLevels = 1 << (bitsPerDim - 1);
            AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(bitsPerDim);
            int d = 4;
            for (int iter = 0; iter < 20; iter++) {
                float[] z = randomGaussianVector(d);
                AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(z);
                double greedyCos = ESVectorUtil.dotProduct(z, result.centeredCode()) / result.codeNorm();

                double bestCos = bruteForceBestCosSimilarity(z, numAbsLevels);

                assertEquals("Mismatch at bitsPerDim=" + bitsPerDim + " iter=" + iter, bestCos, greedyCos, 1e-4);
            }
        }
    }

    public void testCosineSimilarityImprovesMonotonicallyWithMoreBits() {
        // A quantizer's level set at bitsPerDim=b is a strict subset of the level set at any b' > b
        // (magnitudes {0.5, ..., 0.5+2^(b-1)-1} vs {0.5, ..., 0.5+2^(b'-1)-1}), so the achievable
        // cosine similarity between a vector and its quantized code can only improve (never worsen)
        // as bitsPerDim increases. This was violated by the sort-direction bug above.
        int d = 128;
        for (int iter = 0; iter < 10; iter++) {
            float[] z = randomGaussianVector(d);
            double previousCos = -1;
            for (int bitsPerDim = 1; bitsPerDim <= 8; bitsPerDim++) {
                AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(bitsPerDim);
                AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(z);
                double cos = ESVectorUtil.dotProduct(z, result.centeredCode()) / result.codeNorm();
                assertThat(
                    "cos similarity regressed going from fewer to more bits at bitsPerDim=" + bitsPerDim + ": " + cos + " < " + previousCos,
                    cos,
                    greaterThanOrEqualTo(previousCos - 1e-6)
                );
                previousCos = cos;
            }
        }
    }

    private static double bruteForceBestCosSimilarity(float[] z, int numAbsLevels) {
        int d = z.length;
        float[] absZ = new float[d];
        for (int j = 0; j < d; j++) {
            absZ[j] = Math.abs(z[j]);
        }
        return bruteForceRecurse(absZ, new int[d], 0, numAbsLevels);
    }

    private static double bruteForceRecurse(float[] absZ, int[] levels, int idx, int numAbsLevels) {
        if (idx == absZ.length) {
            double dot = 0;
            double normSq = 0;
            for (int j = 0; j < absZ.length; j++) {
                double mag = 0.5 + levels[j];
                dot += absZ[j] * mag;
                normSq += mag * mag;
            }
            return dot / Math.sqrt(normSq);
        }
        double best = -1;
        for (int l = 0; l < numAbsLevels; l++) {
            levels[idx] = l;
            best = Math.max(best, bruteForceRecurse(absZ, levels, idx + 1, numAbsLevels));
        }
        return best;
    }

    private float[] randomGaussianVector(int d) {
        return SvdUtil.randomGaussians(random(), d);
    }
}
