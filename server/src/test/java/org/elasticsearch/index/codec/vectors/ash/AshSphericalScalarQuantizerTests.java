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
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

/**
 * Tests for {@link AshSphericalScalarQuantizer}.
 */
public class AshSphericalScalarQuantizerTests extends ESTestCase {

    public void testInvalidBitsPerDimThrows() {
        expectThrows(IllegalArgumentException.class, () -> new AshSphericalScalarQuantizer(0));
        expectThrows(IllegalArgumentException.class, () -> new AshSphericalScalarQuantizer(-1));
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
                assertMatchesBruteForceOptimum(ssq, numAbsLevels, randomGaussianVector(d), iter);
            }
            for (int iter = 0; iter < 20; iter++) {
                assertMatchesBruteForceOptimum(ssq, numAbsLevels, randomTiedVector(d), iter);
            }
        }
    }

    public void testNormMatchesCode() {
        for (int bitsPerDim = 1; bitsPerDim <= 8; bitsPerDim++) {
            AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(bitsPerDim);
            int nSteps = (1 << (bitsPerDim - 1)) - 1;
            int d = randomIntBetween(1, 200);
            float[] z = randomFlavouredVector(d, nSteps);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(z);

            // the quantization uses doubles, where (0.5 + level)^2 is exact, so
            // the returned norm should be exact
            double normSq = 0;
            for (float c : result.centeredCode()) {
                normSq = Math.fma(c, c, normSq);
            }
            assertEquals("Norm is incorrect! bitsPerDim=" + bitsPerDim + " d=" + d, (float) Math.sqrt(normSq), result.codeNorm(), 0f);
        }
    }

    public void testLevelsAreMonotonicInMagnitude() {
        for (int bitsPerDim = 1; bitsPerDim <= 8; bitsPerDim++) {
            AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(bitsPerDim);
            int nSteps = (1 << (bitsPerDim - 1)) - 1;
            int d = randomIntBetween(2, 64);
            float[] z = randomFlavouredVector(d, nSteps);
            float[] code = ssq.encodeOne(z).centeredCode();
            for (int i = 0; i < d; i++) {
                for (int j = 0; j < d; j++) {
                    if (Math.abs(z[i]) > Math.abs(z[j])) {
                        assertThat(
                            "dim " + i + " has a larger |z| than dim " + j + " but a lower level, at bitsPerDim=" + bitsPerDim,
                            Math.abs(code[i]),
                            greaterThanOrEqualTo(Math.abs(code[j]))
                        );
                    }
                }
            }
        }
    }

    /**
     * Tests various degenerate cases of the general path
     */
    public void testGeneralPathDegenerateInputs() {
        for (int bitsPerDim = 3; bitsPerDim <= 8; bitsPerDim++) {
            AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(bitsPerDim);
            float maxMagnitude = 0.5f + (1 << (bitsPerDim - 1)) - 1;

            // single dimension
            AshSphericalScalarQuantizer.SingleQuantizeResult single = ssq.encodeOne(new float[] { randomFrom(-3f, 0.5f, 7f) });
            assertThat(Math.abs(single.centeredCode()[0]), greaterThanOrEqualTo(0.5f));
            assertThat(Math.abs(single.centeredCode()[0]), lessThanOrEqualTo(maxMagnitude));
            assertEquals(Math.abs(single.centeredCode()[0]), single.codeNorm(), 0f);

            // zero vector
            int d = randomIntBetween(2, 64);
            AshSphericalScalarQuantizer.SingleQuantizeResult zeros = ssq.encodeOne(new float[d]);
            for (int j = 0; j < d; j++) {
                assertEquals("all-zero input must stay at the base level", 0.5f, Math.abs(zeros.centeredCode()[j]), 0f);
            }
            assertEquals((float) Math.sqrt(0.25 * d), zeros.codeNorm(), 0f);

            // one non-zero value
            float[] oneNonZero = new float[d];
            oneNonZero[randomIntBetween(0, d - 1)] = 7f;
            AshSphericalScalarQuantizer.SingleQuantizeResult sparse = ssq.encodeOne(oneNonZero);
            for (int j = 0; j < d; j++) {
                if (oneNonZero[j] == 0) {
                    assertEquals("zero magnitude gained a level", 0.5f, Math.abs(sparse.centeredCode()[j]), 0f);
                }
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

    private static void assertMatchesBruteForceOptimum(AshSphericalScalarQuantizer ssq, int numAbsLevels, float[] z, int iter) {
        AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(z);
        double greedyCos = ESVectorUtil.dotProduct(z, result.centeredCode()) / result.codeNorm();
        double bestCos = bruteForceBestCosSimilarity(z, numAbsLevels);
        assertEquals("Mismatch at bitsPerDim=" + ssq.bitsPerDimension() + " iter=" + iter, bestCos, greedyCos, 1e-4);
    }

    private static double bruteForceBestCosSimilarity(float[] z, int numAbsLevels) {
        float[] absZ = new float[z.length];
        for (int i = 0; i < z.length; i++) {
            absZ[i] = Math.abs(z[i]);
        }
        return bruteForceRecurse(absZ, new int[z.length], 0, numAbsLevels);
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

    /**
     * Returns a vector of a random variety - either degenerate in some form, or standard
     */
    private float[] randomFlavouredVector(int d, int nSteps) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> randomGaussianVector(d);
            case 1 -> randomTiedVector(d);
            case 2 -> randomNearBoundaryVector(d, nSteps);
            case 3 -> randomDegenerateVector(d);
            default -> throw new AssertionError();
        };
    }

    private float[] randomGaussianVector(int d) {
        return SvdUtil.randomGaussians(random(), d);
    }

    private float[] randomDegenerateVector(int d) {
        float[] v = new float[d];
        for (int j = 0; j < d; j++) {
            v[j] = (randomBoolean() ? 1 : -1) * randomFrom(1e-40f, 1e-20f, 1f, 1e20f);
        }
        return v;
    }

    /**
     * Magnitudes drawn from {0.5, 1.0, 1.5, 2.0}, whose ratios are exact, so the critical times of
     * different (step, dimension) pairs collide en masse. This is the tie-heavy input that the
     * removed duplicate-key sorter test stood in for.
     */
    private float[] randomTiedVector(int d) {
        float[] v = new float[d];
        for (int j = 0; j < d; j++) {
            v[j] = (randomBoolean() ? 1 : -1) * (randomIntBetween(1, 4) / 2f);
        }
        return v;
    }

    /**
     * Magnitudes sitting on, or an ulp either side of, an exact level boundary of another magnitude
     * in the same vector: |z_j| = k * |z_0| / s. Whichever magnitude ends up carrying the threshold,
     * most of the others then divide into it at very nearly a whole number of levels, which is where
     * the rounding in the reconstruction can land on the wrong side of a boundary.
     */
    private float[] randomNearBoundaryVector(int d, int nSteps) {
        float[] v = new float[d];
        float base = 1f + randomFloat();
        v[0] = base;
        // at bitsPerDim=1 there are no steps to straddle, so fall back to whole multiples of the base
        int steps = Math.max(nSteps, 1);
        for (int j = 1; j < d; j++) {
            float onBoundary = (float) ((double) randomIntBetween(1, steps) * base / randomIntBetween(1, steps));
            float magnitude = switch (randomIntBetween(0, 2)) {
                case 0 -> onBoundary;
                case 1 -> Math.nextUp(onBoundary);
                default -> Math.nextDown(onBoundary);
            };
            v[j] = randomBoolean() ? magnitude : -magnitude;
        }
        return v;
    }
}
