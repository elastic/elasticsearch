/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

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
        Set<Float> validMags = Set.of(0.5f, 1.5f);
        for (int iter = 0; iter < 20; iter++) {
            int d = randomIntBetween(4, 200);
            float[] input = randomGaussianVector(d);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
            for (int j = 0; j < d; j++) {
                float mag = Math.abs(result.centeredCode()[j]);
                assertTrue("Expected 0.5 or 1.5 but got " + mag, validMags.contains(mag));
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
                assertTrue("Expected positive code for positive input at dim " + j, result.centeredCode()[j] > 0);
            } else {
                assertTrue("Expected negative code for negative input at dim " + j, result.centeredCode()[j] < 0);
            }
        }
    }

    public void test3BitMagnitudes() {
        // 3-bit: numAbsLevels=4, magnitudes in {0.5, 1.5, 2.5, 3.5}
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(3);
        Set<Float> validMags = Set.of(0.5f, 1.5f, 2.5f, 3.5f);
        for (int iter = 0; iter < 20; iter++) {
            int d = randomIntBetween(4, 100);
            float[] input = randomGaussianVector(d);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
            for (int j = 0; j < d; j++) {
                float mag = Math.abs(result.centeredCode()[j]);
                assertTrue("Expected magnitude in " + validMags + " but got " + mag, validMags.contains(mag));
            }
        }
    }

    public void test4BitMagnitudes() {
        // 4-bit: numAbsLevels=8, magnitudes in {0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5}
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(4);
        Set<Float> validMags = Set.of(0.5f, 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f);
        for (int iter = 0; iter < 10; iter++) {
            int d = randomIntBetween(4, 100);
            float[] input = randomGaussianVector(d);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
            for (int j = 0; j < d; j++) {
                float mag = Math.abs(result.centeredCode()[j]);
                assertTrue("Expected magnitude in " + validMags + " but got " + mag, validMags.contains(mag));
            }
        }
    }

    public void testNormPositive() {
        for (int bits = 2; bits <= 4; bits++) {
            AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(bits);
            int d = randomIntBetween(4, 200);
            float[] input = randomGaussianVector(d);
            AshSphericalScalarQuantizer.SingleQuantizeResult result = ssq.encodeOne(input);
            assertTrue("Norm should be positive, got " + result.codeNorm(), result.codeNorm() > 0);
        }
    }

    public void testEncodeOneMatchesBatch() {
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        int d = 16;
        float[] input = randomGaussianVector(d);

        AshSphericalScalarQuantizer.SingleQuantizeResult single = ssq.encodeOne(input);
        AshSphericalScalarQuantizer.QuantizeResult batch = ssq.encode(new float[][] { input });

        assertArrayEquals(single.centeredCode(), batch.centeredCodes()[0], 0f);
        assertEquals(single.codeNorm(), batch.codeNorms()[0], 0f);
    }

    public void testEmptyInput() {
        AshSphericalScalarQuantizer ssq = new AshSphericalScalarQuantizer(2);
        AshSphericalScalarQuantizer.QuantizeResult result = ssq.encode(new float[0][0]);
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
            double trueDot = 0;
            double quantDot = 0;
            AshSphericalScalarQuantizer.SingleQuantizeResult enc = ssq.encodeOne(vectors[i]);
            AshSphericalScalarQuantizer.SingleQuantizeResult qEnc = ssq.encodeOne(query);
            for (int j = 0; j < d; j++) {
                trueDot += (double) vectors[i][j] * query[j];
                quantDot += (double) enc.centeredCode()[j] * qEnc.centeredCode()[j];
            }
            sumProduct += trueDot * quantDot;
            sumTrue2 += trueDot * trueDot;
            sumQuant2 += quantDot * quantDot;
        }
        double correlation = sumProduct / Math.sqrt(sumTrue2 * sumQuant2);
        assertTrue("Expected positive correlation, got " + correlation, correlation > 0.3);
    }

    private float[] randomGaussianVector(int d) {
        float[] v = new float[d];
        for (int j = 0; j < d; j++) {
            v[j] = (float) random().nextGaussian();
        }
        return v;
    }
}
