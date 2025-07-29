/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer.MINIMUM_MSE_GRID;

public class OptimizedScalarQuantizerTests extends ESTestCase {

    static final byte[] ALL_BITS = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };

    static float[] deQuantize(byte[] quantized, byte bits, float[] interval, float[] centroid) {
        float[] dequantized = new float[quantized.length];
        float a = interval[0];
        float b = interval[1];
        int nSteps = (1 << bits) - 1;
        double step = (b - a) / nSteps;
        for (int h = 0; h < quantized.length; h++) {
            double xi = (double) (quantized[h] & 0xFF) * step + a;
            dequantized[h] = (float) (xi + centroid[h]);
        }
        return dequantized;
    }

    public void testQuantizationQuality() {
        int dims = 16;
        int numVectors = 32;
        float[][] vectors = new float[numVectors][];
        float[] centroid = new float[dims];
        for (int i = 0; i < numVectors; ++i) {
            vectors[i] = new float[dims];
            for (int j = 0; j < dims; ++j) {
                vectors[i][j] = randomFloat();
                centroid[j] += vectors[i][j];
            }
        }
        for (int j = 0; j < dims; ++j) {
            centroid[j] /= numVectors;
        }
        // similarity doesn't matter for this test
        OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT);
        float[] scratch = new float[dims];
        for (byte bit : ALL_BITS) {
            float eps = (1f / (float) (1 << (bit)));
            byte[] destination = new byte[dims];
            for (int i = 0; i < numVectors; ++i) {
                System.arraycopy(vectors[i], 0, scratch, 0, dims);
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(scratch, destination, bit, centroid);
                assertValidResults(result);
                assertValidQuantizedRange(destination, bit);

                float[] dequantized = deQuantize(
                    destination,
                    bit,
                    new float[] { result.lowerInterval(), result.upperInterval() },
                    centroid
                );
                float mae = 0;
                for (int k = 0; k < dims; ++k) {
                    mae += Math.abs(dequantized[k] - vectors[i][k]);
                }
                mae /= dims;
                assertTrue("bits: " + bit + " mae: " + mae + " > eps: " + eps, mae <= eps);
            }
        }
    }

    public void testAbusiveEdgeCases() {
        // large zero array
        for (VectorSimilarityFunction vectorSimilarityFunction : VectorSimilarityFunction.values()) {
            if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                continue;
            }
            float[] vector = new float[4096];
            float[] centroid = new float[4096];
            OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(vectorSimilarityFunction);
            byte[][] destinations = new byte[MINIMUM_MSE_GRID.length][4096];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(vector, destinations, ALL_BITS, centroid);
            assertEquals(MINIMUM_MSE_GRID.length, results.length);
            assertValidResults(results);
            for (byte[] destination : destinations) {
                assertArrayEquals(new byte[4096], destination);
            }
            byte[] destination = new byte[4096];
            for (byte bit : ALL_BITS) {
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(vector, destination, bit, centroid);
                assertValidResults(result);
                assertArrayEquals(new byte[4096], destination);
            }
        }

        // single value array
        for (VectorSimilarityFunction vectorSimilarityFunction : VectorSimilarityFunction.values()) {
            float[] vector = new float[] { randomFloat() };
            float[] centroid = new float[] { randomFloat() };
            if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                VectorUtil.l2normalize(vector);
                VectorUtil.l2normalize(centroid);
            }
            OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(vectorSimilarityFunction);
            byte[][] destinations = new byte[MINIMUM_MSE_GRID.length][1];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(vector, destinations, ALL_BITS, centroid);
            assertEquals(MINIMUM_MSE_GRID.length, results.length);
            assertValidResults(results);
            for (int i = 0; i < ALL_BITS.length; i++) {
                assertValidQuantizedRange(destinations[i], ALL_BITS[i]);
            }
            for (byte bit : ALL_BITS) {
                vector = new float[] { randomFloat() };
                centroid = new float[] { randomFloat() };
                if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                    VectorUtil.l2normalize(vector);
                    VectorUtil.l2normalize(centroid);
                }
                byte[] destination = new byte[1];
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(vector, destination, bit, centroid);
                assertValidResults(result);
                assertValidQuantizedRange(destination, bit);
            }
        }

    }

    public void testMathematicalConsistency() {
        int dims = randomIntBetween(1, 4096);
        float[] vector = new float[dims];
        for (int i = 0; i < dims; ++i) {
            vector[i] = randomFloat();
        }
        float[] centroid = new float[dims];
        for (int i = 0; i < dims; ++i) {
            centroid[i] = randomFloat();
        }
        float[] copy = new float[dims];
        for (VectorSimilarityFunction vectorSimilarityFunction : VectorSimilarityFunction.values()) {
            // copy the vector to avoid modifying it
            System.arraycopy(vector, 0, copy, 0, dims);
            if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                VectorUtil.l2normalize(copy);
                VectorUtil.l2normalize(centroid);
            }
            OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(vectorSimilarityFunction);
            byte[][] destinations = new byte[MINIMUM_MSE_GRID.length][dims];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(copy, destinations, ALL_BITS, centroid);
            assertEquals(MINIMUM_MSE_GRID.length, results.length);
            assertValidResults(results);
            for (int i = 0; i < ALL_BITS.length; i++) {
                assertValidQuantizedRange(destinations[i], ALL_BITS[i]);
            }
            for (byte bit : ALL_BITS) {
                byte[] destination = new byte[dims];
                System.arraycopy(vector, 0, copy, 0, dims);
                if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                    VectorUtil.l2normalize(copy);
                    VectorUtil.l2normalize(centroid);
                }
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(copy, destination, bit, centroid);
                assertValidResults(result);
                assertValidQuantizedRange(destination, bit);
            }
        }
    }

    static void assertValidQuantizedRange(byte[] quantized, byte bits) {
        for (byte b : quantized) {
            if (bits < 8) {
                assertTrue(b >= 0);
            }
            assertTrue(b < 1 << bits);
        }
    }

    static void assertValidResults(OptimizedScalarQuantizer.QuantizationResult... results) {
        for (OptimizedScalarQuantizer.QuantizationResult result : results) {
            assertTrue(Float.isFinite(result.lowerInterval()));
            assertTrue(Float.isFinite(result.upperInterval()));
            assertTrue(result.lowerInterval() <= result.upperInterval());
            assertTrue(Float.isFinite(result.additionalCorrection()));
            assertTrue(result.quantizedComponentSum() >= 0);
        }
    }
}
