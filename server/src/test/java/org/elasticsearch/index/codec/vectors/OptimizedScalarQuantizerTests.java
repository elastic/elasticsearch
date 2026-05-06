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

    static float[] deQuantize(int[] quantized, byte bits, float[] interval, float[] centroid) {
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
            int[] destination = new int[dims];
            for (int i = 0; i < numVectors; ++i) {
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(vectors[i], scratch, destination, bit, centroid);

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
            float[] scratch = new float[4096];
            float[] centroid = new float[4096];
            OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(vectorSimilarityFunction);
            int[][] destinations = new int[MINIMUM_MSE_GRID.length][4096];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(
                vector,
                scratch,
                destinations,
                ALL_BITS,
                centroid
            );
            assertEquals(MINIMUM_MSE_GRID.length, results.length);
            assertValidResults(results);
            for (int[] destination : destinations) {
                assertArrayEquals(new int[4096], destination);
            }
            int[] destination = new int[4096];
            for (byte bit : ALL_BITS) {
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(vector, scratch, destination, bit, centroid);
                assertValidResults(result);
                assertArrayEquals(new int[4096], destination);
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
            int[][] destinations = new int[MINIMUM_MSE_GRID.length][1];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(
                vector,
                new float[1],
                destinations,
                ALL_BITS,
                centroid
            );
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
                int[] destination = new int[1];
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(vector, new float[1], destination, bit, centroid);
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
        float[] scratch = new float[dims];
        for (VectorSimilarityFunction vectorSimilarityFunction : VectorSimilarityFunction.values()) {
            // copy the vector to avoid modifying it
            System.arraycopy(vector, 0, copy, 0, dims);
            if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                VectorUtil.l2normalize(copy);
                VectorUtil.l2normalize(centroid);
            }
            OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(vectorSimilarityFunction);
            int[][] destinations = new int[MINIMUM_MSE_GRID.length][dims];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(
                copy,
                scratch,
                destinations,
                ALL_BITS,
                centroid
            );
            assertEquals(MINIMUM_MSE_GRID.length, results.length);
            assertValidResults(results);
            for (int i = 0; i < ALL_BITS.length; i++) {
                assertValidQuantizedRange(destinations[i], ALL_BITS[i]);
            }
            for (byte bit : ALL_BITS) {
                int[] destination = new int[dims];
                System.arraycopy(vector, 0, copy, 0, dims);
                if (vectorSimilarityFunction == VectorSimilarityFunction.COSINE) {
                    VectorUtil.l2normalize(copy);
                    VectorUtil.l2normalize(centroid);
                }
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(copy, scratch, destination, bit, centroid);
                assertValidResults(result);
                assertValidQuantizedRange(destination, bit);
            }
        }
    }

    public void testByteQuantizationQuality() {
        int dims = 16;
        int numVectors = 32;
        byte[][] vectors = new byte[numVectors][];
        float[] centroid = new float[dims];
        for (int i = 0; i < numVectors; ++i) {
            vectors[i] = new byte[dims];
            random().nextBytes(vectors[i]);
            for (int j = 0; j < dims; ++j) {
                centroid[j] += (float) vectors[i][j];
            }
        }
        for (int j = 0; j < dims; ++j) {
            centroid[j] /= numVectors;
        }
        OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(VectorSimilarityFunction.DOT_PRODUCT);
        float[] scratch = new float[dims];
        for (byte bit : ALL_BITS) {
            float eps = (1f / (float) (1 << (bit)));
            // byte range is [-128,127], so max component magnitude after centering is ~256
            // scale eps by range to get a meaningful bound
            float scaledEps = eps * 256;
            int[] destination = new int[dims];
            for (int i = 0; i < numVectors; ++i) {
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(vectors[i], scratch, destination, bit, centroid);
                assertValidResults(result);
                assertValidQuantizedRange(destination, bit);

                // Verify reconstruction quality
                float[] floatVec = new float[dims];
                for (int j = 0; j < dims; j++) {
                    floatVec[j] = vectors[i][j];
                }
                float[] dequantized = deQuantize(
                    destination,
                    bit,
                    new float[] { result.lowerInterval(), result.upperInterval() },
                    centroid
                );
                float mae = 0;
                for (int k = 0; k < dims; ++k) {
                    mae += Math.abs(dequantized[k] - floatVec[k]);
                }
                mae /= dims;
                assertTrue("bits: " + bit + " mae: " + mae + " > scaledEps: " + scaledEps, mae <= scaledEps);
            }
        }
    }

    public void testByteMultiQuantization() {
        int dims = randomIntBetween(2, 256);
        byte[] byteVector = new byte[dims];
        random().nextBytes(byteVector);
        float[] centroid = new float[dims];
        for (int i = 0; i < dims; i++) {
            centroid[i] = randomIntBetween(-128, 127);
        }

        for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
            if (similarityFunction == VectorSimilarityFunction.COSINE) {
                continue;
            }
            OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(similarityFunction);

            float[] residual = new float[dims];
            int[][] destinations = new int[ALL_BITS.length][dims];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(
                byteVector,
                residual,
                destinations,
                ALL_BITS,
                centroid
            );

            assertEquals(ALL_BITS.length, results.length);
            assertValidResults(results);
            for (int i = 0; i < ALL_BITS.length; i++) {
                assertValidQuantizedRange(destinations[i], ALL_BITS[i]);
            }
        }
    }

    public void testByteQuantizationMathematicalConsistency() {
        int dims = randomIntBetween(2, 256);
        byte[] byteVector = new byte[dims];
        random().nextBytes(byteVector);
        float[] centroid = new float[dims];
        for (int i = 0; i < dims; i++) {
            centroid[i] = randomIntBetween(-128, 127);
        }

        for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
            if (similarityFunction == VectorSimilarityFunction.COSINE) {
                continue;
            }
            OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(similarityFunction);

            // Single-level: verify valid results and residual centering
            for (byte bit : ALL_BITS) {
                float[] residual = new float[dims];
                int[] destination = new int[dims];
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(byteVector, residual, destination, bit, centroid);
                assertValidResults(result);
                assertValidQuantizedRange(destination, bit);

                // Verify residual is correctly centered (vector - centroid)
                for (int i = 0; i < dims; i++) {
                    assertEquals(
                        "residual[" + i + "] should be vector - centroid",
                        (float) byteVector[i] - centroid[i],
                        residual[i],
                        1e-5f
                    );
                }
            }

            // Multi-level: verify all results are valid
            float[] residual = new float[dims];
            int[][] destinations = new int[ALL_BITS.length][dims];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(
                byteVector,
                residual,
                destinations,
                ALL_BITS,
                centroid
            );
            assertValidResults(results);
            for (int i = 0; i < ALL_BITS.length; i++) {
                assertValidQuantizedRange(destinations[i], ALL_BITS[i]);
            }
        }
    }

    public void testByteAbusiveEdgeCases() {
        // large zero byte array
        for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
            if (similarityFunction == VectorSimilarityFunction.COSINE) {
                continue;
            }
            byte[] vector = new byte[4096];
            float[] scratch = new float[4096];
            float[] centroid = new float[4096];
            OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(similarityFunction);
            int[][] destinations = new int[MINIMUM_MSE_GRID.length][4096];
            OptimizedScalarQuantizer.QuantizationResult[] results = osq.multiScalarQuantize(
                vector,
                scratch,
                destinations,
                ALL_BITS,
                centroid
            );
            assertEquals(MINIMUM_MSE_GRID.length, results.length);
            assertValidResults(results);
            for (int[] destination : destinations) {
                assertArrayEquals(new int[4096], destination);
            }
            int[] destination = new int[4096];
            for (byte bit : ALL_BITS) {
                OptimizedScalarQuantizer.QuantizationResult result = osq.scalarQuantize(vector, scratch, destination, bit, centroid);
                assertValidResults(result);
                assertArrayEquals(new int[4096], destination);
            }
        }
    }

    static void assertValidQuantizedRange(int[] quantized, byte bits) {
        for (int b : quantized) {
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
