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

/**
 * Tests for {@link AshBinaryQuantizer}.
 */
public class AshBinaryQuantizerTests extends ESTestCase {

    public void testBitsPerDimension() {
        AshBinaryQuantizer bq = new AshBinaryQuantizer();
        assertEquals(1, bq.bitsPerDimension());
    }

    public void testSignPreservation() {
        AshBinaryQuantizer bq = new AshBinaryQuantizer();
        float[] input = { 2.5f, -0.3f, 0.0f, -100.0f, 0.001f };
        AshDimQuantizer.SingleQuantizeResult result = bq.encodeOne(input);

        assertEquals(1.0f, result.centeredCode()[0], 0f);
        assertEquals(-1.0f, result.centeredCode()[1], 0f);
        // 0.0 >= 0 is true, so maps to +1
        assertEquals(1.0f, result.centeredCode()[2], 0f);
        assertEquals(-1.0f, result.centeredCode()[3], 0f);
        assertEquals(1.0f, result.centeredCode()[4], 0f);
    }

    public void testNormAlwaysSqrtDims() {
        AshBinaryQuantizer bq = new AshBinaryQuantizer();
        for (int d = 1; d <= 100; d++) {
            float[] input = new float[d];
            for (int j = 0; j < d; j++) {
                input[j] = randomFloat() * 2 - 1;
            }
            AshDimQuantizer.SingleQuantizeResult result = bq.encodeOne(input);
            assertEquals((float) Math.sqrt(d), result.codeNorm(), 1e-5f);
        }
    }

    public void testAllCodesArePlusMinusOne() {
        AshBinaryQuantizer bq = new AshBinaryQuantizer();
        int d = randomIntBetween(10, 500);
        float[] input = new float[d];
        for (int j = 0; j < d; j++) {
            input[j] = (float) (random().nextGaussian());
        }
        AshDimQuantizer.SingleQuantizeResult result = bq.encodeOne(input);
        for (int j = 0; j < d; j++) {
            assertTrue(
                "Expected +1 or -1 but got " + result.centeredCode()[j],
                result.centeredCode()[j] == 1.0f || result.centeredCode()[j] == -1.0f
            );
        }
    }

    public void testEncodeOneMatchesBatch() {
        AshBinaryQuantizer bq = new AshBinaryQuantizer();
        int d = 16;
        float[] input = new float[d];
        for (int j = 0; j < d; j++) {
            input[j] = (float) random().nextGaussian();
        }

        AshDimQuantizer.SingleQuantizeResult single = bq.encodeOne(input);
        AshDimQuantizer.QuantizeResult batch = bq.encode(new float[][] { input });

        assertArrayEquals(single.centeredCode(), batch.centeredCodes()[0], 0f);
        assertEquals(single.codeNorm(), batch.codeNorms()[0], 0f);
    }

    public void testEmptyInput() {
        AshBinaryQuantizer bq = new AshBinaryQuantizer();
        AshDimQuantizer.QuantizeResult result = bq.encode(new float[0][0]);
        assertEquals(0, result.centeredCodes().length);
        assertEquals(0, result.codeNorms().length);
    }

    public void testBatchMultipleVectors() {
        AshBinaryQuantizer bq = new AshBinaryQuantizer();
        int n = 50;
        int d = 32;
        float[][] input = new float[n][d];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < d; j++) {
                input[i][j] = (float) random().nextGaussian();
            }
        }
        AshDimQuantizer.QuantizeResult result = bq.encode(input);
        assertEquals(n, result.centeredCodes().length);
        assertEquals(n, result.codeNorms().length);
        for (int i = 0; i < n; i++) {
            assertEquals((float) Math.sqrt(d), result.codeNorms()[i], 1e-5f);
            assertEquals(d, result.centeredCodes()[i].length);
        }
    }
}
