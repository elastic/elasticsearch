/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.test.ESTestCase;

import java.util.Random;

public class LogSinkhornTests extends ESTestCase {
    static final float LOG_2 = (float) Math.log(2);

    public void testLogSumExp() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
        }

        float naiveResult = 0;
        for (float elem : x) {
            naiveResult += (float) Math.pow(2, elem);
        }
        naiveResult = (float) (Math.log(naiveResult) / Math.log(2));
        assertEquals(naiveResult, logSumExpBase2(x), 1e-6);

        // Example with large numbers prone to overflow in naive implementation
        float[] largeX = { 1000.0f, 1000.1f, 1000.2f };
        // A naive implementation like Math.log(Math.exp(1000.0) + ...) would overflow
        assertFalse(Double.isInfinite(logSumExpBase2(largeX)));

        // Example with very small numbers prone to underflow
        float[] smallX = { -800.0f, -801.0f, -802.0f };
        // A naive implementation would likely underflow to 0 inside the exp() and return -Infinity
        assertFalse(Double.isInfinite(logSumExpBase2(smallX)));
    }

    private static float logSumExpBase2(float[] vector) {
        assert vector.length > 0;

        // Uses a <a href="https://www.nowozin.net/sebastian/blog/streaming-log-sum-exp-computation.html">streaming algorithm</a>.
        float maxVal = Float.NEGATIVE_INFINITY;
        float sum = 0.0f;
        for (float v : vector) {
            float newMaxVal = Math.max(maxVal, v);
            sum *= (float) Math.pow(2, maxVal - newMaxVal);
            sum += (float) Math.pow(2, v - newMaxVal);
            maxVal = newMaxVal;
        }

        return maxVal + (float) (Math.log(sum) / LOG_2);
    }

    public void testSinkhornIterations() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        int nRows = 9;
        int nCols = 17;
        float[][] x = new float[nRows][nCols];
        float[][] result = new float[nRows][nCols];

        Random random = random();

        SinkhornIterations sinkhornIterations = new SinkhornIterations(nRows, nCols);

        for (int i = 0; i < nRows; i++) {
            for (int j = 0; j < nCols; j++) {
                x[i][j] = random.nextFloat();
            }
        }

        sinkhornIterations.compute(x, 50, 0.5f, result);

        for (float[] values : result) {
            float sum = 0;
            for (float v : values) {
                sum += v;
            }
            assertEquals(1. / nRows, sum, 0.12);
        }

        for (int j = 0; j < result[0].length; j++) {
            float sum = 0;
            for (float[] floats : result) {
                sum += floats[j];
            }
            assertEquals(1. / nCols, sum, 0.12);
        }
    }
}
