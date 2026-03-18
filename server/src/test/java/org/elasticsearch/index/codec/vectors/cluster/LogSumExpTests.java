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

public class LogSumExpTests extends ESTestCase {
    public void testLogSumExp() {
        float[] x = {1.0f, 2.0f, 3.0f};

        float naiveResult = 0;
        for (float elem : x) {
            naiveResult += (float) Math.exp(elem);
        }
        naiveResult = (float) Math.log(naiveResult);
        assertEquals(LogSumExp.compute(x), naiveResult, 0.0);

        // Example with large numbers prone to overflow in naive implementation
        float[] largeX = {1000.0f, 1000.1f, 1000.2f};
        // A naive implementation like Math.log(Math.exp(1000.0) + ...) would overflow
        assertFalse(Double.isInfinite(LogSumExp.compute(largeX)));

        // Example with very small numbers prone to underflow
        float[] smallX = {-800.0f, -801.0f, -802.0f};
        // A naive implementation would likely underflow to 0 inside the exp() and return -Infinity
        assertFalse(Double.isInfinite(LogSumExp.compute(smallX)));
    }

    public void testSinkhornIterations() {
        int nRows = 5;
        int nCols = 10;
        float[][] x = new float[nRows][nCols];
        float[][] result = new float[nRows][nCols];

        Random random = random();

        for (int iter = 0; iter < 1_000; iter++) {
            for (int i = 0; i < nRows; i++) {
                for (int j = 0; j < nCols; j++) {
                    x[i][j] = (float) random.nextDouble();
                }
            }

            SinkhornIterations.compute(x, 20, 0.5f, result);

            for (int i = 0; i < result.length; i++) {
                float sum = 0;
                for (int j = 0; j < result[i].length; j++) {
                    sum += result[i][j];
                }
                assertEquals(1. / nRows, sum, 1e-5);
            }

            for (int j = 0; j < result[0].length; j++) {
                float sum = 0;
                for (float[] floats : result) {
                    sum += floats[j];
                }
                assertEquals(1. / nCols, sum, 1e-5);
            }
        }

    }

}
