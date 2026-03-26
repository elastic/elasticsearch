/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.util.Random;

public class LogSinkhornTests extends ESTestCase {
    public void testLogSumExp() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        for  (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
        }

        float naiveResult = 0;
        for (float elem : x) {
            naiveResult += (float) Math.pow(2, elem);
        }
        naiveResult = (float) (Math.log(naiveResult) / Math.log(2));
        assertEquals(naiveResult, ESVectorUtil.logSumExpBase2(x), 1e-6);

        // Example with large numbers prone to overflow in naive implementation
        float[] largeX = {1000.0f, 1000.1f, 1000.2f};
        // A naive implementation like Math.log(Math.exp(1000.0) + ...) would overflow
        assertFalse(Double.isInfinite(ESVectorUtil.logSumExpBase2(largeX)));

        // Example with very small numbers prone to underflow
        float[] smallX = {-800.0f, -801.0f, -802.0f};
        // A naive implementation would likely underflow to 0 inside the exp() and return -Infinity
        assertFalse(Double.isInfinite(ESVectorUtil.logSumExpBase2(smallX)));
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
                x[i][j] =  random.nextFloat();
            }
        }

        sinkhornIterations.compute(x, 50, 0.5f, result);

        for (int i = 0; i < result.length; i++) {
            float sum = 0;
            for (int j = 0; j < result[i].length; j++) {
                sum += result[i][j];
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
