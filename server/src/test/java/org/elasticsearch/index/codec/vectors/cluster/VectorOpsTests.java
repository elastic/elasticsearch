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

public class VectorOpsTests extends ESTestCase {
    static final float LOG_2 = (float) Math.log(2);

    public void testLogSumExp() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
        }

        assertEquals(logSumExpBase2(x), ESVectorUtil.logSumExpNQT(x), 1e-1);

        // Example with large numbers prone to overflow in naive implementation
        float[] largeX = { 1000.0f, 1000.1f, 1000.2f };
        // A naive implementation like Math.log(Math.exp(1000.0) + ...) would overflow
        assertEquals(logSumExpBase2(largeX), ESVectorUtil.logSumExpNQT(largeX), 1e-1);

        // Example with very small numbers prone to underflow
        float[] smallX = { -800.0f, -801.0f, -802.0f };
        // A naive implementation would likely underflow to 0 inside the exp() and return -Infinity
        assertEquals(logSumExpBase2(smallX), ESVectorUtil.logSumExpNQT(smallX), 1e-1);
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

    public void testLinearCombination() {
        float[] x = new float[19];
        float[] y = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
            y[i] = randomFloat();
        }

        float scaleX = randomFloat();
        float scaleY = randomFloat();

        float[] dest = new float[19];
        linearCombination(scaleX, x, scaleY, y, dest);
        ESVectorUtil.linearCombination(scaleX, x, scaleY, y);
        assertArrayEquals(dest, y, 1e-5f);
    }

    private static void linearCombination(float scaleX, float[] x, float scaleY, float[] y, float[] dest) {
        assert x.length > 0 && x.length == y.length && x.length == dest.length;
        for (int d = 0; d < dest.length; d++) {
            dest[d] = scaleX * x[d] + scaleY * y[d];
        }
    }

    public void testLogSumExpDiff() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        float[] y = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
            y[i] = randomFloat();
        }

        float eps = randomFloat();
        float exactResult = logSumExpBase2Diff(x, y, eps);
        assertEquals(logSumExpBase2Diff(x, y, eps), ESVectorUtil.logSumExpNQTDiff(x, y, eps), 0.1 * exactResult);
    }

    private static float logSumExpBase2Diff(float[] v1, float[] v2, float eps) {
        assert v1.length > 0 && v1.length == v2.length;

        // Uses a <a href="https://www.nowozin.net/sebastian/blog/streaming-log-sum-exp-computation.html">streaming algorithm</a>.
        float maxVal = Float.NEGATIVE_INFINITY;
        float sum = 0.0f;
        for (int i = 0; i < v1.length; i++) {
            float v = (v1[i] - v2[i]) / eps;
            float newMaxVal = Math.max(maxVal, v);
            sum *= (float) Math.pow(2, maxVal - newMaxVal);
            sum += (float) Math.pow(2, v - newMaxVal);
            maxVal = newMaxVal;
        }

        return maxVal + (float) (Math.log(sum) / LOG_2);
    }

    public void testPow2DiffAndScale() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        float[] y = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
            y[i] = randomFloat();
        }

        float a = randomFloat();
        float eps = 1e-1f;

        float[] exactResult = new float[19];
        pow2DiffAndScale(x, y, a, eps, exactResult);
        float[] result = new float[19];
        ESVectorUtil.pow2DiffAndScaleNQT(x, y, a, eps, result);

        assertArrayEqualsPercent(exactResult, result, 0.1f);
    }

    private static void pow2DiffAndScale(float[] v1, float[] v2, float a, float eps, float[] result) {
        assert v1.length > 0 && v1.length == v2.length && v1.length == result.length;

        for (int i = 0; i < v1.length; i++) {
            result[i] = (float) Math.pow(2, (a + v1[i] - v2[i]) / eps);
        }
    }

}
