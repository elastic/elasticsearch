/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

/**
 * Provides numerically stable log-sum-exp functions.
 */
public class LogSumExp {

    /**
     * Calculates the log of the sum of the exponentials of the input array elements.
     * This implementation uses the log-sum-exp trick for numerical stability.
     *
     * The formula used is:
     * log(sum(exp(x_i))) = max(x) + log(sum(exp(x_i - max(x))))
     *
     * The max is computed in a <a href="https://www.nowozin.net/sebastian/blog/streaming-log-sum-exp-computation.html">streaming fashion</a>.
     *
     * @param input The input array of double values (log probabilities/values). We assume that it is not null and
     *              that its length is larger than 0.
     * @return The log-sum-exp result.
     */
    public static float compute(float[] input) {
        float maxVal = Float.NEGATIVE_INFINITY;
        float sum = 0.0f;
        for (float v : input) {
            if (v <= maxVal) {
                sum += (float) Math.exp(v - maxVal);
            } else {
                sum *= (float) Math.exp(maxVal - v);
                sum += 1.0f;
                maxVal = v;
            }
        }

        return maxVal + (float) Math.log(sum);
    }

    /**
     * Calculates the log of the sum of the exponentials of the input array elements in the specified column.
     * This implementation uses the log-sum-exp trick for numerical stability.
     *
     * The formula used is:
     * log(sum(exp(x_i))) = max(x) + log(sum(exp(x_i - max(x))))
     *
     * The max is computed in a <a href="https://www.nowozin.net/sebastian/blog/streaming-log-sum-exp-computation.html">streaming fashion</a>.
     *
     * @param input The input 2D array of double values (log probabilities/values). We assume that it is not null,
     *              that its length is larger than 0, and that its sub-arrays are of the same length, larger than 0.
     * @param column The column used for the computation
     * @return The log-sum-exp result.
     */
    public static float compute(float[][] input, int column) {
        float maxVal = Float.NEGATIVE_INFINITY;
        float sum = 0.0f;
        for (float[] floats : input) {
            float v = floats[column];
            if (v <= maxVal) {
                sum += (float) Math.exp(v - maxVal);
            } else {
                sum *= (float) Math.exp(maxVal - v);
                sum += 1.0f;
                maxVal = v;
            }
        }

        return maxVal + (float) Math.log(sum);
    }

    public enum Axis {
        ROWS,
        COLUMNS,
    }

    /**
     * Calculates the log of the sum of the exponentials of the input array elements along the specified direction.
     * This implementation uses the log-sum-exp trick for numerical stability.
     *
     * The formula used is:
     * log(sum(exp(x_i))) = max(x) + log(sum(exp(x_i - max(x))))
     *
     * The max is computed in a <a href="https://www.nowozin.net/sebastian/blog/streaming-log-sum-exp-computation.html">streaming fashion</a>.
     *
     * @param input The input 2D array of double values (log probabilities/values). We assume that it is not null,
     *              that its length is larger than 0, and that its sub-arrays are of the same length, larger than 0.
     * @param axis The direction in the matrix along which to do the computation
     * @param result the array where the result will be written.
     *               If axis == ROWS, result.length == input.length, if axis == COLUMNS, result.length == input[0].length.
     */
    public static void compute(float[][] input, Axis axis, float[] result) {
        if (axis == Axis.ROWS) {
            if (result.length != input.length) {
                throw new IllegalArgumentException("The length of result array is not equal to the input array length");
            }
            for (int i = 0; i < input.length; i++) {
                result[i] = compute(input[i]);
            }
        }
        if (axis == Axis.COLUMNS) {
            int nColumns = input[0].length;
            if (result.length != nColumns) {
                throw new IllegalArgumentException("The length of result array is not equal to the number of columns in the input array");
            }
            for (int j = 0; j < nColumns; j++) {
                result[j] = compute(input, j);
            }
        }
    }
}
