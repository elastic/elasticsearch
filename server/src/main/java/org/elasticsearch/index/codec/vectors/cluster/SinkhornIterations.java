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
import org.elasticsearch.simdvec.MathUtils;

/**
 * The Sinkhorn algorithm (aka Iterative Proportional Fitting) solves the optimal transport problem with entropic regularization.
 * Given an (n x k) cost matrix C, the problem is:
 * min_P sum_i sum_j (P[i][j] * C[i][j] + eps * C[i][j] * log2(C[i][j]))
 * such that the rows and columns of P sum to 1/n and 1/k, respectively.
 * In this implementation, we start from the base-2 logarithm/exponentiation instead of the natural one. Then, we use
 * <a href="https://arxiv.org/pdf/2206.08957">Not-Quite-Trascendental</a> (NQT) approximations to pow2 and log2. This gives a massive
 * boost in performance.
 */
public class SinkhornIterations {
    private final float[] logRowSums;
    private final float[] logColumnSums;
    private final float[][] transposedInput;
    private final float rawLogRowSumTarget;
    private final float rawLogColumnSumTarget;

    SinkhornIterations(int nRows, int nColumns) {
        logRowSums = new float[nRows];
        logColumnSums = new float[nColumns];
        transposedInput = new float[nColumns][nRows];
        rawLogRowSumTarget = MathUtils.log2NQT(nRows);
        rawLogColumnSumTarget = MathUtils.log2NQT(nColumns);
    }

    /**
     * Run the Sinkhorn algorithm.
     * @param input The input 2D array of double values (log probabilities/values). We assume that it is not null,
     *              that its length is larger than 0, and that its sub-arrays are of the same length, larger than 0.
     * @param iterations the number of iterations to perform, must be positive.
     * @param eps the amount of entropic regularization (the temperature coefficient of the LogSumExp approximation to the min)
     * @param result the solution with rows summing to 1/n and columns summing to 1/k.
     */
    public void compute(float[][] input, int iterations, float eps, float[][] result) {
        transpose(input, transposedInput);

        float logRowSumTarget = -eps * rawLogRowSumTarget;
        float logColumnSumTarget = -eps * rawLogColumnSumTarget;

        // The order of the updates does matter here. The axis that is most important in our calculation should always go last.
        for (int i = 0; i < iterations; i++) {
            // Update logRowSums (normalize across columns)
            logSumExpNQT(logColumnSums, input, eps, -eps, logRowSumTarget, logRowSums);

            // Update logColumnSums (normalize across rows)
            logSumExpNQT(logRowSums, transposedInput, eps, -eps, logColumnSumTarget, logColumnSums);
        }

        createTransportPlan(logRowSums, logColumnSums, input, eps, result);
    }

    private static void transpose(float[][] input, float[][] output) {
        int nRows = input.length;
        int nColumns = input[0].length;
        for (int i = 0; i < nRows; i++) {
            for (int j = 0; j < nColumns; j++) {
                output[j][i] = input[i][j];
            }
        }
    }

    // Creates the Sinkhorn plan:
    // result = np.pow(2, (rowVector + columnVector.T - matrix) / eps)
    // Instead of base-2 logarithms and powers, it uses NQT.
    private static void createTransportPlan(float[] rowVector, float[] columnvector, float[][] matrix, float eps, float[][] result) {
        for (int i = 0; i < rowVector.length; i++) {
            ESVectorUtil.pow2DiffAndScaleNQT(columnvector, matrix[i], rowVector[i], eps, result[i]);
        }
    }

    // Calculates the Sinkhorn update:
    // For each row i: result[i] = a * log2(sum_j(pow(2, (vector[j] - matrix[i][j]) / eps))) + b
    // Instead of base-2 logarithms and powers, it uses NQT.
    public static void logSumExpNQT(float[] vector, float[][] matrix, float eps, float a, float b, float[] result) {
        int nRows = matrix.length;
        for (int i = 0; i < nRows; i++) {
            result[i] = a * ESVectorUtil.logSumExpNQTDiff(vector, matrix[i], eps) + b;
        }
    }
}
