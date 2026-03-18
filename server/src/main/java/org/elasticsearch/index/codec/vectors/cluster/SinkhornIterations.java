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
 * The Sinkhorn algorithm (aka Iterative Proportional Fitting) finds the closest approximation to the (n x k) input matrix, such that its rows
 * and columns sum to 1/n and 1/k, respectively. In this implementation, we perform the iterations in the log domain for numerical stability.
 */
public class SinkhornIterations {
    private final float[] logRowSums;
    private final float[] logColumnSums;

    SinkhornIterations(int nRows, int nColumns) {
        logRowSums = new float[nRows];
        logColumnSums = new float[nColumns];
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
        int nRows = input.length;
        int nColumns = input[0].length;

        float logRowSumTarget = -eps * (float) Math.log(nRows);
        float logColumnSumTarget = -eps * (float) Math.log(nColumns);

        for (int i = 0; i < iterations; i++) {
            // Update logRowSums (normalize across columns)
            rowVectorSubtractAndNormalize(logColumnSums, input, eps, result);
            LogSumExp.compute(result, LogSumExp.Axis.ROWS,  logRowSums);
            affineTransform(logRowSums, -eps, logRowSumTarget);

            // Update logColumnSums (normalize across rows)
            columnVectorSubtractAndNormalize(logRowSums, input, eps, result);
            LogSumExp.compute(result, LogSumExp.Axis.COLUMNS,  logColumnSums);
            affineTransform(logColumnSums, -eps, logColumnSumTarget);
        }

        combinePartialsAndNormalize(logRowSums, logColumnSums, input, eps, result);
    }

    // Assumes that the length of the input vector is equal to the number of columns fof the input matrix
    private static void rowVectorSubtractAndNormalize(float[] vector, float[][] matrix, float normConstant, float[][] result) {
        int nRows = matrix.length;
        int nColumns = matrix[0].length;

        for (int i = 0; i < nRows; i++) {
            for (int j = 0; j < nColumns; j++) {
                result[i][j] = (vector[j] - matrix[i][j]) / normConstant;
            }
        }
    }

    // Assumes that the length of the input vector is equal to the number of rows fof the input matrix
    private static void columnVectorSubtractAndNormalize(float[] vector, float[][] matrix, float normConstant, float[][] result) {
        int nRows = matrix.length;
        int nColumns = matrix[0].length;

        for (int i = 0; i < nRows; i++) {
            for (int j = 0; j < nColumns; j++) {
                result[i][j] = (vector[i] - matrix[i][j]) / normConstant;
            }
        }
    }

    // Computes v[i] = a * v[i] + b.
    private static void affineTransform(float[] vector, float a, float b) {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = a * vector[i] + b;
        }
    }

    private static void combinePartialsAndNormalize(float[] rowVector, float[] columnvector, float[][] matrix, float normConstant, float[][] result) {
        for (int i = 0; i < rowVector.length; i++) {
            for (int j = 0; j < columnvector.length; j++) {
                result[i][j] = (float) Math.exp((rowVector[i] + columnvector[j] - matrix[i][j]) / normConstant);
            }
        }
    }
}
