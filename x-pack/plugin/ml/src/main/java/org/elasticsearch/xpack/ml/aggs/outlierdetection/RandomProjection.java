/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import java.util.Random;

/**
 * Random orthogonal projection for dimensionality reduction.
 * Projects vectors from {@code originalDim} dimensions to {@code projectedDim}
 * dimensions while approximately preserving pairwise distances.
 * <p>
 * Generates a Gaussian random matrix and orthogonalises the rows via
 * Modified Gram-Schmidt, then scales by {@code sqrt(originalDim / projectedDim)}.
 * Orthogonal rows give lower-variance distance estimates than i.i.d. Gaussian
 * (see Choromanski et al., "The Unreasonable Effectiveness of Structured Random
 * Orthogonal Embeddings", 2017).
 * <p>
 * A shared seed ensures all shards use the same projection matrix.
 */
public class RandomProjection {

    private final int originalDim;
    private final int projectedDim;
    private final float[][] projectionMatrix;

    /**
     * @param originalDim  the original vector dimensionality
     * @param projectedDim the target (reduced) dimensionality
     * @param seed         random seed — must be identical across all shards
     */
    public RandomProjection(int originalDim, int projectedDim, long seed) {
        if (projectedDim > originalDim) {
            throw new IllegalArgumentException("projectedDim [" + projectedDim + "] must be <= originalDim [" + originalDim + "]");
        }
        this.originalDim = originalDim;
        this.projectedDim = projectedDim;
        this.projectionMatrix = buildOrthogonalMatrix(originalDim, projectedDim, seed);
    }

    /**
     * Build a random orthogonal projection matrix using Modified Gram-Schmidt.
     * Each row is a unit vector in R^originalDim, mutually orthogonal, then
     * scaled so that squared distances are approximately preserved.
     */
    private static float[][] buildOrthogonalMatrix(int originalDim, int projectedDim, long seed) {
        Random rng = new Random(seed);
        float[][] matrix = new float[projectedDim][originalDim];

        // Generate random Gaussian rows
        for (int i = 0; i < projectedDim; i++) {
            for (int j = 0; j < originalDim; j++) {
                matrix[i][j] = (float) rng.nextGaussian();
            }
        }

        // Modified Gram-Schmidt orthogonalisation
        for (int i = 0; i < projectedDim; i++) {
            // Subtract projections onto all previous rows
            for (int k = 0; k < i; k++) {
                float dot = dotProduct(matrix[i], matrix[k]);
                for (int j = 0; j < originalDim; j++) {
                    matrix[i][j] -= dot * matrix[k][j];
                }
            }
            // Normalise to unit length
            float norm = norm(matrix[i]);
            if (norm < 1e-10f) {
                // Degenerate row — regenerate (extremely unlikely for Gaussian)
                for (int j = 0; j < originalDim; j++) {
                    matrix[i][j] = (float) rng.nextGaussian();
                }
                // Re-run orthogonalisation for this row
                i--;
                continue;
            }
            for (int j = 0; j < originalDim; j++) {
                matrix[i][j] /= norm;
            }
        }

        // Scale so that E[||Px||^2] ≈ ||x||^2: multiply by sqrt(originalDim / projectedDim)
        float scale = (float) Math.sqrt((double) originalDim / projectedDim);
        for (int i = 0; i < projectedDim; i++) {
            for (int j = 0; j < originalDim; j++) {
                matrix[i][j] *= scale;
            }
        }

        return matrix;
    }

    private static float dotProduct(float[] a, float[] b) {
        float sum = 0f;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    private static float norm(float[] v) {
        return (float) Math.sqrt(dotProduct(v, v));
    }

    /**
     * Project a single vector from originalDim to projectedDim.
     *
     * @param vector input vector of length {@code originalDim}
     * @return projected vector of length {@code projectedDim}
     */
    public float[] project(float[] vector) {
        assert vector.length == originalDim : "expected dim " + originalDim + " but got " + vector.length;
        float[] result = new float[projectedDim];
        for (int i = 0; i < projectedDim; i++) {
            float dot = 0f;
            float[] row = projectionMatrix[i];
            for (int j = 0; j < originalDim; j++) {
                dot += row[j] * vector[j];
            }
            result[i] = dot;
        }
        return result;
    }

    /**
     * Project a batch of vectors.
     *
     * @param vectors array of vectors, each of length {@code originalDim}
     * @return array of projected vectors, each of length {@code projectedDim}
     */
    public float[][] projectBatch(float[][] vectors) {
        float[][] results = new float[vectors.length][];
        for (int v = 0; v < vectors.length; v++) {
            results[v] = project(vectors[v]);
        }
        return results;
    }

    public int getOriginalDim() {
        return originalDim;
    }

    public int getProjectedDim() {
        return projectedDim;
    }
}
