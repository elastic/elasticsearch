/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import java.util.Random;

/**
 * Johnson-Lindenstrauss random projection for dimensionality reduction.
 * Projects vectors from {@code originalDim} dimensions to {@code projectedDim}
 * dimensions while approximately preserving pairwise distances.
 * <p>
 * Uses a Gaussian random matrix scaled by {@code 1/sqrt(projectedDim)}.
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
            throw new IllegalArgumentException(
                "projectedDim [" + projectedDim + "] must be <= originalDim [" + originalDim + "]"
            );
        }
        this.originalDim = originalDim;
        this.projectedDim = projectedDim;
        this.projectionMatrix = buildMatrix(originalDim, projectedDim, seed);
    }

    private static float[][] buildMatrix(int originalDim, int projectedDim, long seed) {
        Random rng = new Random(seed);
        float scale = (float) (1.0 / Math.sqrt(projectedDim));
        float[][] matrix = new float[projectedDim][originalDim];
        for (int i = 0; i < projectedDim; i++) {
            for (int j = 0; j < originalDim; j++) {
                matrix[i][j] = (float) rng.nextGaussian() * scale;
            }
        }
        return matrix;
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
