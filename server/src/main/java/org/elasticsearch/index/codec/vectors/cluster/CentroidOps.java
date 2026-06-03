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
 * Encapsulates all vector/centroid-type-specific arithmetic for k-means clustering.
 * <p>
 * Currently provides {@link FloatOps} for {@code float[]} vectors/centroids.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]})
 */
public sealed interface CentroidOps<V> permits CentroidOps.FloatOps {

    // ---- Distance operations ----

    /** Squared Euclidean distance between two vectors. */
    float squareDistance(V a, V b);

    /** Squared Euclidean distance over a sub-range {@code [offset, offset+length)}. */
    float squareDistance(V a, V b, int offset, int length);

    /**
     * Compute squared distances from {@code query} to four centroids in bulk.
     * Results are written into {@code distances[offset..offset+3]}.
     */
    void squareDistanceBulk(V query, V c0, V c1, V c2, V c3, int offset, float[] distances);

    /**
     * Compute squared distances from a sub-range of {@code query} to four centroids in bulk.
     * {@code queryOffset} and {@code length} define the range within each vector.
     */
    void squareDistanceBulk(V query, int queryOffset, int length, V c0, V c1, V c2, V c3, float[] distances);

    /**
     * SOAR distance: {@code ||x-c||^2 + lambda * ((x-c1)^T (x-c))^2 / ||x-c1||^2}.
     *
     * @param vector             the query vector x
     * @param centroid           the candidate centroid c
     * @param diffs              precomputed {@code x - c1} (primary centroid residual)
     * @param soarLambda         lambda weight
     * @param vectorCentroidDist precomputed {@code ||x - c1||^2}
     */
    float soarDistance(V vector, V centroid, float[] diffs, float soarLambda, float vectorCentroidDist);

    /** Bulk SOAR distance to four candidate centroids. */
    void soarDistanceBulk(V vector, V c0, V c1, V c2, V c3, float[] diffs, float soarLambda, float vectorCentroidDist, float[] distances);

    /** Dot product between two vectors (used for Frobenius norm computation). */
    float dotProduct(V a, V b);

    // ---- Centroid lifecycle ----

    /** Allocate a fresh zero-filled centroid of the given dimension. */
    V newCentroid(int dims);

    /** Allocate a 2D centroid array of shape {@code [k][dims]}. */
    V[] newCentroidArray(int k, int dims);

    /** Allocate a 1D centroid array of length {@code k} with null inner elements. */
    V[] newCentroidArrayShallow(int k);

    /** Element-wise deep copy from {@code source} to {@code destination}. */
    void deepCopy(V[] source, V[] destination);

    /** Copy elements of the centroid array ({@code System.arraycopy} semantics). */
    void arrayCopy(V[] src, int srcPos, V[] dest, int destPos, int length);

    /** Returns the length (dimension) of the vector. */
    int length(V vector);

    // ---- Centroid update operations ----

    /**
     * Copy the contents of {@code vector} into {@code centroid} (first assignment).
     * Equivalent to {@code System.arraycopy(vector, 0, centroid, 0, dim)}.
     */
    void initCentroid(V centroid, V vector, int dim);

    /**
     * Compute {@code diffs[d] = vector[d] - centroid[d]} as floats (for SOAR residuals).
     * Always produces {@code float[]} regardless of vector type, because the SOAR formula
     * operates in float space.
     */
    void computeDiffs(V vector, V centroid, float[] diffs);

    /**
     * Convert centroids to {@code float[][]} for use with float-only subsystems (e.g. {@link NeighborHood}).
     * For {@link FloatOps} this is a no-op cast.
     */
    float[][] toFloatCentroids(V[] centroids);

    // ---- Convergence ----

    /**
     * Computes the normalized Frobenius norm between two centroid arrays:
     * {@code sqrt(sum_i ||vecs1[i] - vecs2[i]||^2 / sum_i ||vecs2[i]||^2)}.
     */
    float normalizedFrobeniusNorm(V[] vecs1, V[] vecs2);

    /** Convenience constant for the float ops singleton. */
    CentroidOps<float[]> FLOAT = FloatOps.INSTANCE;

    // ---- Implementations ----

    /**
     * {@link CentroidOps} for {@code float[]} vectors and centroids.
     * Delegates to {@link ESVectorUtil} for SIMD-accelerated operations.
     */
    final class FloatOps implements CentroidOps<float[]> {

        public static final FloatOps INSTANCE = new FloatOps();

        private FloatOps() {}

        @Override
        public float squareDistance(float[] a, float[] b) {
            return ESVectorUtil.squareDistance(a, b);
        }

        @Override
        public float squareDistance(float[] a, float[] b, int offset, int length) {
            return ESVectorUtil.squareDistance(a, b, offset, length);
        }

        @Override
        public void squareDistanceBulk(float[] query, float[] c0, float[] c1, float[] c2, float[] c3, int offset, float[] distances) {
            ESVectorUtil.squareDistanceBulk(query, c0, c1, c2, c3, offset, distances);
        }

        @Override
        public void squareDistanceBulk(
            float[] query,
            int queryOffset,
            int length,
            float[] c0,
            float[] c1,
            float[] c2,
            float[] c3,
            float[] distances
        ) {
            ESVectorUtil.squareDistanceBulk(query, queryOffset, length, c0, c1, c2, c3, distances);
        }

        @Override
        public float soarDistance(float[] vector, float[] centroid, float[] diffs, float soarLambda, float vectorCentroidDist) {
            return ESVectorUtil.soarDistance(vector, centroid, diffs, soarLambda, vectorCentroidDist);
        }

        @Override
        public void soarDistanceBulk(
            float[] vector,
            float[] c0,
            float[] c1,
            float[] c2,
            float[] c3,
            float[] diffs,
            float soarLambda,
            float vectorCentroidDist,
            float[] distances
        ) {
            ESVectorUtil.soarDistanceBulk(vector, c0, c1, c2, c3, diffs, soarLambda, vectorCentroidDist, distances);
        }

        @Override
        public float dotProduct(float[] a, float[] b) {
            return ESVectorUtil.dotProduct(a, b);
        }

        @Override
        public float[] newCentroid(int dims) {
            return new float[dims];
        }

        @Override
        public float[][] newCentroidArray(int k, int dims) {
            float[][] result = new float[k][];
            for (int i = 0; i < k; i++) {
                result[i] = new float[dims];
            }
            return result;
        }

        @Override
        public float[][] newCentroidArrayShallow(int k) {
            return new float[k][];
        }

        @Override
        public void deepCopy(float[][] source, float[][] destination) {
            for (int i = 0; i < source.length; i++) {
                System.arraycopy(source[i], 0, destination[i], 0, source[i].length);
            }
        }

        @Override
        public void arrayCopy(float[][] src, int srcPos, float[][] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public int length(float[] vector) {
            return vector.length;
        }

        @Override
        public void initCentroid(float[] centroid, float[] vector, int dim) {
            System.arraycopy(vector, 0, centroid, 0, dim);
        }

        /**
         * Accumulate a vector into a centroid: {@code centroid[d] += vector[d]}.
         * This is a float-only operation; byte centroids use {@code int[]} accumulators
         * in {@code CentroidAssignment.updateCentroidsByte()} to avoid overflow.
         */
        void accumulate(float[] centroid, float[] vector, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] += vector[d];
            }
        }

        /**
         * Divide each element of the centroid by {@code count}: {@code centroid[d] /= count}.
         * Float-only; byte centroids divide from {@code int[]} accumulators and round once.
         */
        void divide(float[] centroid, float count, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] /= count;
            }
        }

        /**
         * SGD linear combination: {@code dest[d] += scale * src[d]}.
         * Float-only; byte SGD operates on float shadow arrays via {@code ESVectorUtil}.
         */
        void linearCombination(float scale, float[] src, float[] dest) {
            ESVectorUtil.linearCombination(scale, src, dest);
        }

        /**
         * SGD linear combination: {@code dest[d] = scaleOther * other[d] + scaleDest * dest[d]}.
         * Float-only; byte SGD operates on float shadow arrays via {@code ESVectorUtil}.
         */
        void linearCombination(float scaleOther, float[] other, float scaleDest, float[] dest) {
            ESVectorUtil.linearCombination(scaleOther, other, scaleDest, dest);
        }

        @Override
        public void computeDiffs(float[] vector, float[] centroid, float[] diffs) {
            for (int j = 0; j < diffs.length; j++) {
                diffs[j] = vector[j] - centroid[j];
            }
        }

        @Override
        public float normalizedFrobeniusNorm(float[][] vecs1, float[][] vecs2) {
            assert vecs1.length == vecs2.length;
            float result = 0;
            float norm2 = 0;
            for (int i = 0; i < vecs1.length; i++) {
                result += ESVectorUtil.squareDistance(vecs1[i], vecs2[i]);
                norm2 += ESVectorUtil.dotProduct(vecs2[i], vecs2[i]);
            }
            return MathUtils.sqrt(result / norm2);
        }

        @Override
        public float[][] toFloatCentroids(float[][] centroids) {
            return centroids;
        }
    }
}
