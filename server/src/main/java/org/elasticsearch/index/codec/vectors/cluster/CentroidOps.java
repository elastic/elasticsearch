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

import java.io.IOException;

/**
 * Encapsulates all vector/centroid-type-specific arithmetic for k-means clustering.
 * <p>
 * Two implementations are provided: {@link FloatOps} for {@code float[]} vectors/centroids
 * and {@link ByteOps} for {@code byte[]} vectors/centroids.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
public sealed interface CentroidOps<V> permits CentroidOps.FloatOps, CentroidOps.ByteOps {

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

    void initCentroid(V centroid, V vector, int dim);

    /**
     * Abstracts accumulation and finalization of centroid updates, parameterized by
     * accumulator type {@code W} and vector type {@code V}.
     * <p>
     * {@link FloatOps} implements this as {@code Accumulator<float[], float[]>} where the
     * accumulator is the centroid itself. {@link ByteOps} implements as {@code Accumulator<int[], byte[]>}
     * using int-precision accumulators to avoid overflow during summation.
     *
     * @param <W> the accumulator array type ({@code float[]} for floats, {@code int[]} for bytes)
     * @param <V> the vector/centroid array type ({@code float[]} or {@code byte[]})
     */
    interface Accumulator<W, V> {
        /**
         * Accumulate a vector into the accumulator: {@code accumulator[d] += vector[d]},
         * widening as needed for the byte path.
         */
        void accumulate(W accumulator, V vector, int dim);

        /**
         * Initialize the accumulator from a vector (first assignment for a cluster).
         * Copies vector values into the accumulator, widening for the byte path.
         */
        void initAccumulator(W accumulator, V vector, int dim);

        /**
         * Divide the accumulator by {@code count} and write the result into {@code centroid}.
         * For float ops, this divides in place. For byte ops, this rounds and clamps to byte range.
         */
        void divideAccumulator(V centroid, W accumulator, float count, int dim);
    }

    /**
     * Computes SGD linear combination: {@code dest[i] = scaleOther * other[i] + scaleDest * dest[i]} where the source is a vector
     * and the destination is always a float accumulator.
     */
    void linearCombination(float scaleOther, V other, float scaleDest, float[] dest);

    /**
     * Computes {@code dest[i] += scale * src[i]}. The source is type V and the destination is always a float accumulator.
     */
    void addScaled(float scale, V src, float[] dest);

    /**
     * Compute the mean centroid of all vectors in the given collection.
     * For float vectors this accumulates directly into a {@code float[]} centroid.
     * For byte vectors this accumulates into {@code int[]} precision and rounds once at the end.
     */
    V computeMeanCentroid(ClusteringVectorValues<V> vectors, int dimension) throws IOException;

    /**
     * Compute {@code diffs[d] = vector[d] - centroid[d]} as floats (for SOAR residuals).
     * Always produces {@code float[]} regardless of vector type, because the SOAR formula
     * operates in float space.
     */
    void computeDiffs(V vector, V centroid, float[] diffs);

    /**
     * Creates a scoped mutation context for SGD-based centroid updates.
     * <p>
     * For {@link FloatOps}, the context operates directly on the centroid arrays (no allocation).
     * For {@link ByteOps}, the context maintains a float-precision shadow array internally.
     * When the context is closed, float shadows are rounded back into the native centroids and released.
     * <p>
     * Usage:
     * <pre>{@code
     * try (var sgd = ops.newMutationContext(centroids, dim)) {
     *     sgd.update(k, learningRate, vec);
     * } // auto-syncs to native and releases float shadow
     * }</pre>
     *
     * @param centroids the centroid array to mutate
     * @param dim the vector dimension
     */
    MutationContext<V> newMutationContext(V[] centroids, int dim);

    /**
     * A scoped, autocloseable context for SGD centroid updates that maintains float-precision
     * state and syncs back to the native centroid representation on close.
     *
     * @param <V> the centroid array type
     */
    interface MutationContext<V> extends AutoCloseable {
        /**
         * Returns the float-precision view of centroid {@code k} for direct mutation.
         * For float centroids this is the centroid itself; for byte centroids it is the float shadow.
         */
        float[] floatCentroid(int k);

        /**
         * Sync the float-precision state back to native centroids.
         * Called automatically by {@link #close()}, but may also be called explicitly
         * between SGD epochs (e.g. before distance computation on byte centroids).
         */
        void syncToNative();

        /**
         * Closes this context, syncing state and releasing any allocated shadow arrays.
         */
        @Override
        void close();
    }

    // ---- Convergence ----

    /**
     * Computes the normalized Frobenius norm between two centroid arrays:
     * {@code sqrt(sum_i ||vecs1[i] - vecs2[i]||^2 / sum_i ||vecs2[i]||^2)}.
     */
    float normalizedFrobeniusNorm(V[] vecs1, V[] vecs2);

    /** Convenience constant for the float ops singleton. */
    CentroidOps<float[]> FLOAT = FloatOps.INSTANCE;

    /** Convenience constant for the byte ops singleton. */
    CentroidOps<byte[]> BYTE = ByteOps.INSTANCE;

    // ---- Implementations ----

    /**
     * {@link CentroidOps} for {@code float[]} vectors and centroids.
     * Delegates to {@link ESVectorUtil} for SIMD-accelerated operations.
     */
    final class FloatOps implements CentroidOps<float[]>, Accumulator<float[], float[]> {

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

        @Override
        public void accumulate(float[] centroid, float[] vector, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] += vector[d];
            }
        }

        @Override
        public void initAccumulator(float[] centroid, float[] vector, int dim) {
            initCentroid(centroid, vector, dim);
        }

        @Override
        public void divideAccumulator(float[] centroid, float[] accumulator, float count, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] = accumulator[d] / count;
            }
        }

        @Override
        public float[] computeMeanCentroid(ClusteringVectorValues<float[]> vectors, int dimension) throws IOException {
            assert vectors.size() > 0 : "cannot compute mean of zero vectors";
            float[] centroid = new float[dimension];
            initAccumulator(centroid, vectors.vectorValue(0), dimension);
            for (int i = 1; i < vectors.size(); i++) {
                accumulate(centroid, vectors.vectorValue(i), dimension);
            }
            divideAccumulator(centroid, centroid, vectors.size(), dimension);
            return centroid;
        }

        @Override
        public void linearCombination(float scaleOther, float[] other, float scaleDest, float[] dest) {
            ESVectorUtil.linearCombination(scaleOther, other, scaleDest, dest);
        }

        @Override
        public void addScaled(float scale, float[] src, float[] dest) {
            ESVectorUtil.linearCombination(scale, src, dest);
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
        public MutationContext<float[]> newMutationContext(float[][] centroids, int dim) {
            return new MutationContext<>() {
                @Override
                public float[] floatCentroid(int k) {
                    return centroids[k];
                }

                @Override
                public void syncToNative() {
                    // no-op: float centroids are mutated in place
                }

                @Override
                public void close() {
                    // no-op
                }
            };
        }
    }

    /**
     * {@link CentroidOps} for {@code byte[]} vectors and centroids.
     * <p>
     * Centroid averaging uses {@code int[]} accumulators via the {@link Accumulator} interface.
     * SGD updates use float shadow arrays (see {@code BalancedASKMeansLocal}, {@code BalancedOTKMeansLocal}).
     */
    final class ByteOps implements CentroidOps<byte[]>, Accumulator<int[], byte[]> {

        public static final ByteOps INSTANCE = new ByteOps();

        private ByteOps() {}

        @Override
        public float squareDistance(byte[] a, byte[] b) {
            return ESVectorUtil.squareDistance(a, b);
        }

        @Override
        public float squareDistance(byte[] a, byte[] b, int offset, int length) {
            return ESVectorUtil.squareDistance(a, b, offset, length);
        }

        @Override
        public void squareDistanceBulk(byte[] query, byte[] c0, byte[] c1, byte[] c2, byte[] c3, int offset, float[] distances) {
            ESVectorUtil.squareDistanceBulk(query, c0, c1, c2, c3, offset, distances);
        }

        @Override
        public void squareDistanceBulk(
            byte[] query,
            int queryOffset,
            int length,
            byte[] c0,
            byte[] c1,
            byte[] c2,
            byte[] c3,
            float[] distances
        ) {
            ESVectorUtil.squareDistanceBulk(query, queryOffset, length, c0, c1, c2, c3, distances);
        }

        @Override
        public float soarDistance(byte[] vector, byte[] centroid, float[] diffs, float soarLambda, float vectorCentroidDist) {
            return ESVectorUtil.soarDistance(vector, centroid, diffs, soarLambda, vectorCentroidDist);
        }

        @Override
        public void soarDistanceBulk(
            byte[] vector,
            byte[] c0,
            byte[] c1,
            byte[] c2,
            byte[] c3,
            float[] diffs,
            float soarLambda,
            float vectorCentroidDist,
            float[] distances
        ) {
            ESVectorUtil.soarDistanceBulk(vector, c0, c1, c2, c3, diffs, soarLambda, vectorCentroidDist, distances);
        }

        @Override
        public float dotProduct(byte[] a, byte[] b) {
            return ESVectorUtil.dotProduct(a, b);
        }

        @Override
        public byte[][] newCentroidArray(int k, int dims) {
            return new byte[k][dims];
        }

        @Override
        public byte[][] newCentroidArrayShallow(int k) {
            return new byte[k][];
        }

        @Override
        public void deepCopy(byte[][] source, byte[][] destination) {
            for (int i = 0; i < source.length; i++) {
                System.arraycopy(source[i], 0, destination[i], 0, source[i].length);
            }
        }

        @Override
        public void arrayCopy(byte[][] src, int srcPos, byte[][] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public int length(byte[] vector) {
            return vector.length;
        }

        @Override
        public void initCentroid(byte[] centroid, byte[] vector, int dim) {
            System.arraycopy(vector, 0, centroid, 0, dim);
        }

        @Override
        public void accumulate(int[] centroid, byte[] vector, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] += vector[d];
            }
        }

        @Override
        public void initAccumulator(int[] centroid, byte[] vector, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] = vector[d];
            }
        }

        @Override
        public void divideAccumulator(byte[] centroid, int[] accumulator, float count, int dim) {
            for (int d = 0; d < dim; d++) {
                // Round the average and clamp to byte range
                centroid[d] = (byte) Math.clamp(Math.round((float) accumulator[d] / count), -128, 127);
            }
        }

        @Override
        public void computeDiffs(byte[] vector, byte[] centroid, float[] diffs) {
            for (int j = 0; j < diffs.length; j++) {
                diffs[j] = vector[j] - centroid[j];
            }
        }

        @Override
        public float normalizedFrobeniusNorm(byte[][] vecs1, byte[][] vecs2) {
            assert vecs1.length == vecs2.length;
            float result = 0;
            float norm2 = 0;
            for (int i = 0; i < vecs1.length; i++) {
                result += squareDistance(vecs1[i], vecs2[i]);
                norm2 += dotProduct(vecs2[i], vecs2[i]);
            }
            return MathUtils.sqrt(result / norm2);
        }

        @Override
        public void linearCombination(float scaleOther, byte[] other, float scaleDest, float[] dest) {
            ESVectorUtil.linearCombination(scaleOther, other, scaleDest, dest);
        }

        @Override
        public void addScaled(float scale, byte[] src, float[] dest) {
            ESVectorUtil.linearCombination(scale, src, dest);
        }

        @Override
        public byte[] computeMeanCentroid(ClusteringVectorValues<byte[]> vectors, int dimension) throws IOException {
            assert vectors.size() > 0 : "cannot compute mean of zero vectors";
            int[] acc = new int[dimension];
            initAccumulator(acc, vectors.vectorValue(0), dimension);
            for (int i = 1; i < vectors.size(); i++) {
                accumulate(acc, vectors.vectorValue(i), dimension);
            }
            byte[] centroid = new byte[dimension];
            divideAccumulator(centroid, acc, vectors.size(), dimension);
            return centroid;
        }

        @Override
        public MutationContext<byte[]> newMutationContext(byte[][] centroids, int dim) {
            // Eager allocation is appropriate here: SGD updates all centroids every iteration
            // (each vector updates its assigned centroid), so all shadows will be accessed.
            // Lazy instantiation would add branch overhead without saving allocations.

            // Allocate float shadow from current byte centroids
            float[][] shadow = new float[centroids.length][];
            for (int i = 0; i < centroids.length; i++) {
                byte[] src = centroids[i];
                float[] dst = new float[src.length];
                for (int j = 0; j < src.length; j++) {
                    dst[j] = src[j];
                }
                shadow[i] = dst;
            }
            return new MutationContext<>() {
                @Override
                public float[] floatCentroid(int k) {
                    return shadow[k];
                }

                @Override
                public void syncToNative() {
                    // Syncs all centroids unconditionally — this is correct because SGD touches
                    // all centroids during each epoch, so all shadows are potentially dirty.
                    for (int k = 0; k < centroids.length; k++) {
                        byte[] byteCentroid = centroids[k];
                        float[] floatShadow = shadow[k];
                        for (int d = 0; d < dim; d++) {
                            byteCentroid[d] = (byte) Math.clamp(Math.round(floatShadow[d]), -128, 127);
                        }
                    }
                }

                @Override
                public void close() {
                    syncToNative();
                }
            };
        }

    }
}
