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

    /** Copy the first {@code dim} elements of {@code vector} into {@code centroid}. */
    void initCentroid(V centroid, V vector, int dim);

    /**
     * Creates a reusable accumulator state for mean-based centroid updates via
     * {@link CentroidAssignment#updateCentroids}.
     * <p>
     * For {@link FloatOps}, accumulation happens directly on the centroid arrays (no extra allocation).
     * For {@link ByteOps}, allocates {@code int[k][dim]} accumulators to avoid overflow during summation.
     * <p>
     * Callers should allocate once and reuse across iterations to avoid repeated allocation.
     *
     * @param centroids the centroid array
     * @param k number of centroids
     * @param dim vector dimension
     */
    AccumulatorState<V> newAccumulatorState(V[] centroids, int k, int dim);

    /**
     * Opaque, reusable state for mean-based centroid accumulation.
     * Encapsulates the accumulator array and type-specific operations (init, accumulate, divide).
     *
     * @param <V> the vector/centroid array type
     */
    interface AccumulatorState<V> {
        /** Initialize accumulator for cluster {@code k} from the given vector (first assignment). */
        void init(int k, V vector, int dim);

        /** Accumulate a vector into cluster {@code k}'s accumulator. */
        void accumulate(int k, V vector, int dim);

        /** Divide accumulator for cluster {@code k} by count and write result into centroids[k]. */
        void divide(V[] centroids, int k, float count, int dim);
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
     * Applies a batch update to a centroid: {@code centroid[i] = scaleSrc * src[i] + scaleCentroid * centroid[i]}.
     * <p>
     * For float centroids, this operates directly on the centroid array.
     * For byte centroids, this loads the centroid into the provided float buffer, applies the
     * linear combination, and writes the result back to the byte centroid.
     *
     * @param scaleSrc      scale factor for the source (batch sum)
     * @param src           the source float array (e.g. accumulated batch sums)
     * @param scaleCentroid scale factor for the existing centroid value
     * @param centroid      the centroid to update in place
     * @param buffer        a reusable float[dim] scratch buffer (used internally by ByteOps; ignored by FloatOps)
     * @param dim           vector dimension
     */
    void blendBatchIntoCentroid(float scaleSrc, float[] src, float scaleCentroid, V centroid, float[] buffer, int dim);

    /**
     * Allocates a scratch buffer for use with {@link #blendBatchIntoCentroid}, or returns {@code null}
     * if no buffer is needed (e.g. for float centroids that are updated in place).
     *
     * @param dim vector dimension
     */
    float[] allocateBlendBuffer(int dim);

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

        private void accumulate(float[] centroid, float[] vector, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] += vector[d];
            }
        }

        private void initAccumulator(float[] centroid, float[] vector, int dim) {
            initCentroid(centroid, vector, dim);
        }

        private void divideAccumulator(float[] centroid, float[] accumulator, float count, int dim) {
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
        public void blendBatchIntoCentroid(float scaleSrc, float[] src, float scaleCentroid, float[] centroid, float[] buffer, int dim) {
            // Float centroids can be updated directly — no load/store needed
            ESVectorUtil.linearCombination(scaleSrc, src, scaleCentroid, centroid);
        }

        @Override
        public float[] allocateBlendBuffer(int dim) {
            return null; // float centroids are updated in place, no scratch buffer needed
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
        public AccumulatorState<float[]> newAccumulatorState(float[][] centroids, int k, int dim) {
            // Float centroids accumulate in place — the centroid array IS the accumulator
            return new AccumulatorState<>() {
                @Override
                public void init(int k, float[] vector, int dim) {
                    System.arraycopy(vector, 0, centroids[k], 0, dim);
                }

                @Override
                public void accumulate(int k, float[] vector, int dim) {
                    float[] centroid = centroids[k];
                    for (int d = 0; d < dim; d++) {
                        centroid[d] += vector[d];
                    }
                }

                @Override
                public void divide(float[][] ignored, int k, float count, int dim) {
                    float[] centroid = centroids[k];
                    for (int d = 0; d < dim; d++) {
                        centroid[d] /= count;
                    }
                }
            };
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
     * Centroid averaging uses {@code int[]} accumulators to avoid overflow during summation.
     * SGD updates use float shadow arrays (see {@code BalancedASKMeansLocal}, {@code BalancedOTKMeansLocal}).
     */
    final class ByteOps implements CentroidOps<byte[]> {

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

        private void accumulate(int[] centroid, byte[] vector, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] += vector[d];
            }
        }

        private void initAccumulator(int[] centroid, byte[] vector, int dim) {
            for (int d = 0; d < dim; d++) {
                centroid[d] = vector[d];
            }
        }

        private void divideAccumulator(byte[] centroid, int[] accumulator, float count, int dim) {
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
        public void blendBatchIntoCentroid(float scaleSrc, float[] src, float scaleCentroid, byte[] centroid, float[] buffer, int dim) {
            // Load byte centroid into float buffer
            for (int d = 0; d < dim; d++) {
                buffer[d] = centroid[d];
            }
            // Apply linear combination in float precision
            ESVectorUtil.linearCombination(scaleSrc, src, scaleCentroid, buffer);
            // Write back to byte centroid with rounding and clamping
            for (int d = 0; d < dim; d++) {
                centroid[d] = (byte) Math.clamp(Math.round(buffer[d]), -128, 127);
            }
        }

        @Override
        public float[] allocateBlendBuffer(int dim) {
            return new float[dim];
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
        public AccumulatorState<byte[]> newAccumulatorState(byte[][] centroids, int k, int dim) {
            int[][] accumulators = new int[k][dim];
            return new AccumulatorState<>() {
                @Override
                public void init(int k, byte[] vector, int dim) {
                    int[] acc = accumulators[k];
                    for (int d = 0; d < dim; d++) {
                        acc[d] = vector[d];
                    }
                }

                @Override
                public void accumulate(int k, byte[] vector, int dim) {
                    int[] acc = accumulators[k];
                    for (int d = 0; d < dim; d++) {
                        acc[d] += vector[d];
                    }
                }

                @Override
                public void divide(byte[][] centroids, int k, float count, int dim) {
                    int[] acc = accumulators[k];
                    byte[] centroid = centroids[k];
                    for (int d = 0; d < dim; d++) {
                        centroid[d] = (byte) Math.clamp(Math.round(acc[d] / count), -128, 127);
                    }
                }
            };
        }

        @Override
        public MutationContext<byte[]> newMutationContext(byte[][] centroids, int dim) {
            // Single reusable float buffer — flushes back to byte[] when switching centroids.
            // This trades ~2x indexing throughput for eliminating the k * dim * 4 byte shadow
            // allocation that would otherwise be needed (e.g. ~40MB at 26K centroids, dim=384).
            // TODO: a pool of N float[] buffers (e.g. N=256, direct-mapped by k % N) could
            // significantly reduce flush/load overhead by keeping hot centroids resident;
            // evaluate in a follow-up if the throughput regression is a concern.
            float[] buffer = new float[dim];
            return new MutationContext<>() {
                int currentK = -1;

                @Override
                public float[] floatCentroid(int k) {
                    if (k != currentK) {
                        // Flush the previously loaded centroid back to byte[]
                        if (currentK >= 0) {
                            flushToNative(centroids[currentK], buffer, dim);
                        }
                        // Load the requested centroid into the float buffer
                        byte[] src = centroids[k];
                        for (int d = 0; d < dim; d++) {
                            buffer[d] = src[d];
                        }
                        currentK = k;
                    }
                    return buffer;
                }

                @Override
                public void syncToNative() {
                    // Flush the currently loaded centroid and force a reload on next access
                    if (currentK >= 0) {
                        flushToNative(centroids[currentK], buffer, dim);
                        currentK = -1;
                    }
                }

                @Override
                public void close() {
                    syncToNative();
                }
            };
        }

        private static void flushToNative(byte[] byteCentroid, float[] floatBuffer, int dim) {
            for (int d = 0; d < dim; d++) {
                byteCentroid[d] = (byte) Math.clamp(Math.round(floatBuffer[d]), -128, 127);
            }
        }

    }
}
