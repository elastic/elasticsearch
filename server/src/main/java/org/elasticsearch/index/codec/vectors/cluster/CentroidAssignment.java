/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntUnaryOperator;

/**
 * Generic centroid assignment and update operations for k-means clustering.
 * <p>
 * These methods were originally instance methods on {@code ClusteringFloatVectorValues}
 * but have been extracted here so they can operate generically over any vector/centroid type.
 */
public final class CentroidAssignment {

    private static final int PREFIX_MIN_DIMENSIONS = 128;
    private static final float PREFIX_LENGTH_RATIO = 0.5f;
    private static final int PREFIX_MULTIPLE = 64;
    private static final int PREFIX_TOPK_SIZE = 4;

    private CentroidAssignment() {}

    /**
     * Find the closest centroid for a batch of contiguous vectors, considering all centroids.
     *
     * @param vectors         the vector values
     * @param ops             centroid operations for the vector type
     * @param startOrd        the first vector ordinal (inclusive) to process
     * @param endOrd          the last vector ordinal (exclusive) to process
     * @param centroids       the centroid vectors to compare against
     * @param ordTranslator   translate the vector ord to the position of the vector on the result array
     * @param centroidChanged a bitset tracking which centroids had assignments change
     * @param results         input/output array indexed by document ordinal
     * @return {@code true} if any assignment changed
     */
    static <V> boolean bestCentroids(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        int startOrd,
        int endOrd,
        V[] centroids,
        IntUnaryOperator ordTranslator,
        FixedBitSet centroidChanged,
        int[] results
    ) throws IOException {
        final float[] distances = new float[4];
        final PrefixScratch prefixScratch = maybeCreatePrefixScratch(centroids.length, vectors.dimension());
        boolean changed = false;
        for (int i = startOrd; i < endOrd; i++) {
            V vector = vectors.vectorValue(i);
            final int translatedOrd = ordTranslator.applyAsInt(i);
            final int assignment = results[translatedOrd];
            final int bestCentroid = computeBestCentroid(vector, centroids, distances, prefixScratch, ops);
            if (bestCentroid != assignment) {
                if (assignment != -1) {
                    centroidChanged.set(assignment);
                }
                centroidChanged.set(bestCentroid);
                changed = true;
                results[translatedOrd] = bestCentroid;
            }
        }
        return changed;
    }

    /**
     * Find the closest centroid for a batch of contiguous vectors, restricting the search to each
     * vector's current centroid and its pre-computed neighborhood of nearby centroids.
     */
    static <V> boolean bestCentroidsFromNeighbours(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        int startOrd,
        int endOrd,
        V[] centroids,
        IntUnaryOperator ordTranslator,
        FixedBitSet centroidChanged,
        NeighborHood[] neighborhoods,
        int[] results
    ) throws IOException {
        final float[] distances = new float[4];
        final PrefixScratch prefixScratch = maybeCreatePrefixScratch(centroids.length, vectors.dimension());
        boolean changed = false;
        for (int i = startOrd; i < endOrd; i++) {
            V vector = vectors.vectorValue(i);
            final int translatedOrd = ordTranslator.applyAsInt(i);
            final int assignment = results[translatedOrd];
            assert assignment != -1 : "vector is not assigned to any cluster: ord=" + translatedOrd;
            final int bestCentroid = computeBestCentroidFromNeighbours(
                vector,
                centroids,
                assignment,
                neighborhoods[assignment],
                distances,
                prefixScratch,
                ops
            );
            if (bestCentroid != assignment) {
                centroidChanged.set(assignment);
                centroidChanged.set(bestCentroid);
                changed = true;
                results[translatedOrd] = bestCentroid;
            }
        }
        return changed;
    }

    /**
     * Recompute centroid positions as the mean of their assigned vectors.
     * <p>
     * The {@code accumulatorState} encapsulates type-specific accumulation logic:
     * for float centroids it accumulates in place; for byte centroids it uses int-precision
     * accumulators internally and rounds back on divide.
     *
     * @param accumulatorState reusable state from {@link CentroidOps#newAccumulatorState}.
     *                         Callers should allocate once and reuse across iterations.
     */
    static <V> void updateCentroids(
        ClusteringVectorValues<V> vectors,
        V[] centroids,
        IntUnaryOperator ordTranslator,
        FixedBitSet[] centroidChangedSlices,
        int[] centroidCounts,
        int[] assignments,
        CentroidOps.AccumulatorState<V> accumulatorState
    ) throws IOException {
        Arrays.fill(centroidCounts, 0);
        FixedBitSet centroidChanged = centroidChangedSlices[0];
        for (int j = 1; j < centroidChangedSlices.length; j++) {
            centroidChanged.or(centroidChangedSlices[j]);
        }
        int dim = vectors.dimension();

        for (int idx = 0; idx < vectors.size(); idx++) {
            final int assignment = assignments[ordTranslator.applyAsInt(idx)];
            if (centroidChanged.get(assignment)) {
                V vector = vectors.vectorValue(idx);
                if (centroidCounts[assignment]++ == 0) {
                    accumulatorState.init(assignment, vector, dim);
                } else {
                    accumulatorState.accumulate(assignment, vector, dim);
                }
            }
        }

        for (int clusterIdx = 0; clusterIdx < centroids.length; clusterIdx++) {
            if (centroidChanged.get(clusterIdx)) {
                float count = (float) centroidCounts[clusterIdx];
                if (count > 0) {
                    accumulatorState.divide(centroids, clusterIdx, count, dim);
                }
            }
        }
    }

    /**
     * Compute the squared distances between a batch of contiguous vectors and all centroids.
     */
    static <V> void computeSquaredDistances(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        int startOrd,
        int endOrd,
        V[] centroids,
        float[][] squaredDistances
    ) throws IOException {
        for (int i = startOrd; i < endOrd; i++) {
            V vector = vectors.vectorValue(i);
            computeSquaredDistances(vector, centroids, squaredDistances[i], ops);
        }
    }

    /**
     * Compute the squared distances between a batch of contiguous vectors and all centroids,
     * restricting the search to each vector's current centroid and its neighborhood.
     */
    static <V> void computeSquaredDistancesFromNeighbors(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        int startOrd,
        int endOrd,
        V[] centroids,
        IntUnaryOperator assigner,
        NeighborHood[] neighborhoods,
        float[][] squaredDistances
    ) throws IOException {
        for (int i = startOrd; i < endOrd; i++) {
            V vector = vectors.vectorValue(i);
            int bestCentroid = assigner.applyAsInt(i);

            float[] ordDists = squaredDistances[i];
            ordDists[0] = ops.squareDistance(vector, centroids[bestCentroid]);

            int[] neighbors = neighborhoods[bestCentroid].neighbors();
            int j = 0;
            for (; j < neighbors.length - 3; j += 4) {
                ops.squareDistanceBulk(
                    vector,
                    centroids[neighbors[j]],
                    centroids[neighbors[j + 1]],
                    centroids[neighbors[j + 2]],
                    centroids[neighbors[j + 3]],
                    j + 1,
                    ordDists
                );
            }
            for (; j < neighbors.length; j++) {
                ordDists[j + 1] = ops.squareDistance(vector, centroids[neighbors[j]]);
            }
        }
    }

    // ---- Private helpers ----

    private static PrefixScratch maybeCreatePrefixScratch(int numCentroids, int dims) {
        return dims >= (PREFIX_MIN_DIMENSIONS * 2) && numCentroids > PREFIX_TOPK_SIZE * 2 ? new PrefixScratch(prefixLength(dims)) : null;
    }

    private static <V> int computeBestCentroid(
        V vector,
        V[] centroids,
        float[] distances,
        PrefixScratch prefixScratch,
        CentroidOps<V> ops
    ) {
        if (prefixScratch != null) {
            return computeBestCentroidPrefix(vector, centroids, distances, prefixScratch, ops);
        }
        final int limit = centroids.length - 3;
        int bestCentroidOffset = 0;
        float minDsq = Float.MAX_VALUE;
        int i = 0;
        for (; i < limit; i += 4) {
            ops.squareDistanceBulk(vector, centroids[i], centroids[i + 1], centroids[i + 2], centroids[i + 3], 0, distances);
            for (int j = 0; j < distances.length; j++) {
                float dsq = distances[j];
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = i + j;
                }
            }
        }
        for (; i < centroids.length; i++) {
            float dsq = ops.squareDistance(vector, centroids[i]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = i;
            }
        }
        return bestCentroidOffset;
    }

    private static <V> void computeSquaredDistances(V vector, V[] centroids, float[] distances, CentroidOps<V> ops) {
        final int limit = centroids.length - 3;
        int i = 0;
        for (; i < limit; i += 4) {
            ops.squareDistanceBulk(vector, centroids[i], centroids[i + 1], centroids[i + 2], centroids[i + 3], i, distances);
        }
        for (; i < centroids.length; i++) {
            distances[i] = ops.squareDistance(vector, centroids[i]);
        }
    }

    private static <V> int computeBestCentroidFromNeighbours(
        V vector,
        V[] centroids,
        int centroidIdx,
        NeighborHood neighborhood,
        float[] distances,
        PrefixScratch prefixScratch,
        CentroidOps<V> ops
    ) {
        if (prefixScratch != null) {
            return computeBestCentroidFromNeighboursPrefix(vector, centroids, distances, centroidIdx, neighborhood, prefixScratch, ops);
        }
        final int limit = neighborhood.neighbors().length - 3;
        int bestCentroidOffset = centroidIdx;
        assert centroidIdx >= 0 && centroidIdx < centroids.length;
        float minDsq = ops.squareDistance(vector, centroids[centroidIdx]);
        int i = 0;
        for (; i < limit; i += 4) {
            if (minDsq < neighborhood.maxIntraDistance()) {
                return bestCentroidOffset;
            }
            ops.squareDistanceBulk(
                vector,
                centroids[neighborhood.neighbors()[i]],
                centroids[neighborhood.neighbors()[i + 1]],
                centroids[neighborhood.neighbors()[i + 2]],
                centroids[neighborhood.neighbors()[i + 3]],
                0,
                distances
            );
            for (int j = 0; j < distances.length; j++) {
                float dsq = distances[j];
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = neighborhood.neighbors()[i + j];
                }
            }
        }
        for (; i < neighborhood.neighbors().length; i++) {
            if (minDsq < neighborhood.maxIntraDistance()) {
                return bestCentroidOffset;
            }
            int offset = neighborhood.neighbors()[i];
            assert offset >= 0 && offset < centroids.length : "Invalid neighbor offset: " + offset;
            float dsq = ops.squareDistance(vector, centroids[offset]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = offset;
            }
        }
        return bestCentroidOffset;
    }

    private static <V> int computeBestCentroidFromNeighboursPrefix(
        V vector,
        V[] centroids,
        float[] distances,
        int centroidIdx,
        NeighborHood neighborhood,
        PrefixScratch scratch,
        CentroidOps<V> ops
    ) {
        final int dims = ops.length(vector);
        final int prefixLength = scratch.prefixLength;
        final int suffixLength = dims - prefixLength;
        int bestCentroidOffset = centroidIdx;
        assert centroidIdx >= 0 && centroidIdx < centroids.length;
        float bestDistance = ops.squareDistance(vector, centroids[centroidIdx]);
        if (bestDistance < neighborhood.maxIntraDistance()) {
            return bestCentroidOffset;
        }

        final int[] neighbors = neighborhood.neighbors();
        scratch.reset();
        int limit = neighbors.length - 3;
        int i = 0;
        for (; i < limit; i += 4) {
            ops.squareDistanceBulk(
                vector,
                0,
                prefixLength,
                centroids[neighbors[i]],
                centroids[neighbors[i + 1]],
                centroids[neighbors[i + 2]],
                centroids[neighbors[i + 3]],
                distances
            );
            for (int k = 0; k < distances.length; k++) {
                scratch.add(distances[k], neighbors[i + k]);
            }
        }
        for (; i < neighbors.length; i++) {
            int offset = neighbors[i];
            assert offset >= 0 && offset < centroids.length : "Invalid neighbor offset: " + offset;
            float prefixDistance = ops.squareDistance(vector, centroids[offset], 0, prefixLength);
            scratch.add(prefixDistance, offset);
        }

        final int topLimit = Math.min(PREFIX_TOPK_SIZE, neighbors.length);
        int j = 0;
        for (; j + 3 < topLimit; j += 4) {
            ops.squareDistanceBulk(
                vector,
                prefixLength,
                suffixLength,
                centroids[scratch.topPrefixIds[j]],
                centroids[scratch.topPrefixIds[j + 1]],
                centroids[scratch.topPrefixIds[j + 2]],
                centroids[scratch.topPrefixIds[j + 3]],
                distances
            );
            for (int k = 0; k < 4; k++) {
                int centroidOrd = scratch.topPrefixIds[j + k];
                float fullDistance = scratch.topPrefixDistances[j + k] + distances[k];
                if (fullDistance < bestDistance) {
                    bestDistance = fullDistance;
                    bestCentroidOffset = centroidOrd;
                }
            }
        }
        assert j >= topLimit;
        return bestCentroidOffset;
    }

    private static <V> int computeBestCentroidPrefix(
        V vector,
        V[] centroids,
        float[] distances,
        PrefixScratch scratch,
        CentroidOps<V> ops
    ) {
        final int dims = ops.length(vector);
        final int prefixLength = scratch.prefixLength;
        final int suffixLength = dims - prefixLength;
        scratch.reset();

        int bulkLimit = centroids.length - 3;
        int i = 0;
        for (; i < bulkLimit; i += 4) {
            ops.squareDistanceBulk(vector, 0, prefixLength, centroids[i], centroids[i + 1], centroids[i + 2], centroids[i + 3], distances);
            for (int k = 0; k < 4; k++) {
                scratch.add(distances[k], i + k);
            }
        }
        for (; i < centroids.length; i++) {
            float prefixDistance = ops.squareDistance(vector, centroids[i], 0, prefixLength);
            scratch.add(prefixDistance, i);
        }

        int bestCentroid = -1;
        float bestDistance = Float.MAX_VALUE;
        int topLimit = Math.min(PREFIX_TOPK_SIZE, centroids.length);
        int j = 0;
        for (; j + 3 < topLimit; j += 4) {
            ops.squareDistanceBulk(
                vector,
                prefixLength,
                suffixLength,
                centroids[scratch.topPrefixIds[j]],
                centroids[scratch.topPrefixIds[j + 1]],
                centroids[scratch.topPrefixIds[j + 2]],
                centroids[scratch.topPrefixIds[j + 3]],
                distances
            );
            for (int k = 0; k < 4; k++) {
                int centroidOrd = scratch.topPrefixIds[j + k];
                float fullDistance = scratch.topPrefixDistances[j + k] + distances[k];
                if (fullDistance < bestDistance) {
                    bestDistance = fullDistance;
                    bestCentroid = centroidOrd;
                }
            }
        }
        assert j >= topLimit;
        return bestCentroid == -1 ? 0 : bestCentroid;
    }

    private static int prefixLength(int dims) {
        int computed = Math.round(dims * PREFIX_LENGTH_RATIO);
        int roundedToMultiple = ((computed + PREFIX_MULTIPLE - 1) / PREFIX_MULTIPLE) * PREFIX_MULTIPLE;
        return roundedToMultiple;
    }

    record PrefixScratch(float[] topPrefixDistances, int[] topPrefixIds, int prefixLength) {
        PrefixScratch(int prefixLength) {
            this(new float[PREFIX_TOPK_SIZE], new int[PREFIX_TOPK_SIZE], prefixLength);
        }

        public void reset() {
            Arrays.fill(topPrefixDistances, Float.POSITIVE_INFINITY);
            Arrays.fill(topPrefixIds, -1);
        }

        public void add(float distance, int id) {
            int last = topPrefixDistances.length - 1;
            if (distance >= topPrefixDistances[last]) {
                return;
            }
            int i = last;
            while (i > 0 && distance < topPrefixDistances[i - 1]) {
                topPrefixDistances[i] = topPrefixDistances[i - 1];
                topPrefixIds[i] = topPrefixIds[i - 1];
                i--;
            }
            topPrefixDistances[i] = distance;
            topPrefixIds[i] = id;
        }
    }
}
