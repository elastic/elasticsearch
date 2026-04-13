/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

/**
 * A {@link FloatVectorValues} that adds best-centroid computation.
 */
public abstract sealed class ClusteringFloatVectorValues extends FloatVectorValues permits KMeansFloatVectorValues,
    ClusteringFloatVectorValuesSlice {

    // the minimum distance that is considered to be "far enough" to a centroid in order to compute the soar distance.
    // For vectors that are closer than this distance to the centroid don't get spilled because they are well represented
    // by the centroid itself. In many cases, it indicates a degenerated distribution, e.g the cluster is composed of the
    // many equal vectors.
    private static final float SOAR_MIN_DISTANCE = 1e-16f;
    private static final int PREFIX_MIN_DIMENSIONS = 128;
    private static final float PREFIX_LENGTH_RATIO = 0.5f;
    // we require all prefixes to be a multiple 64, we want to take best advantage of vectorization
    private static final int PREFIX_MULTIPLE = 64;
    private static final int PREFIX_TOPK_SIZE = 4;

    @Override
    public abstract ClusteringFloatVectorValues copy() throws IOException;

    /**
     * Find the closest centroid for a batch of contiguous vectors, considering all centroids.
     *
     * @param startOrd        the first vector ordinal (inclusive) to process
     * @param endOrd          the last vector ordinal (exclusive) to process
     * @param centroids       the centroid vectors to compare against
     * @param ordTranslator   translate the vector ord to the position of the vector on the result array
     * @param centroidChanged a bitset tracking which centroids had assignments change;
     *                        bits are set for both the old and new centroid when a vector is reassigned
     * @param results         input/output array indexed by document ordinal; on entry holds the
     *                        current centroid assignments (or {@code -1} for unassigned),
     *                        on exit holds the updated assignments
     * @return {@code true} if any assignment changed, {@code false} if all assignments remained the same
     */
    final boolean bestCentroids(
        int startOrd,
        int endOrd,
        float[][] centroids,
        IntToIntFunction ordTranslator,
        FixedBitSet centroidChanged,
        int[] results
    ) throws IOException {
        final float[] distances = new float[4];
        final PrefixScratch prefixScratch = maybeCreatePrefixScratch(centroids.length, dimension());
        boolean changed = false;
        for (int i = startOrd; i < endOrd; i++) {
            float[] vector = vectorValue(i);
            final int translatedOrd = ordTranslator.apply(i);
            final int assignment = results[translatedOrd];
            final int bestCentroid = computeBestCentroid(vector, centroids, distances, prefixScratch);
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

    final PrefixScratch maybeCreatePrefixScratch(int numCentroids, int dims) {
        return dimension() >= (PREFIX_MIN_DIMENSIONS * 2) && numCentroids > PREFIX_TOPK_SIZE * 2
            ? new PrefixScratch(prefixLength(dims))
            : null;
    }

    /**
     * Find the closest centroid for a batch of contiguous vectors, restricting the search to each
     * vector's current centroid and its pre-computed neighborhood of nearby centroids.
     *
     * @param startOrd        the first vector ordinal (inclusive) to process
     * @param endOrd          the last vector ordinal (exclusive) to process
     * @param centroids       the centroid vectors to compare against
     * @param ordTranslator   translate the vector ord to the position of the vector on the result array
     * @param centroidChanged a bitset tracking which centroids had assignments change;
     *                        bits are set for both the old and new centroid when a vector is reassigned
     * @param neighborhoods   per-centroid neighborhoods; {@code neighborhoods[c]} contains the
     *                        neighboring centroid indices and maximum intra-cluster distance for centroid {@code c}
     * @param results         input/output array indexed by document ordinal; on entry holds the
     *                        current centroid assignments, on exit holds the updated assignments
     * @return {@code true} if any assignment changed, {@code false} if all assignments remained the same
     */
    final boolean bestCentroidsFromNeighbours(
        int startOrd,
        int endOrd,
        float[][] centroids,
        IntToIntFunction ordTranslator,
        FixedBitSet centroidChanged,
        NeighborHood[] neighborhoods,
        int[] results
    ) throws IOException {
        final float[] distances = new float[4];
        final PrefixScratch prefixScratch = maybeCreatePrefixScratch(centroids.length, dimension());
        boolean changed = false;
        for (int i = startOrd; i < endOrd; i++) {
            float[] vector = vectorValue(i);
            final int translatedOrd = ordTranslator.apply(i);
            final int assignment = results[translatedOrd];
            assert assignment != -1 : "vector is not assigned to any cluster: ord=" + translatedOrd;
            final int bestCentroid = computeBestCentroidFromNeighbours(
                vector,
                centroids,
                assignment,
                neighborhoods[assignment],
                distances,
                prefixScratch
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
     * Recompute centroid positions as the mean of their assigned vectors. Only centroids whose
     * assignments changed (as indicated by the union of {@code centroidChangedSlices}) are
     * recomputed; unchanged centroids are left as-is.
     *
     * @param centroids             the centroid vectors; updated in place with the new mean positions
     * @param ordTranslator         translate the vector ord to thr position of the vector on the assignments array
     * @param centroidChangedSlices per-thread bitsets indicating which centroids had assignment changes;
     *                              these are OR'd together to determine the full set of changed centroids
     * @param centroidCounts        scratch array of length {@code centroids.length}; on exit holds the
     *                              number of vectors assigned to each changed centroid
     * @param assignments           the current centroid assignment for each document ordinal
     */
    final void updateCentroids(
        float[][] centroids,
        IntToIntFunction ordTranslator,
        FixedBitSet[] centroidChangedSlices,
        int[] centroidCounts,
        int[] assignments
    ) throws IOException {
        Arrays.fill(centroidCounts, 0);
        FixedBitSet centroidChanged = centroidChangedSlices[0];
        for (int j = 1; j < centroidChangedSlices.length; j++) {
            centroidChanged.or(centroidChangedSlices[j]);
        }
        int dim = dimension();
        for (int idx = 0; idx < size(); idx++) {
            final int assignment = assignments[ordTranslator.apply(idx)];
            if (centroidChanged.get(assignment)) {
                float[] centroid = centroids[assignment];
                float[] vector = vectorValue(idx);
                if (centroidCounts[assignment]++ == 0) {
                    System.arraycopy(vector, 0, centroid, 0, dim);
                } else {
                    for (int d = 0; d < dim; d++) {
                        centroid[d] += vector[d];
                    }
                }
            }
        }

        for (int clusterIdx = 0; clusterIdx < centroids.length; clusterIdx++) {
            if (centroidChanged.get(clusterIdx)) {
                float count = (float) centroidCounts[clusterIdx];
                if (count > 0) {
                    float[] centroid = centroids[clusterIdx];
                    for (int d = 0; d < dim; d++) {
                        centroid[d] /= count;
                    }
                }
            }
        }
    }

    /**
     * Compute the squared distances between a batch of contiguous vectors and all centroids.
     *
     * @param startOrd         the first vector ordinal (inclusive) to process
     * @param endOrd           the last vector ordinal (exclusive) to process
     * @param centroids        the centroid vectors to compare against
     * @param squaredDistances array of distances indexed by document ordinal
     */
    final void computeSquaredDistances(int startOrd, int endOrd, float[][] centroids, float[][] squaredDistances) throws IOException {
        for (int i = startOrd; i < endOrd; i++) {
            float[] vector = vectorValue(i);
            computeSquaredDistances(vector, centroids, squaredDistances[i]);
        }
    }

    /**
     * Compute the squared distances between a batch of contiguous vectors and all centroids, restricting the search to each
     * vector's current centroid and its pre-computed neighborhood of nearby centroids.
     *
     * @param startOrd         the first vector ordinal (inclusive) to process
     * @param endOrd           the last vector ordinal (exclusive) to process
     * @param centroids        the centroid vectors to compare against
     * @param assigner         a function that given the vector ID, returns the vector's current centroid
     * @param neighborhoods    per-centroid neighborhoods; {@code neighborhoods[c]} contains the
     *                         neighboring centroid indices and maximum intra-cluster distance for centroid {@code c}
     * @param squaredDistances array of distances indexed by document ordinal
     */
    final void computeSquaredDistancesFromNeighbors(
        int startOrd,
        int endOrd,
        float[][] centroids,
        IntToIntFunction assigner,
        NeighborHood[] neighborhoods,
        float[][] squaredDistances
    ) throws IOException {
        for (int i = startOrd; i < endOrd; i++) {
            float[] vector = vectorValue(i);
            final int bestCentroid = assigner.apply(i);
            squaredDistances[i][0] = ESVectorUtil.squareDistance(vector, centroids[bestCentroid]);
            int[] neighbors = neighborhoods[bestCentroid].neighbors();
            for (int j = 0; j < neighbors.length; j++) {
                int neigh = neighbors[j];
                squaredDistances[i][j + 1] = ESVectorUtil.squareDistance(vector, centroids[neigh]);
            }
        }
    }

    /**
     * Assign a secondary ("spilled") centroid to each vector in the given ordinal range using the
     * <a href="https://arxiv.org/abs/2404.18984">SOAR</a> adjusted distance. The SOAR distance for
     * a vector {@code x} with primary centroid {@code c_1} to a candidate centroid {@code c} is:
     * <pre>
     *   soar(x, c) = ||x - c||^2 + lambda * ((x - c_1)^T (x - c))^2 / ||x - c_1||^2
     * </pre>
     * Each vector is assigned to the candidate centroid with the smallest SOAR distance. Vectors
     * that are extremely close to their primary centroid (within {@link #SOAR_MIN_DISTANCE}) receive
     * {@link HierarchicalKMeans#NO_SOAR_ASSIGNMENT} since they are already well represented.
     * <p>
     * When {@code neighborhoods} is non-null, only the neighboring centroids of the vector's
     * primary assignment are considered as candidates; otherwise all centroids (excluding the
     * primary) are evaluated.
     *
     * @param startOrd            the first vector ordinal (inclusive) to process
     * @param endOrd              the last vector ordinal (exclusive) to process
     * @param centroids           the centroid vectors
     * @param neighborhoods       per-centroid neighborhoods used to restrict candidate centroids,
     *                            or {@code null} to consider all centroids
     * @param soarLambda          the lambda weighting factor for the SOAR residual penalty term
     * @param assignments         the primary centroid assignment for each vector ordinal
     * @param spilledAssignments  output array; {@code spilledAssignments[i]} receives the secondary
     *                            centroid index for vector {@code i}, or
     *                            {@link HierarchicalKMeans#NO_SOAR_ASSIGNMENT} if the vector is too
     *                            close to its primary centroid
     */
    final void assignSpilled(
        int startOrd,
        int endOrd,
        float[][] centroids,
        NeighborHood[] neighborhoods,
        float soarLambda,
        int[] assignments,
        int[] spilledAssignments
    ) throws IOException {
        // SOAR uses an adjusted distance for assigning spilled documents which is
        // given by:
        //
        // soar(x, c) = ||x - c||^2 + lambda * ((x - c_1)^t (x - c))^2 / ||x - c_1||^2
        //
        // Here, x is the document, c is the nearest centroid, and c_1 is the first
        // centroid the document was assigned to. The document is assigned to the
        // cluster with the smallest soar(x, c).
        float[] diffs = new float[dimension()];
        final float[] distances = new float[4];
        for (int i = startOrd; i < endOrd; i++) {
            float[] vector = vectorValue(i);
            final int currAssignment = assignments[i];
            final int centroidCount;
            final IntToIntFunction centroidOrds;
            if (neighborhoods != null) {
                assert neighborhoods[currAssignment] != null;
                NeighborHood neighborhood = neighborhoods[currAssignment];
                centroidCount = neighborhood.neighbors().length;
                centroidOrds = c -> neighborhood.neighbors()[c];
            } else {
                centroidCount = centroids.length - 1;
                centroidOrds = c -> c < currAssignment ? c : c + 1; // skip the current centroid
            }
            spilledAssignments[i] = computeSoarAssignment(
                vector,
                centroids,
                currAssignment,
                centroidCount,
                centroidOrds,
                soarLambda,
                diffs,
                distances
            );
        }
    }

    /**
     * Find the closest centroid for a materialized vector, considering all centroids.
     *
     * @param vector    the vector to assign
     * @param centroids the centroid vectors to compare against
     * @param distances scratch array of length 4 used for bulk distance results
     * @return the index into {@code centroids} of the nearest centroid
     */
    private static int computeBestCentroid(float[] vector, float[][] centroids, float[] distances, PrefixScratch prefixScratch) {
        if (prefixScratch != null) {
            return computeBestCentroidPrefix(vector, centroids, distances, prefixScratch);
        }
        final int limit = centroids.length - 3;
        int bestCentroidOffset = 0;
        float minDsq = Float.MAX_VALUE;
        int i = 0;
        for (; i < limit; i += 4) {
            ESVectorUtil.squareDistanceBulk(vector, centroids[i], centroids[i + 1], centroids[i + 2], centroids[i + 3], 0, distances);
            for (int j = 0; j < distances.length; j++) {
                float dsq = distances[j];
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = i + j;
                }
            }
        }
        for (; i < centroids.length; i++) {
            float dsq = ESVectorUtil.squareDistance(vector, centroids[i]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = i;
            }
        }
        return bestCentroidOffset;
    }

    /**
     * Computes the squared distances between a materialized vector and all centroids.
     *
     * @param vector    the vector to assign
     * @param centroids the centroid vectors to compare against
     * @param distances the computed distances
     */
    private static void computeSquaredDistances(float[] vector, float[][] centroids, float[] distances) {
        final int limit = centroids.length - 3;
        int i = 0;
        for (; i < limit; i += 4) {
            ESVectorUtil.squareDistanceBulk(vector, centroids[i], centroids[i + 1], centroids[i + 2], centroids[i + 3], i, distances);
        }
        for (; i < centroids.length; i++) {
            distances[i] = ESVectorUtil.squareDistance(vector, centroids[i]);
        }
    }

    /**
     * Find the closest centroid for a materialized vector, restricting the search to its
     * currently assigned centroid and that centroid's pre-computed neighborhood.
     *
     * @param vector       the vector to assign
     * @param centroids    the centroid vectors to compare against
     * @param centroidIdx  the index of the vector's current centroid assignment
     * @param neighborhood the neighborhood of {@code centroidIdx}, containing neighboring
     *                     centroid indices and the maximum intra-cluster distance
     * @param distances    scratch array of length 4 used for bulk distance results
     * @return the index into {@code centroids} of the nearest centroid (may be {@code centroidIdx}
     *         if no closer neighbor was found)
     */
    private static int computeBestCentroidFromNeighbours(
        float[] vector,
        float[][] centroids,
        int centroidIdx,
        NeighborHood neighborhood,
        float[] distances,
        PrefixScratch prefixScratch
    ) {
        if (prefixScratch != null) {
            return computeBestCentroidFromNeighboursPrefix(vector, centroids, distances, centroidIdx, neighborhood, prefixScratch);
        }
        final int limit = neighborhood.neighbors().length - 3;
        int bestCentroidOffset = centroidIdx;
        assert centroidIdx >= 0 && centroidIdx < centroids.length;
        float minDsq = ESVectorUtil.squareDistance(vector, centroids[centroidIdx]);
        int i = 0;
        for (; i < limit; i += 4) {
            if (minDsq < neighborhood.maxIntraDistance()) {
                // if the distance found is smaller than the maximum intra-cluster distance
                // we don't consider it for further re-assignment
                return bestCentroidOffset;
            }
            ESVectorUtil.squareDistanceBulk(
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
                // if the distance found is smaller than the maximum intra-cluster distance
                // we don't consider it for further re-assignment
                return bestCentroidOffset;
            }
            int offset = neighborhood.neighbors()[i];
            assert offset >= 0 && offset < centroids.length : "Invalid neighbor offset: " + offset;
            // compute the distance to the centroid
            float dsq = ESVectorUtil.squareDistance(vector, centroids[offset]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = offset;
            }
        }
        return bestCentroidOffset;
    }

    private static int computeBestCentroidFromNeighboursPrefix(
        float[] vector,
        float[][] centroids,
        float[] distances,
        int centroidIdx,
        NeighborHood neighborhood,
        PrefixScratch scratch
    ) {
        final int dims = vector.length;
        final int prefixLength = scratch.prefixLength;
        final int suffixLength = dims - prefixLength;
        int bestCentroidOffset = centroidIdx;
        assert centroidIdx >= 0 && centroidIdx < centroids.length;
        float bestDistance = ESVectorUtil.squareDistance(vector, centroids[centroidIdx]);
        if (bestDistance < neighborhood.maxIntraDistance()) {
            return bestCentroidOffset;
        }

        final int[] neighbors = neighborhood.neighbors();
        scratch.reset();
        int limit = neighbors.length - 3;
        int i = 0;
        for (; i < limit; i += 4) {
            ESVectorUtil.squareDistanceBulk(
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
            float prefixDistance = ESVectorUtil.squareDistance(vector, centroids[offset], 0, prefixLength);
            scratch.add(prefixDistance, offset);
        }

        final int topLimit = Math.min(PREFIX_TOPK_SIZE, neighbors.length);
        int j = 0;
        for (; j + 3 < topLimit; j += 4) {
            ESVectorUtil.squareDistanceBulk(
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

    private static int computeBestCentroidPrefix(float[] vector, float[][] centroids, float[] distances, PrefixScratch scratch) {
        final int dims = vector.length;
        final int prefixLength = scratch.prefixLength;
        final int suffixLength = dims - prefixLength;
        scratch.reset();

        int bulkLimit = centroids.length - 3;
        int i = 0;
        for (; i < bulkLimit; i += 4) {
            ESVectorUtil.squareDistanceBulk(
                vector,
                0,
                prefixLength,
                centroids[i],
                centroids[i + 1],
                centroids[i + 2],
                centroids[i + 3],
                distances
            );
            for (int k = 0; k < 4; k++) {
                scratch.add(distances[k], i + k);
            }
        }
        for (; i < centroids.length; i++) {
            float prefixDistance = ESVectorUtil.squareDistance(vector, centroids[i], 0, prefixLength);
            scratch.add(prefixDistance, i);
        }

        int bestCentroid = -1;
        float bestDistance = Float.MAX_VALUE;
        int topLimit = Math.min(PREFIX_TOPK_SIZE, centroids.length);
        int j = 0;
        for (; j + 3 < topLimit; j += 4) {
            ESVectorUtil.squareDistanceBulk(
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
        // TODO do we want to have a "max prefix"? e.g. 2048 vectors might index just fine with a prefix of 768 or 512 vs 1024
        return roundedToMultiple;
    }

    private record PrefixScratch(float[] topPrefixDistances, int[] topPrefixIds, int prefixLength) {
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

    private static int computeSoarAssignment(
        float[] vector,
        float[][] centroids,
        int currAssignment,
        int centroidCount,
        IntToIntFunction centroidOrds,
        float soarLambda,
        float[] diffs,
        float[] distances
    ) {
        float[] currentCentroid = centroids[currAssignment];
        // TODO: cache these?
        float vectorCentroidDist = ESVectorUtil.squareDistance(vector, currentCentroid);
        if (vectorCentroidDist <= SOAR_MIN_DISTANCE) {
            return NO_SOAR_ASSIGNMENT; // no SOAR assignment
        }

        for (int j = 0; j < diffs.length; j++) {
            diffs[j] = vector[j] - currentCentroid[j];
        }

        final int limit = centroidCount - 3;
        int bestAssignment = -1;
        float minSoar = Float.MAX_VALUE;
        int j = 0;
        for (; j < limit; j += 4) {
            ESVectorUtil.soarDistanceBulk(
                vector,
                centroids[centroidOrds.apply(j)],
                centroids[centroidOrds.apply(j + 1)],
                centroids[centroidOrds.apply(j + 2)],
                centroids[centroidOrds.apply(j + 3)],
                diffs,
                soarLambda,
                vectorCentroidDist,
                distances
            );
            for (int k = 0; k < distances.length; k++) {
                float soar = distances[k];
                if (soar < minSoar) {
                    minSoar = soar;
                    bestAssignment = centroidOrds.apply(j + k);
                }
            }
        }

        for (; j < centroidCount; j++) {
            int centroidOrd = centroidOrds.apply(j);
            float soar = ESVectorUtil.soarDistance(vector, centroids[centroidOrd], diffs, soarLambda, vectorCentroidDist);
            if (soar < minSoar) {
                minSoar = soar;
                bestAssignment = centroidOrd;
            }
        }
        assert bestAssignment != -1 : "Failed to assign soar vector to centroid";
        return bestAssignment;
    }
}
