/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.IntUnaryOperator;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
abstract class KMeansLocal<V> {

    protected final CentroidOps<V> ops;

    KMeansLocal(CentroidOps<V> ops) {
        this.ops = ops;
    }

    /** Number of workers to use for parallelism */
    protected abstract int numWorkers();

    /** compute the neighborhoods for the given centroids and clustersPerNeighborhood */
    protected abstract NeighborHood[] computeNeighborhoods(V[] centroids, int clustersPerNeighborhood) throws IOException;

    /**
     * Uses a Reservoir Sampling approach to picking the initial centroids which are subsequently expected
     * to be used by a clustering algorithm.
     *
     * @param vectors used to pick an initial set of random centroids
     * @param centroidCount the total number of centroids to pick
     * @param ops the centroid operations for creating/copying centroids
     * @return randomly selected centroids that are the min of centroidCount and sampleSize
     * @throws IOException is thrown if vectors is inaccessible
     */
    static <V> V[] pickInitialCentroids(ClusteringVectorValues<V> vectors, int centroidCount, CentroidOps<V> ops) throws IOException {
        Random random = new Random(42L);
        int centroidsSize = Math.min(vectors.size(), centroidCount);
        V[] centroids = ops.newCentroidArray(centroidsSize, vectors.dimension());
        for (int i = 0; i < vectors.size(); i++) {
            if (i < centroidCount) {
                V vector = vectors.vectorValue(i);
                ops.initCentroid(centroids[i], vector, vectors.dimension());
            } else if (random.nextDouble() < centroidCount * (1.0 / i)) {
                int c = random.nextInt(centroidCount);
                V vector = vectors.vectorValue(i);
                ops.initCentroid(centroids[c], vector, vectors.dimension());
            }
        }
        return centroids;
    }

    /** Assign vectors from {@code startOrd} to {@code endOrd} to the closest centroid. */
    protected static <V> boolean stepLloydSlice(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        IntUnaryOperator ordTranslator,
        V[] centroids,
        FixedBitSet centroidChanged,
        int[] assignments,
        NeighborHood[] neighborhoods,
        int startOrd,
        int endOrd
    ) throws IOException {
        centroidChanged.clear();
        if (neighborhoods != null) {
            return CentroidAssignment.bestCentroidsFromNeighbours(
                vectors,
                ops,
                startOrd,
                endOrd,
                centroids,
                ordTranslator,
                centroidChanged,
                neighborhoods,
                assignments
            );
        } else {
            return CentroidAssignment.bestCentroids(vectors, ops, startOrd, endOrd, centroids, ordTranslator, centroidChanged, assignments);
        }
    }

    protected static <V> boolean stepLloydSliceConcurrent(
        TaskExecutor executor,
        int numWorkers,
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        IntUnaryOperator ordTranslator,
        V[] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException {
        assert numWorkers == centroidChangedSlices.length;
        final int len = vectors.size() / numWorkers;
        final List<Callable<Boolean>> runners = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            final int start = i * len;
            final int end = i == numWorkers - 1 ? vectors.size() : (i + 1) * len;
            final FixedBitSet centroidChangedSlice = centroidChangedSlices[i];
            runners.add(
                () -> stepLloydSlice(
                    vectors.copy(),
                    ops,
                    ordTranslator,
                    centroids,
                    centroidChangedSlice,
                    assignments,
                    neighborHoods,
                    start,
                    end
                )
            );
        }
        final List<Boolean> hasChanges = executor.invokeAll(runners);
        return hasChanges.stream().anyMatch(Boolean::booleanValue);
    }

    /**
     * Compute a clustering that is not neighbor aware.
     * Different implementations of this abstract class may use different algorithm for clustering.
     *
     * @param vectors the vectors to cluster
     * @param kMeansResult the output object to populate which minimally includes centroids,
     *                     but may include assignments and soar assignments as well; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     * @throws IOException is thrown if vectors is inaccessible
     */
    final void cluster(ClusteringVectorValues<V> vectors, KMeansResult<V> kMeansResult) throws IOException {
        doCluster(vectors, kMeansResult, null);
    }

    /**
     * Computes any overspill assignments and returns them in {@code OverspillAssignments}
     */
    protected abstract OverspillAssignments assignSpilled(
        ClusteringVectorValues<V> vectors,
        KMeansResult<V> kMeansResult,
        NeighborHood[] neighborhoods
    ) throws IOException;

    /**
     * Compute a clustering that considers prior clustered neighborhoods when adjusting centroids.
     * Different implementations of this abstract class may use different algorithm for clustering.
     * This also is used to generate the neighborhood aware additional overspill assignments
     *
     * @param vectors the vectors to cluster
     * @param kMeansResult the output object to populate which minimally includes centroids,
     *                     the prior assignments of the given vectors; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     *                     and assignments that are the same size as the vectors.
     * @param clustersPerNeighborhood number of nearby neighboring centroids to be used to update the centroid positions.
     *
     * @return the clustering result with the overspill assignments
     * @throws IOException is thrown if vectors is inaccessible or if the clustersPerNeighborhood is less than 2
     */
    final KMeansWithOverspill<V> cluster(ClusteringVectorValues<V> vectors, KMeansResult<V> kMeansResult, int clustersPerNeighborhood)
        throws IOException {
        if (clustersPerNeighborhood < 2) {
            throw new IllegalArgumentException("clustersPerNeighborhood must be at least 2, got [" + clustersPerNeighborhood + "]");
        }
        NeighborHood[] neighborhoods = null;
        // if there are very few centroids, don't bother with neighborhoods or neighbor aware clustering
        if (kMeansResult.centroids().length > clustersPerNeighborhood) {
            neighborhoods = computeNeighborhoods(kMeansResult.centroids(), clustersPerNeighborhood);
        }
        doCluster(vectors, kMeansResult, neighborhoods);
        OverspillAssignments overspill = null;
        if (kMeansResult.centroids().length > 1) {
            overspill = assignSpilled(vectors, kMeansResult, neighborhoods);
        }
        return new KMeansWithOverspill<>(kMeansResult, overspill);
    }

    private void doCluster(ClusteringVectorValues<V> vectors, KMeansResult<V> kMeansResult, NeighborHood[] neighborhoods)
        throws IOException {
        innerCluster(vectors, kMeansResult, neighborhoods);
        removeEmptyClusters(kMeansResult, neighborhoods, ops);
    }

    protected abstract void innerCluster(ClusteringVectorValues<V> vectors, KMeansResult<V> kMeansResult, NeighborHood[] neighborhoods)
        throws IOException;

    private static <V> void removeEmptyClusters(KMeansResult<V> kMeansResult, NeighborHood[] neighborhoods, CentroidOps<V> ops) {
        V[] centroids = kMeansResult.centroids();
        int[] assignments = kMeansResult.assignments();
        int[] centroidVectorCount = kMeansResult.clusterCounts();

        Arrays.fill(centroidVectorCount, 0, centroids.length, 0);

        // handle assignment here so we can track distance and cluster size
        int effectiveCluster = -1;
        int effectiveK = 0;
        for (int assignment : assignments) {
            centroidVectorCount[assignment]++;
            // this cluster has received an assignment, its now effective, but only count it once
            if (centroidVectorCount[assignment] == 1) {
                effectiveK++;
                effectiveCluster = assignment;
            }
        }

        if (effectiveK == 1) {
            int dims = ops.length(centroids[0]);
            V[] singleClusterCentroid = ops.newCentroidArray(1, dims);
            ops.initCentroid(singleClusterCentroid[0], centroids[effectiveCluster], dims);
            final int[] singleClusterCounts = new int[1];
            singleClusterCounts[0] = assignments.length;
            kMeansResult.setCentroids(singleClusterCentroid, singleClusterCounts);
            Arrays.fill(kMeansResult.assignments(), 0);
            return;
        }

        if (effectiveK == centroids.length) {
            return;
        }

        // TODO eventually, we should get rid of this allocation by overhauling how centroids
        // are stored and handled in KMeansResult
        int dims = ops.length(centroids[0]);
        final V[] newCentroids = ops.newCentroidArray(effectiveK, dims);
        final int[] newClusterCounts = new int[effectiveK];
        final int[] centroidIndexMap = new int[centroids.length];
        // Use -1 as a sentinel for removed (empty) centroids so that neighborhood remapping
        // can distinguish removed centroids from centroid 0.
        Arrays.fill(centroidIndexMap, -1);
        int currentCluster = 0;
        for (int c = 0; c < centroids.length; c++) {
            if (centroidVectorCount[c] > 0) {
                centroidIndexMap[c] = currentCluster;
                ops.initCentroid(newCentroids[currentCluster], centroids[c], dims);
                newClusterCounts[currentCluster] = centroidVectorCount[c];
                currentCluster++;
            }
        }

        for (int i = 0; i < assignments.length; i++) {
            if (centroidVectorCount[assignments[i]] > 0) {
                assignments[i] = centroidIndexMap[assignments[i]];
            }
        }
        kMeansResult.setCentroids(newCentroids, newClusterCounts);

        if (neighborhoods != null) {
            // Remap neighborhood indices to match the compacted centroid array, filtering out
            // any neighbors that referenced a removed (empty) centroid. We iterate non-empty
            // centroids in ascending order; since centroidIndexMap is monotonically increasing
            // for non-empty entries, each write index is <= the read index, so earlier writes
            // can't overwrite unprocessed entries.
            for (int c = 0; c < centroids.length; c++) {
                if (centroidVectorCount[c] == 0) {
                    continue;
                }
                int newIdx = centroidIndexMap[c];
                int[] oldNeighbors = neighborhoods[c].neighbors();
                int kept = 0;
                for (int n : oldNeighbors) {
                    if (centroidIndexMap[n] != -1) {
                        oldNeighbors[kept++] = centroidIndexMap[n];
                    }
                }
                if (kept == oldNeighbors.length) {
                    neighborhoods[newIdx] = neighborhoods[c];
                } else {
                    int[] trimmed = new int[kept];
                    System.arraycopy(oldNeighbors, 0, trimmed, 0, kept);
                    neighborhoods[newIdx] = new NeighborHood(trimmed, neighborhoods[c].maxIntraDistance());
                }
            }
        }
    }
}
