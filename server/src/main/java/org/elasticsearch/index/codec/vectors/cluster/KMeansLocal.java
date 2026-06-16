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
import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

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

    /** assign to each vector the soar assignment */
    protected abstract void assignSpilled(
        ClusteringVectorValues<V> vectors,
        KMeansIntermediate<V> kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException;

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
        IntToIntFunction ordTranslator,
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
        IntToIntFunction ordTranslator,
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

    /** Assign vectors from {@code startOrd} to {@code endOrd} to the SOAR centroid. */
    protected static <V> void assignSpilledSlice(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        KMeansIntermediate<V> kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda,
        int startOrd,
        int endOrd
    ) throws IOException {
        int[] assignments = kmeansIntermediate.assignments();
        assert assignments != null;
        assert assignments.length == vectors.size();
        int[] spilledAssignments = kmeansIntermediate.soarAssignments();
        assert spilledAssignments != null;
        assert spilledAssignments.length == vectors.size();
        V[] centroids = kmeansIntermediate.centroids();
        CentroidAssignment.assignSpilled(
            vectors,
            ops,
            startOrd,
            endOrd,
            centroids,
            neighborhoods,
            soarLambda,
            assignments,
            spilledAssignments
        );
    }

    protected static <V> void assignSpilledConcurrent(
        TaskExecutor executor,
        int numWorkers,
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        KMeansIntermediate<V> kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        final int len = vectors.size() / numWorkers;
        final List<Callable<Void>> runners = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            final int start = i * len;
            final int end = i == numWorkers - 1 ? vectors.size() : (i + 1) * len;
            runners.add(() -> {
                assignSpilledSlice(vectors.copy(), ops, kmeansIntermediate, neighborhoods, soarLambda, start, end);
                return null;
            });
        }
        executor.invokeAll(runners);
    }

    /**
     * Compute a clustering that is not neighbor aware.
     * Different implementations of this abstract class may use different algorithm for clustering.
     *
     * @param vectors the vectors to cluster
     * @param kMeansIntermediate the output object to populate which minimally includes centroids,
     *                     but may include assignments and soar assignments as well; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     * @throws IOException is thrown if vectors is inaccessible
     */
    final void cluster(ClusteringVectorValues<V> vectors, KMeansIntermediate<V> kMeansIntermediate) throws IOException {
        doCluster(vectors, kMeansIntermediate, -1, -1);
    }

    /**
     * Compute a clustering that considers prior clustered neighborhoods when adjusting centroids.
     * Different implementations of this abstract class may use different algorithm for clustering.
     * This also is used to generate the neighborhood aware additional (SOAR) assignments
     *
     * @param vectors the vectors to cluster
     * @param kMeansIntermediate the output object to populate which minimally includes centroids,
     *                     the prior assignments of the given vectors; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     *                     and assignments that are the same size as the vectors.  The SOAR assignments are overwritten by this operation.
     * @param clustersPerNeighborhood number of nearby neighboring centroids to be used to update the centroid positions.
     * @param soarLambda   lambda used for SOAR assignments
     *
     * @throws IOException is thrown if vectors is inaccessible or if the clustersPerNeighborhood is less than 2
     * This also is used to generate the neighborhood aware additional (SOAR) assignments.
     */
    final void cluster(
        ClusteringVectorValues<V> vectors,
        KMeansIntermediate<V> kMeansIntermediate,
        int clustersPerNeighborhood,
        float soarLambda
    ) throws IOException {
        if (clustersPerNeighborhood < 2) {
            throw new IllegalArgumentException("clustersPerNeighborhood must be at least 2, got [" + clustersPerNeighborhood + "]");
        }
        doCluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);
    }

    /**
     * cluster using a Lloyd kmeans algorithm that also considers prior clustered neighborhoods when adjusting centroids
     * this also is used to generate the neighborhood aware additional (SOAR) assignments
     *
     * @param vectors the vectors to cluster
     * @param kMeansIntermediate the output object to populate which minimally includes centroids, the prior assignments of the given
     *                           vectors; care should be taken in passing in a valid output object with a centroids array that is the size
     *                           of centroids expected and assignments that are the same size as the vectors.
     *                           The SOAR assignments are overwritten by this operation.
     * @param clustersPerNeighborhood number of nearby neighboring centroids to be used to update the centroid positions.
     * @param soarLambda   lambda used for SOAR assignments
     *
     * @throws IOException is thrown if vectors is inaccessible or if the clustersPerNeighborhood is less than 2
     */
    protected void doCluster(
        ClusteringVectorValues<V> vectors,
        KMeansIntermediate<V> kMeansIntermediate,
        int clustersPerNeighborhood,
        float soarLambda
    ) throws IOException {
        V[] centroids = kMeansIntermediate.centroids();
        boolean neighborAware = clustersPerNeighborhood != -1 && centroids.length > 1;
        NeighborHood[] neighborhoods = null;
        // if there are very few centroids, don't bother with neighborhoods or neighbor aware clustering
        if (neighborAware && centroids.length > clustersPerNeighborhood) {
            neighborhoods = computeNeighborhoods(centroids, clustersPerNeighborhood);
        }
        innerCluster(vectors, kMeansIntermediate, neighborhoods);
        removeEmptyClusters(kMeansIntermediate, neighborhoods, ops);
        if (neighborAware && soarLambda >= 0 && kMeansIntermediate.centroids().length > 1) {
            assert kMeansIntermediate.soarAssignments().length == 0;
            kMeansIntermediate.setSoarAssignments(new int[vectors.size()]);
            assignSpilled(vectors, kMeansIntermediate, neighborhoods, soarLambda);
        }
    }

    protected abstract void innerCluster(
        ClusteringVectorValues<V> vectors,
        KMeansIntermediate<V> kMeansIntermediate,
        NeighborHood[] neighborhoods
    ) throws IOException;

    private static <V> void removeEmptyClusters(
        KMeansIntermediate<V> kMeansIntermediate,
        NeighborHood[] neighborhoods,
        CentroidOps<V> ops
    ) {
        V[] centroids = kMeansIntermediate.centroids();
        int[] assignments = kMeansIntermediate.assignments();
        int[] centroidVectorCount = kMeansIntermediate.clusterCounts();

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
            kMeansIntermediate.setCentroids(singleClusterCentroid, singleClusterCounts);
            Arrays.fill(kMeansIntermediate.assignments(), 0);
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
        kMeansIntermediate.setCentroids(newCentroids, newClusterCounts);

        if (neighborhoods != null) {
            // This change will cause that neighborhoods.length > newCentroids.length.
            // Doing it like this avoids more memory allocations and is fine as long as
            // we do not use neighborhoods.length to get the number of clusters.
            for (int c = 0; c < centroids.length; c++) {
                neighborhoods[centroidIndexMap[c]] = neighborhoods[c];
                int[] neighbors = neighborhoods[c].neighbors();
                for (int i = 0; i < neighbors.length; i++) {
                    neighbors[i] = centroidIndexMap[neighbors[i]];
                }
                neighborhoods[centroidIndexMap[c]] = neighborhoods[c];
            }
        }
    }
}
