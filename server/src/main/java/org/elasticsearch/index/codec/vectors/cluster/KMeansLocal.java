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
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.MathUtils;

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
 */
abstract class KMeansLocal {

    KMeansLocal() {}

    /** Number of workers to use for parallelism **/
    protected abstract int numWorkers();

    /** assign to each vector the soar assignment **/
    protected abstract void assignSpilled(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException;

    /** compute the neighborhoods for the given centroids and clustersPerNeighborhood */
    protected abstract NeighborHood[] computeNeighborhoods(float[][] centroids, int clustersPerNeighborhood) throws IOException;

    /**
     * uses a Reservoir Sampling approach to picking the initial centroids which are subsequently expected
     * to be used by a clustering algorithm
     *
     * @param vectors used to pick an initial set of random centroids
     * @param centroidCount the total number of centroids to pick
     * @return randomly selected centroids that are the min of centroidCount and sampleSize
     * @throws IOException is thrown if vectors is inaccessible
     */
    static float[][] pickInitialCentroids(ClusteringFloatVectorValues vectors, int centroidCount) throws IOException {
        Random random = new Random(42L);
        int centroidsSize = Math.min(vectors.size(), centroidCount);
        float[][] centroids = new float[centroidsSize][vectors.dimension()];
        for (int i = 0; i < vectors.size(); i++) {
            float[] vector;
            if (i < centroidCount) {
                vector = vectors.vectorValue(i);
                System.arraycopy(vector, 0, centroids[i], 0, vector.length);
            } else if (random.nextDouble() < centroidCount * (1.0 / i)) {
                int c = random.nextInt(centroidCount);
                vector = vectors.vectorValue(i);
                System.arraycopy(vector, 0, centroids[c], 0, vector.length);
            }
        }
        return centroids;
    }

    /** Assign vectors from {@code startOrd} to {@code endOrd} to the closest centroid. */
    protected static boolean stepLloydSlice(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
        float[][] centroids,
        FixedBitSet centroidChanged,
        int[] assignments,
        NeighborHood[] neighborhoods,
        int startOrd,
        int endOrd
    ) throws IOException {
        centroidChanged.clear();
        if (neighborhoods != null) {
            return vectors.bestCentroidsFromNeighbours(
                startOrd,
                endOrd,
                centroids,
                ordTranslator,
                centroidChanged,
                neighborhoods,
                assignments
            );
        } else {
            return vectors.bestCentroids(startOrd, endOrd, centroids, ordTranslator, centroidChanged, assignments);
        }
    }

    protected static boolean stepLloydSliceConcurrent(
        TaskExecutor executor,
        int numWorkers,
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
        float[][] centroids,
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
                () -> stepLloydSlice(vectors.copy(), ordTranslator, centroids, centroidChangedSlice, assignments, neighborHoods, start, end)
            );
        }
        final List<Boolean> hasChanges = executor.invokeAll(runners);
        return hasChanges.stream().anyMatch(Boolean::booleanValue);
    }

    /** Assign vectors from {@code startOrd} to {@code endOrd} to the SOAR centroid. */
    protected static void assignSpilledSlice(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
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
        float[][] centroids = kmeansIntermediate.centroids();
        vectors.assignSpilled(startOrd, endOrd, centroids, neighborhoods, soarLambda, assignments, spilledAssignments);
    }

    protected static void assignSpilledConcurrent(
        TaskExecutor executor,
        int numWorkers,
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        final int len = vectors.size() / numWorkers;
        final List<Callable<Void>> runners = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            final int start = i * len;
            final int end = i == numWorkers - 1 ? vectors.size() : (i + 1) * len;
            runners.add(() -> {
                assignSpilledSlice(vectors.copy(), kmeansIntermediate, neighborhoods, soarLambda, start, end);
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
    final void cluster(ClusteringFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate) throws IOException {
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
     */
    final void cluster(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kMeansIntermediate,
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
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kMeansIntermediate,
        int clustersPerNeighborhood,
        float soarLambda
    ) throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        boolean neighborAware = clustersPerNeighborhood != -1 && centroids.length > 1;
        NeighborHood[] neighborhoods = null;
        // if there are very few centroids, don't bother with neighborhoods or neighbor aware clustering
        if (neighborAware && centroids.length > clustersPerNeighborhood) {
            neighborhoods = computeNeighborhoods(centroids, clustersPerNeighborhood);
        }
        innerCluster(vectors, kMeansIntermediate, neighborhoods);
        removeEmptyClusters(kMeansIntermediate);
        if (neighborAware && soarLambda >= 0) {
            assert kMeansIntermediate.soarAssignments().length == 0;
            kMeansIntermediate.setSoarAssignments(new int[vectors.size()]);
            assignSpilled(vectors, kMeansIntermediate, neighborhoods, soarLambda);
        }
    }

    protected abstract void innerCluster(
        ClusteringFloatVectorValues vectors,
        KMeansIntermediate kMeansIntermediate,
        NeighborHood[] neighborhoods
    ) throws IOException;

    protected static void deepCopy(float[][] source, float[][] destination) {
        for (int i = 0; i < source.length; i++) {
            System.arraycopy(source[i], 0, destination[i], 0, source[i].length);
        }
    }

    // Computes: (sum_i sum_j pow(vecs1[i][j] - vecs2[i][j], 2)) / (sum_i sum_j pow(vecs2[i][j], 2))
    protected static float normalizedFrobeniusNorm(float[][] vecs1, float[][] vecs2) {
        assert vecs1.length == vecs2.length;
        float result = 0;
        float norm2 = 0;
        for (int i = 0; i < vecs1.length; i++) {
            result += ESVectorUtil.squareDistance(vecs1[i], vecs2[i]);
            norm2 += ESVectorUtil.dotProduct(vecs2[i], vecs2[i]);
        }
        return MathUtils.sqrt(result / norm2);
    }

    private static void removeEmptyClusters(KMeansIntermediate kMeansIntermediate) {
        float[][] centroids = kMeansIntermediate.centroids();
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
            final float[][] singleClusterCentroid = new float[1][];
            singleClusterCentroid[0] = centroids[effectiveCluster];
            final int[] singleClusterCounts = new int[1];
            singleClusterCounts[0] = assignments.length;
            kMeansIntermediate.setCentroids(singleClusterCentroid, singleClusterCounts);
            Arrays.fill(kMeansIntermediate.assignments(), 0);
            return;
        }

        if (effectiveK == centroids.length) {
            return;
        }

        final float[][] newCentroids = new float[effectiveK][centroids[0].length];
        final int[] newClusterCounts = new int[effectiveK];
        final int[] centroidIndexMap = new int[centroids.length];
        int currentCluster = 0;
        for (int c = 0; c < centroids.length; c++) {
            if (centroidVectorCount[c] > 0) {
                centroidIndexMap[c] = currentCluster;
                System.arraycopy(centroids[c], 0, newCentroids[currentCluster], 0, centroids[c].length);
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
    }
}
