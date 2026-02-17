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
import org.apache.lucene.util.hnsw.IntToIntFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
abstract class KMeansLocal {

    private final int sampleSize;
    private final int maxIterations;

    KMeansLocal(int sampleSize, int maxIterations) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
    }

    /** Number of workers to use for parallelism **/
    protected abstract int numWorkers();

    /** assign to each vector the closest centroid **/
    protected abstract boolean stepLloyd(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction translateOrd,
        float[][] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException;

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

    /**
     * cluster using a lloyd k-means algorithm that is not neighbor aware
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
     * cluster using a lloyd kmeans algorithm that also considers prior clustered neighborhoods when adjusting centroids
     * this also is used to generate the neighborhood aware additional (SOAR) assignments
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

    private void doCluster(
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
        cluster(vectors, kMeansIntermediate, neighborhoods);
        if (neighborAware && soarLambda >= 0) {
            assert kMeansIntermediate.soarAssignments().length == 0;
            kMeansIntermediate.setSoarAssignments(new int[vectors.size()]);
            assignSpilled(vectors, kMeansIntermediate, neighborhoods, soarLambda);
        }
    }

    private void cluster(ClusteringFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, NeighborHood[] neighborhoods)
        throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();
        int[] assignments = kMeansIntermediate.assignments();

        if (k == 1) {
            Arrays.fill(assignments, 0);
            return;
        }
        IntToIntFunction ordTranslator = i -> i;
        ClusteringFloatVectorValues sampledVectors = vectors;
        if (sampleSize < n) {
            sampledVectors = ClusteringFloatVectorValuesSlice.createRandomSlice(vectors, sampleSize, 42L);
            ordTranslator = sampledVectors::ordToDoc;
        }

        assert assignments.length == n;
        FixedBitSet[] centroidChangedSlices = new FixedBitSet[numWorkers()];
        for (int i = 0; i < numWorkers(); i++) {
            centroidChangedSlices[i] = new FixedBitSet(centroids.length);
        }
        int[] centroidCounts = new int[centroids.length];
        for (int i = 0; i < maxIterations; i++) {
            // This is potentially sampled, so we need to translate ordinals
            if (stepLloyd(sampledVectors, ordTranslator, centroids, centroidChangedSlices, assignments, neighborhoods)) {
                sampledVectors.updateCentroids(centroids, ordTranslator, centroidChangedSlices, centroidCounts, assignments);
            } else {
                break;
            }
        }
        // If we were sampled, do a once over the full set of vectors to finalize the centroids
        if (sampleSize < n || maxIterations == 0) {
            // No ordinal translation needed here, we are using the full set of vectors
            if (stepLloyd(vectors, i -> i, centroids, centroidChangedSlices, assignments, neighborhoods)) {
                sampledVectors.updateCentroids(centroids, ordTranslator, centroidChangedSlices, centroidCounts, assignments);
            }
        }
    }

    /**
     * helper that calls {@link KMeansLocal#cluster(ClusteringFloatVectorValues, KMeansIntermediate)} given a set of initialized centroids,
     * this call is not neighbor aware
     *
     * @param vectors the vectors to cluster
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static void cluster(ClusteringFloatVectorValues vectors, float[][] centroids, int sampleSize, int maxIterations)
        throws IOException {
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, new int[vectors.size()], vectors::ordToDoc);
        KMeansLocal kMeans = new KMeansLocalSerial(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }
}
