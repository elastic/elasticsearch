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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
class KMeansLocal {

    final int sampleSize;
    final int maxIterations;
    final int clustersPerNeighborhood;
    final float soarLambda;

    KMeansLocal(int sampleSize, int maxIterations, int clustersPerNeighborhood, float soarLambda) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
        this.clustersPerNeighborhood = clustersPerNeighborhood;
        this.soarLambda = soarLambda;
    }

    KMeansLocal(int sampleSize, int maxIterations) {
        this(sampleSize, maxIterations, -1, -1f);
    }

    /**
     * uses a Reservoir Sampling approach to picking the initial centroids which are subsequently expected
     * to be used by a clustering algorithm
     *
     * @param vectors used to pick an initial set of random centroids
     * @param centroidCount the total number of centroids to pick
     * @return randomly selected centroids that are the min of centroidCount and sampleSize
     * @throws IOException is thrown if vectors is inaccessible
     */
    static float[][] pickInitialCentroids(FloatVectorValues vectors, int centroidCount) throws IOException {
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

    private boolean stepLloyd(
        FloatVectorValues vectors,
        float[][] centroids,
        float[][] nextCentroids,
        int[] assignments,
        int sampleSize,
        List<int[]> neighborhoods
    ) throws IOException {
        boolean changed = false;
        int dim = vectors.dimension();
        int[] centroidCounts = new int[centroids.length];

        for (int i = 0; i < nextCentroids.length; i++) {
            Arrays.fill(nextCentroids[i], 0.0f);
        }

        for (int i = 0; i < sampleSize; i++) {
            float[] vector = vectors.vectorValue(i);
            int[] neighborOffsets = null;
            int centroidIdx = -1;
            if (neighborhoods != null) {
                neighborOffsets = neighborhoods.get(assignments[i]);
                centroidIdx = assignments[i];
            }
            int bestCentroidOffset = getBestCentroidOffset(centroids, vector, centroidIdx, neighborOffsets);
            if (assignments[i] != bestCentroidOffset) {
                changed = true;
            }
            assignments[i] = bestCentroidOffset;
            centroidCounts[bestCentroidOffset]++;
            for (short d = 0; d < dim; d++) {
                nextCentroids[bestCentroidOffset][d] += vector[d];
            }
        }

        for (int clusterIdx = 0; clusterIdx < centroids.length; clusterIdx++) {
            if (centroidCounts[clusterIdx] > 0) {
                float countF = (float) centroidCounts[clusterIdx];
                for (short d = 0; d < dim; d++) {
                    centroids[clusterIdx][d] = nextCentroids[clusterIdx][d] / countF;
                }
            }
        }

        return changed;
    }

    int getBestCentroidOffset(float[][] centroids, float[] vector, int centroidIdx, int[] centroidOffsets) {
        int bestCentroidOffset = centroidIdx;
        float minDsq;
        if (centroidIdx > 0 && centroidIdx < centroids.length) {
            minDsq = VectorUtil.squareDistance(vector, centroids[centroidIdx]);
        } else {
            minDsq = Float.MAX_VALUE;
        }

        int k = 0;
        for (int j = 0; j < centroids.length; j++) {
            if (centroidOffsets == null || j == centroidOffsets[k]) {
                float dsq = VectorUtil.squareDistance(vector, centroids[j]);
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = j;
                }
            }
        }
        return bestCentroidOffset;
    }

    private void computeNeighborhoods(float[][] centers, List<int[]> neighborhoods, int clustersPerNeighborhood) {
        int k = neighborhoods.size();

        if (k == 0 || clustersPerNeighborhood <= 0) {
            return;
        }

        List<NeighborQueue> neighborQueues = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            neighborQueues.add(new NeighborQueue(clustersPerNeighborhood, true));
        }
        for (int i = 0; i < k - 1; i++) {
            for (int j = i + 1; j < k; j++) {
                float dsq = VectorUtil.squareDistance(centers[i], centers[j]);
                neighborQueues.get(j).insertWithOverflow(i, dsq);
                neighborQueues.get(i).insertWithOverflow(j, dsq);
            }
        }

        for (int i = 0; i < k; i++) {
            NeighborQueue queue = neighborQueues.get(i);
            int neighborCount = queue.size();
            int[] neighbors = new int[neighborCount];
            queue.consumeNodes(neighbors);
            neighborhoods.set(i, neighbors);
        }
    }

    private int[] assignSpilled(FloatVectorValues vectors, List<int[]> neighborhoods, float[][] centroids, int[] assignments)
        throws IOException {
        // SOAR uses an adjusted distance for assigning spilled documents which is
        // given by:
        //
        // soar(x, c) = ||x - c||^2 + lambda * ((x - c_1)^t (x - c))^2 / ||x - c_1||^2
        //
        // Here, x is the document, c is the nearest centroid, and c_1 is the first
        // centroid the document was assigned to. The document is assigned to the
        // cluster with the smallest soar(x, c).

        int[] spilledAssignments = new int[assignments.length];

        float[] diffs = new float[vectors.dimension()];
        for (int i = 0; i < vectors.size(); i++) {
            float[] vector = vectors.vectorValue(i);

            int currAssignment = assignments[i];
            float[] currentCentroid = centroids[currAssignment];
            for (short j = 0; j < vectors.dimension(); j++) {
                float diff = vector[j] - currentCentroid[j];
                diffs[j] = diff;
            }

            // TODO: cache these?
            // float vectorCentroidDist = assignmentDistances[i];
            float vectorCentroidDist = VectorUtil.squareDistance(vector, currentCentroid);

            int bestAssignment = -1;
            float minSoar = Float.MAX_VALUE;
            assert neighborhoods.get(currAssignment) != null;
            for (int neighbor : neighborhoods.get(currAssignment)) {
                if (neighbor == currAssignment) {
                    continue;
                }
                float[] neighborCentroid = centroids[neighbor];
                float soar = distanceSoar(diffs, vector, neighborCentroid, vectorCentroidDist);
                if (soar < minSoar) {
                    bestAssignment = neighbor;
                    minSoar = soar;
                }
            }

            spilledAssignments[i] = bestAssignment;
        }

        return spilledAssignments;
    }

    private float distanceSoar(float[] residual, float[] vector, float[] centroid, float rnorm) {
        // TODO: combine these to be more efficient
        float dsq = VectorUtil.squareDistance(vector, centroid);
        float rproj = ESVectorUtil.soarResidual(vector, centroid, residual);
        return dsq + soarLambda * rproj * rproj / rnorm;
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
    void cluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate) throws IOException {
        cluster(vectors, kMeansIntermediate, false);
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
     * @param neighborAware whether nearby neighboring centroids and their vectors should be used to update the centroid positions,
     *                      implies SOAR assignments
     * @throws IOException is thrown if vectors is inaccessible
     */
    void cluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, boolean neighborAware) throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();

        List<int[]> neighborhoods = null;
        if (neighborAware) {
            int k = centroids.length;
            neighborhoods = new ArrayList<>(k);
            for (int i = 0; i < k; ++i) {
                neighborhoods.add(null);
            }
            computeNeighborhoods(centroids, neighborhoods, clustersPerNeighborhood);
        }
        cluster(vectors, kMeansIntermediate, neighborhoods);
        if (neighborAware && clustersPerNeighborhood > 0) {
            int[] assignments = kMeansIntermediate.assignments();
            assert assignments != null;
            assert assignments.length == vectors.size();
            kMeansIntermediate.setSoarAssignments(assignSpilled(vectors, neighborhoods, centroids, assignments));
        }
    }

    void cluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, List<int[]> neighborhoods) throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();

        if (k == 1 || k >= n) {
            return;
        }

        int[] assignments = new int[n];
        float[][] nextCentroids = new float[centroids.length][vectors.dimension()];
        for (int i = 0; i < maxIterations; i++) {
            if (stepLloyd(vectors, centroids, nextCentroids, assignments, sampleSize, neighborhoods) == false) {
                break;
            }
        }
        stepLloyd(vectors, centroids, nextCentroids, assignments, vectors.size(), neighborhoods);
    }

    /**
     * helper that calls {@link KMeansLocal#cluster(FloatVectorValues, KMeansIntermediate)} given a set of initialized centroids,
     * this call is not neighbor aware
     *
     * @param vectors the vectors to cluster
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static void cluster(FloatVectorValues vectors, float[][] centroids, int sampleSize, int maxIterations) throws IOException {
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids);
        KMeansLocal kMeans = new KMeansLocal(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }

}
