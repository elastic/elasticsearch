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

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
class KMeansLocal extends KMeans {

    final int clustersPerNeighborhood;

    KMeansLocal(int sampleSize, int maxIterations, int clustersPerNeighborhood) {
        super(sampleSize, maxIterations);
        this.clustersPerNeighborhood = clustersPerNeighborhood;
    }

    private void computeNeighborhoods(
        float[][] centers,
        List<int[]> neighborhoods, // Modified in place
        int clustersPerNeighborhood
    ) {
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
            Arrays.sort(neighbors);
            neighborhoods.set(i, neighbors);
        }
    }

    @Override
    int getBestCentroidOffset(float[][] centroids, float[] vector, int vectorIdx, ClusteringAugment augment) {
        assert augment instanceof NeighborsClusteringAugment;

        int centroidIdx = ((NeighborsClusteringAugment) augment).getCentroidIdx(vectorIdx);
        List<int[]> neighborhoods = ((NeighborsClusteringAugment) augment).neighborhoods;

        int bestCentroidOffset = centroidIdx;
        float minDsq = VectorUtil.squareDistance(vector, centroids[centroidIdx]);

        int[] neighborOffsets = neighborhoods.get(centroidIdx);
        for (int neighborOffset : neighborOffsets) {
            float dsq = VectorUtil.squareDistance(vector, centroids[neighborOffset]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = neighborOffset;
            }
        }
        return bestCentroidOffset;
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
        float lambda = 1.0F;
        // TODO: combine these to be more efficient
        float dsq = VectorUtil.squareDistance(vector, centroid);
        float rproj = ESVectorUtil.soarResidual(vector, centroid, residual);
        return dsq + lambda * rproj * rproj / rnorm;
    }

    /**
     * cluster using a lloyd kmeans algorithm that also considers prior clustered neighborhoods when adjusting centroids
     * this also is used to generate the neighborhood aware additional (SOAR) assignments
     *
     * @param vectors the vectors to cluster
     * @param kMeansResult the output object to populate which minimally includes centroids,
     *                     the prior assignments of the given vectors; care should be taken in
     *                     passing in a valid output object with a centroids array that is the size of centroids expected
     *                     and assignments that are the same size as the vectors.  The SOAR assignments are overwritten by this operation.
     * @throws IOException is thrown if vectors is inaccessible
     */
    @Override
    void cluster(FloatVectorValues vectors, KMeansResult kMeansResult) throws IOException {
        float[][] centroids = kMeansResult.centroids();
        int[] assignments = kMeansResult.assignments();

        assert assignments != null;
        assert assignments.length == vectors.size();

        int k = centroids.length;
        List<int[]> neighborhoods = new ArrayList<>(k);
        for (int i = 0; i < k; ++i) {
            neighborhoods.add(null);
        }
        computeNeighborhoods(centroids, neighborhoods, clustersPerNeighborhood);
        ClusteringAugment augment = new NeighborsClusteringAugment(assignments, neighborhoods);
        super.cluster(vectors, kMeansResult, augment);
        kMeansResult.setSoarAssignments(assignSpilled(vectors, neighborhoods, centroids, assignments));
    }

    static class NeighborsClusteringAugment extends ClusteringAugment {
        final List<int[]> neighborhoods;
        final int[] assignments;

        NeighborsClusteringAugment(int[] assignments, List<int[]> neighborhoods) {
            this.neighborhoods = neighborhoods;
            this.assignments = assignments;
        }

        public int getCentroidIdx(int vectorIdx) {
            return this.assignments[vectorIdx];
        }
    }
}
