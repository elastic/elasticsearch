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
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.index.codec.vectors.SampleReader;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
class KMeansLocal {

    // the minimum distance that is considered to be "far enough" to a centroid in order to compute the soar distance.
    // For vectors that are closer than this distance to the centroid, we use the squared distance to find the
    // second closest centroid.
    private static final float SOAR_MIN_DISTANCE = 1e-16f;

    final int sampleSize;
    final int maxIterations;

    KMeansLocal(int sampleSize, int maxIterations) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
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

    private static boolean stepLloyd(
        FloatVectorValues vectors,
        IntToIntFunction translateOrd,
        float[][] centroids,
        float[][] nextCentroids,
        int[] assignments,
        NeighborHood[] neighborhoods
    ) throws IOException {
        boolean changed = false;
        int dim = vectors.dimension();
        int[] centroidCounts = new int[centroids.length];

        for (float[] nextCentroid : nextCentroids) {
            Arrays.fill(nextCentroid, 0.0f);
        }

        for (int idx = 0; idx < vectors.size(); idx++) {
            float[] vector = vectors.vectorValue(idx);
            int vectorOrd = translateOrd.apply(idx);
            final int assignment = assignments[vectorOrd];
            final int bestCentroidOffset;
            if (neighborhoods != null) {
                bestCentroidOffset = getBestCentroidFromNeighbours(centroids, vector, assignment, neighborhoods[assignment]);
            } else {
                bestCentroidOffset = getBestCentroid(centroids, vector);
            }
            if (assignment != bestCentroidOffset) {
                assignments[vectorOrd] = bestCentroidOffset;
                changed = true;
            }
            centroidCounts[bestCentroidOffset]++;
            for (int d = 0; d < dim; d++) {
                nextCentroids[bestCentroidOffset][d] += vector[d];
            }
        }

        for (int clusterIdx = 0; clusterIdx < centroids.length; clusterIdx++) {
            if (centroidCounts[clusterIdx] > 0) {
                float countF = (float) centroidCounts[clusterIdx];
                for (int d = 0; d < dim; d++) {
                    centroids[clusterIdx][d] = nextCentroids[clusterIdx][d] / countF;
                }
            }
        }

        return changed;
    }

    private static int getBestCentroidFromNeighbours(float[][] centroids, float[] vector, int centroidIdx, NeighborHood neighborhood) {
        int bestCentroidOffset = centroidIdx;
        assert centroidIdx >= 0 && centroidIdx < centroids.length;
        float minDsq = VectorUtil.squareDistance(vector, centroids[centroidIdx]);
        for (int i = 0; i < neighborhood.neighbors.length; i++) {
            int offset = neighborhood.neighbors[i];
            // float score = neighborhood.scores[i];
            assert offset >= 0 && offset < centroids.length : "Invalid neighbor offset: " + offset;
            if (minDsq < neighborhood.maxIntraDistance) {
                // if the distance found is smaller than the maximum intra-cluster distance
                // we don't consider it for further re-assignment
                return bestCentroidOffset;
            }
            // compute the distance to the centroid
            float dsq = VectorUtil.squareDistance(vector, centroids[offset]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = offset;
            }
        }
        return bestCentroidOffset;
    }

    private static int getBestCentroid(float[][] centroids, float[] vector) {
        int bestCentroidOffset = 0;
        float minDsq = Float.MAX_VALUE;
        for (int i = 0; i < centroids.length; i++) {
            float dsq = VectorUtil.squareDistance(vector, centroids[i]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = i;
            }
        }
        return bestCentroidOffset;
    }

    private NeighborHood[] computeNeighborhoods(float[][] centers, int clustersPerNeighborhood) {
        int k = centers.length;
        assert k > clustersPerNeighborhood;
        NeighborQueue[] neighborQueues = new NeighborQueue[k];
        for (int i = 0; i < k; i++) {
            neighborQueues[i] = new NeighborQueue(clustersPerNeighborhood, true);
        }
        for (int i = 0; i < k - 1; i++) {
            for (int j = i + 1; j < k; j++) {
                float dsq = VectorUtil.squareDistance(centers[i], centers[j]);
                neighborQueues[j].insertWithOverflow(i, dsq);
                neighborQueues[i].insertWithOverflow(j, dsq);
            }
        }

        NeighborHood[] neighborhoods = new NeighborHood[k];
        for (int i = 0; i < k; i++) {
            NeighborQueue queue = neighborQueues[i];
            if (queue.size() == 0) {
                // no neighbors, skip
                neighborhoods[i] = NeighborHood.EMPTY;
                continue;
            }
            // consume the queue into the neighbors array and get the maximum intra-cluster distance
            int[] neighbors = new int[queue.size()];
            float maxIntraDistance = queue.topScore();
            int iter = 0;
            while (queue.size() > 0) {
                neighbors[neighbors.length - ++iter] = queue.pop();
            }
            neighborhoods[i] = new NeighborHood(neighbors, maxIntraDistance);
        }
        return neighborhoods;
    }

    private void assignSpilled(
        FloatVectorValues vectors,
        KMeansIntermediate kmeansIntermediate,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        // SOAR uses an adjusted distance for assigning spilled documents which is
        // given by:
        //
        // soar(x, c) = ||x - c||^2 + lambda * ((x - c_1)^t (x - c))^2 / ||x - c_1||^2
        //
        // Here, x is the document, c is the nearest centroid, and c_1 is the first
        // centroid the document was assigned to. The document is assigned to the
        // cluster with the smallest soar(x, c).
        int[] assignments = kmeansIntermediate.assignments();
        assert assignments != null;
        assert assignments.length == vectors.size();
        int[] spilledAssignments = kmeansIntermediate.soarAssignments();
        assert spilledAssignments != null;
        assert spilledAssignments.length == vectors.size();
        float[][] centroids = kmeansIntermediate.centroids();

        float[] diffs = new float[vectors.dimension()];
        for (int i = 0; i < vectors.size(); i++) {
            float[] vector = vectors.vectorValue(i);

            int currAssignment = assignments[i];
            float[] currentCentroid = centroids[currAssignment];

            // TODO: cache these?
            float vectorCentroidDist = VectorUtil.squareDistance(vector, currentCentroid);

            if (vectorCentroidDist > SOAR_MIN_DISTANCE) {
                for (int j = 0; j < vectors.dimension(); j++) {
                    float diff = vector[j] - currentCentroid[j];
                    diffs[j] = diff;
                }
            }

            int bestAssignment = -1;
            float minSoar = Float.MAX_VALUE;
            int centroidCount = centroids.length;
            IntToIntFunction centroidOrds = c -> c;
            if (neighborhoods != null) {
                assert neighborhoods[currAssignment] != null;
                NeighborHood neighborhood = neighborhoods[currAssignment];
                centroidCount = neighborhood.neighbors.length;
                centroidOrds = c -> neighborhood.neighbors[c];
            }
            for (int j = 0; j < centroidCount; j++) {
                int centroidOrd = centroidOrds.apply(j);
                if (centroidOrd == currAssignment) {
                    continue; // skip the current assignment
                }
                float[] centroid = centroids[centroidOrd];
                float soar;
                if (vectorCentroidDist > SOAR_MIN_DISTANCE) {
                    soar = ESVectorUtil.soarDistance(vector, centroid, diffs, soarLambda, vectorCentroidDist);
                } else {
                    // if the vector is very close to the centroid, we look for the second-nearest centroid
                    soar = VectorUtil.squareDistance(vector, centroid);
                }
                if (soar < minSoar) {
                    minSoar = soar;
                    bestAssignment = centroidOrd;
                }
            }

            assert bestAssignment != -1 : "Failed to assign soar vector to centroid";
            spilledAssignments[i] = bestAssignment;
        }
    }

    record NeighborHood(int[] neighbors, float maxIntraDistance) {
        static final NeighborHood EMPTY = new NeighborHood(new int[0], Float.POSITIVE_INFINITY);
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
    void cluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, int clustersPerNeighborhood, float soarLambda)
        throws IOException {
        if (clustersPerNeighborhood < 2) {
            throw new IllegalArgumentException("clustersPerNeighborhood must be at least 2, got [" + clustersPerNeighborhood + "]");
        }
        doCluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);
    }

    private void doCluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, int clustersPerNeighborhood, float soarLambda)
        throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        boolean neighborAware = clustersPerNeighborhood != -1 && centroids.length > 1;
        NeighborHood[] neighborhoods = null;
        // if there are very few centroids, don't bother with neighborhoods or neighbor aware clustering
        if (neighborAware && centroids.length > clustersPerNeighborhood) {
            neighborhoods = computeNeighborhoods(centroids, clustersPerNeighborhood);
        }
        cluster(vectors, kMeansIntermediate, neighborhoods);
        if (neighborAware) {
            assert kMeansIntermediate.soarAssignments().length == 0;
            kMeansIntermediate.setSoarAssignments(new int[vectors.size()]);
            assignSpilled(vectors, kMeansIntermediate, neighborhoods, soarLambda);
        }
    }

    private void cluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, NeighborHood[] neighborhoods)
        throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();
        int[] assignments = kMeansIntermediate.assignments();

        if (k == 1) {
            Arrays.fill(assignments, 0);
            return;
        }
        IntToIntFunction translateOrd = i -> i;
        FloatVectorValues sampledVectors = vectors;
        if (sampleSize < n) {
            sampledVectors = SampleReader.createSampleReader(vectors, sampleSize, 42L);
            translateOrd = sampledVectors::ordToDoc;
        }

        assert assignments.length == n;
        float[][] nextCentroids = new float[centroids.length][vectors.dimension()];
        for (int i = 0; i < maxIterations; i++) {
            // This is potentially sampled, so we need to translate ordinals
            if (stepLloyd(sampledVectors, translateOrd, centroids, nextCentroids, assignments, neighborhoods) == false) {
                break;
            }
        }
        // If we were sampled, do a once over the full set of vectors to finalize the centroids
        if (sampleSize < n || maxIterations == 0) {
            // No ordinal translation needed here, we are using the full set of vectors
            stepLloyd(vectors, i -> i, centroids, nextCentroids, assignments, neighborhoods);
        }
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
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, new int[vectors.size()], vectors::ordToDoc);
        KMeansLocal kMeans = new KMeansLocal(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }

}
