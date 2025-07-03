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

    // the minimum distance that is considered to be "far enough" to a centroid in order to compute the soar distance.
    // For vectors that are closer than this distance to the centroid, we use the squared distance to find the
    // second closest centroid.
    private static final float SOAR_MIN_DISTANCE = 1e-16f;

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

    private static boolean stepLloyd(
        FloatVectorValues vectors,
        IntToIntFunction translateOrd,
        float[][] centroids,
        float[][] nextCentroids,
        int[] assignments,
        List<NeighborHood> neighborhoods
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
                bestCentroidOffset = getBestCentroidFromNeighbours(centroids, vector, assignment, neighborhoods.get(assignment));
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

    private void computeNeighborhoods(float[][] centers, List<NeighborHood> neighborhoods, int clustersPerNeighborhood) {
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
            if (queue.size() == 0) {
                // no neighbors, skip
                neighborhoods.set(i, NeighborHood.EMPTY);
                continue;
            }
            // consume the queue into the neighbors array and get the maximum intra-cluster distance
            int[] neighbors = new int[queue.size()];
            float maxIntraDistance = queue.topScore();
            int iter = 0;
            while (queue.size() > 0) {
                neighbors[neighbors.length - ++iter] = queue.pop();
            }
            NeighborHood neighborHood = new NeighborHood(neighbors, maxIntraDistance);
            neighborhoods.set(i, neighborHood);
        }
    }

    private int[] assignSpilled(FloatVectorValues vectors, List<NeighborHood> neighborhoods, float[][] centroids, int[] assignments)
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
                assert neighborhoods.get(currAssignment) != null;
                NeighborHood neighborhood = neighborhoods.get(currAssignment);
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

        return spilledAssignments;
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

    record NeighborHood(int[] neighbors, float maxIntraDistance) {
        static final NeighborHood EMPTY = new NeighborHood(new int[0], Float.POSITIVE_INFINITY);
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

        List<NeighborHood> neighborhoods = null;
        // if there are very few centroids, don't bother with neighborhoods or neighbor aware clustering
        if (neighborAware && centroids.length > clustersPerNeighborhood) {
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

    private void cluster(FloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, List<NeighborHood> neighborhoods)
        throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();

        if (k == 1 || k >= n) {
            return;
        }
        IntToIntFunction translateOrd = i -> i;
        FloatVectorValues sampledVectors = vectors;
        if (sampleSize < n) {
            sampledVectors = SampleReader.createSampleReader(vectors, sampleSize, 42L);
            translateOrd = sampledVectors::ordToDoc;
        }
        int[] assignments = kMeansIntermediate.assignments();
        assert assignments.length == n;
        float[][] nextCentroids = new float[centroids.length][vectors.dimension()];
        for (int i = 0; i < maxIterations; i++) {
            // This is potentially sampled, so we need to translate ordinals
            if (stepLloyd(sampledVectors, translateOrd, centroids, nextCentroids, assignments, neighborhoods) == false) {
                break;
            }
        }
        // If we were sampled, do a once over the full set of vectors to finalize the centroids
        if (sampleSize < n) {
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
