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
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
class KMeansLocal {

    // the minimum distance that is considered to be "far enough" to a centroid in order to compute the soar distance.
    // For vectors that are closer than this distance to the centroid don't get spilled because they are well represented
    // by the centroid itself. In many cases, it indicates a degenerated distribution, e.g the cluster is composed of the
    // many equal vectors.
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
        FixedBitSet centroidChanged,
        int[] centroidCounts,
        int[] assignments,
        NeighborHood[] neighborhoods
    ) throws IOException {
        boolean changed = false;
        int dim = vectors.dimension();
        centroidChanged.clear();
        final float[] distances = new float[4];
        for (int idx = 0; idx < vectors.size(); idx++) {
            float[] vector = vectors.vectorValue(idx);
            int vectorOrd = translateOrd.apply(idx);
            final int assignment = assignments[vectorOrd];
            final int bestCentroidOffset;
            if (neighborhoods != null) {
                bestCentroidOffset = getBestCentroidFromNeighbours(centroids, vector, assignment, neighborhoods[assignment], distances);
            } else {
                bestCentroidOffset = getBestCentroid(centroids, vector, distances);
            }
            if (assignment != bestCentroidOffset) {
                if (assignment != -1) {
                    centroidChanged.set(assignment);
                }
                centroidChanged.set(bestCentroidOffset);
                assignments[vectorOrd] = bestCentroidOffset;
                changed = true;
            }
        }
        if (changed) {
            Arrays.fill(centroidCounts, 0);
            for (int idx = 0; idx < vectors.size(); idx++) {
                final int assignment = assignments[translateOrd.apply(idx)];
                if (centroidChanged.get(assignment)) {
                    float[] centroid = centroids[assignment];
                    if (centroidCounts[assignment]++ == 0) {
                        Arrays.fill(centroid, 0.0f);
                    }
                    float[] vector = vectors.vectorValue(idx);
                    for (int d = 0; d < dim; d++) {
                        centroid[d] += vector[d];
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

        return changed;
    }

    private static int getBestCentroidFromNeighbours(
        float[][] centroids,
        float[] vector,
        int centroidIdx,
        NeighborHood neighborhood,
        float[] distances
    ) {
        final int limit = neighborhood.neighbors().length - 3;
        int bestCentroidOffset = centroidIdx;
        assert centroidIdx >= 0 && centroidIdx < centroids.length;
        float minDsq = VectorUtil.squareDistance(vector, centroids[centroidIdx]);
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
            // float score = neighborhood.scores[i];
            assert offset >= 0 && offset < centroids.length : "Invalid neighbor offset: " + offset;
            // compute the distance to the centroid
            float dsq = VectorUtil.squareDistance(vector, centroids[offset]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = offset;
            }
        }
        return bestCentroidOffset;
    }

    private static int getBestCentroid(float[][] centroids, float[] vector, float[] distances) {
        final int limit = centroids.length - 3;
        int bestCentroidOffset = 0;
        float minDsq = Float.MAX_VALUE;
        int i = 0;
        for (; i < limit; i += 4) {
            ESVectorUtil.squareDistanceBulk(vector, centroids[i], centroids[i + 1], centroids[i + 2], centroids[i + 3], distances);
            for (int j = 0; j < distances.length; j++) {
                float dsq = distances[j];
                if (dsq < minDsq) {
                    minDsq = dsq;
                    bestCentroidOffset = i + j;
                }
            }
        }
        for (; i < centroids.length; i++) {
            float dsq = VectorUtil.squareDistance(vector, centroids[i]);
            if (dsq < minDsq) {
                minDsq = dsq;
                bestCentroidOffset = i;
            }
        }
        return bestCentroidOffset;
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
        final float[] distances = new float[4];
        for (int i = 0; i < vectors.size(); i++) {
            float[] vector = vectors.vectorValue(i);
            int currAssignment = assignments[i];
            float[] currentCentroid = centroids[currAssignment];
            // TODO: cache these?
            float vectorCentroidDist = VectorUtil.squareDistance(vector, currentCentroid);
            if (vectorCentroidDist <= SOAR_MIN_DISTANCE) {
                spilledAssignments[i] = NO_SOAR_ASSIGNMENT; // no SOAR assignment
                continue;
            }

            for (int j = 0; j < vectors.dimension(); j++) {
                diffs[j] = vector[j] - currentCentroid[j];
            }
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
            spilledAssignments[i] = bestAssignment;
        }
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
            neighborhoods = NeighborHood.computeNeighborhoods(centroids, clustersPerNeighborhood);
        }
        cluster(vectors, kMeansIntermediate, neighborhoods);
        if (neighborAware && soarLambda >= 0) {
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
            sampledVectors = FloatVectorValuesSlice.createRandomSlice(vectors, sampleSize, 42L);
            translateOrd = sampledVectors::ordToDoc;
        }

        assert assignments.length == n;
        FixedBitSet centroidChanged = new FixedBitSet(centroids.length);
        int[] centroidCounts = new int[centroids.length];
        for (int i = 0; i < maxIterations; i++) {
            // This is potentially sampled, so we need to translate ordinals
            if (stepLloyd(sampledVectors, translateOrd, centroids, centroidChanged, centroidCounts, assignments, neighborhoods) == false) {
                break;
            }
        }
        // If we were sampled, do a once over the full set of vectors to finalize the centroids
        if (sampleSize < n || maxIterations == 0) {
            // No ordinal translation needed here, we are using the full set of vectors
            stepLloyd(vectors, i -> i, centroids, centroidChanged, centroidCounts, assignments, neighborhoods);
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
