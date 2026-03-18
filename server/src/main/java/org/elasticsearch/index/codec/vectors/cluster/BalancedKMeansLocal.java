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

/**
 * k-means implementation specific to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
abstract class BalancedKMeansLocal extends KMeansLocal {

    private final int sampleSize;
    private final int maxIterations;
    private final int sinkhornIterations;
    private final float gamma;
    private final float alpha;
    private final float etaMin;
    private final float forgettingFactor;

    BalancedKMeansLocal(int sampleSize, int maxIterations) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
        // TODO these values are hardcoded for now
        this.sinkhornIterations = 10;
        this.gamma = 0.1f;
        this.alpha = 0.8f;
        this.etaMin = 1e-5f;
        this.forgettingFactor = 0.9f;
    }

    /** Number of workers to use for parallelism **/
    protected abstract int numWorkers();

    /** compute the distance from every vector to every centroid **/
    private void computeDistances(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
        float[][] centroids,
        float[][] distances
    ) throws IOException {
        vectors.computeSquaredDistances(0, vectors.size(), centroids, ordTranslator, distances);
    }

    /** update the centroids using stochastic gradient descent **/
    protected void updateCentroids(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
        float[] cumulative_cluster_weights,
        float[][] softAssignments,
        float[][] centroids
    ) throws IOException {
        int dim = vectors.dimension();
        for (int idx = 0; idx < vectors.size(); idx++) {
            float[] vec = vectors.vectorValue(idx);
            for (int k = 0; k < centroids.length; k++) {
                cumulative_cluster_weights[k] += softAssignments[idx][k];
            }

            for (int k = 0; k < centroids.length; k++) {
                float[] centroid = centroids[k];
                float learning_rate = 1.f / cumulative_cluster_weights[k];
                float w = learning_rate * softAssignments[idx][k];
                for (int d = 0; d < dim; d++) {
                    centroid[d] += w * (vec[d] - centroid[d]);
                }
            }
        }
    }

    /** assign to each vector the closest centroid **/
    protected abstract void assign(
        ClusteringFloatVectorValues vectors,
        IntToIntFunction ordTranslator,
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

    @Override
    protected void innerCluster(ClusteringFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate, NeighborHood[] neighborhoods)
        throws IOException {
        float[][] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();
        int miniBatchSize = 2 * k;
        int[] assignments = kMeansIntermediate.assignments();

        if (k == 1) {
            Arrays.fill(assignments, 0);
            return;
        }

        float[][] distances = new float[miniBatchSize][k]; // distances from sampledVectors to centroids
        float[][] softAssignments = new float[miniBatchSize][k]; // soft-assignments of sampledVectors to centroids

        float[] cumulative_cluster_weights = new float[k]; // maintains soft cluster counts for each cluster.
                                                           // Used to compute the learning rate in the SGD update of the centroids
        Arrays.fill(cumulative_cluster_weights, 1); //  start with one to avoid 1 / epsilon numerical issues.

        float eta = this.gamma;

        SinkhornIterations sinkhorn = new SinkhornIterations(miniBatchSize, k);
        OnlineQuantileEstimator medianEstimator = null; // We cannot initialize the estimator now because we need to know its range.

        ClusteringFloatVectorValuesSlice sampledVectors = new ClusteringFloatVectorValuesSlice(vectors, miniBatchSize);

        for (int i = 0; i < maxIterations; i++) {
            for (int batch = 0; batch < sampleSize; batch += miniBatchSize) {
                // This simple version performs sampling with replacement (that is, two batches can share vectors) for simplicity.
                // To be more precise, we could sample without replacement but the current approach seems good enough.
                sampledVectors.updateRandomSlice(batch);
                IntToIntFunction ordTranslator = sampledVectors::ordToDoc;

                computeDistances(sampledVectors, ordTranslator, centroids, distances);

                if (medianEstimator == null) {
                    // Getting the range of the median estimator from the first batch.
                    // Since the estimator snaps the values to the provided range, this is a safe operation.
                    float maxDistance = Float.NEGATIVE_INFINITY;
                    for (float[] dist: distances) {
                        for (float d : dist) {
                            maxDistance = Math.max(maxDistance, d);
                        }
                    }
                    medianEstimator = new OnlineQuantileEstimator(0.5f, 0, maxDistance, 0.0001f, 42L);
                }

                for (float[] dist: distances) {
                    medianEstimator.updateEstimate(dist);
                }

                float current_median = medianEstimator.getEstimate();
                float eps = Math.max(eta * current_median, etaMin);
                // Perform Shinkhorn iterations in log domain to obtain a balanced assignment.
                int iterations = (i == maxIterations - 1)? sinkhornIterations: 2 * sinkhornIterations;
                sinkhorn.compute(distances, iterations, eps, softAssignments);

                // Update the centroids using SGD.
                updateCentroids(sampledVectors, ordTranslator, cumulative_cluster_weights, softAssignments, centroids);
            }
            eta *= alpha;
            for (int kk = 0; kk < k; kk++) {
                cumulative_cluster_weights[kk] *= forgettingFactor;
            }
        }

        assert assignments.length == n;
        FixedBitSet[] centroidChangedSlices = new FixedBitSet[numWorkers()];
        for (int i = 0; i < numWorkers(); i++) {
            centroidChangedSlices[i] = new FixedBitSet(centroids.length);
        }

        assign(vectors, i -> i, centroids, centroidChangedSlices, assignments, neighborhoods);

        // If we were sampled, do a once over the full set of vectors to finalize the centroids
//        if (sampleSize < n || maxIterations == 0) {
//            centroidChanged.clear();
//            if (neighborhoods != null) {
//                return vectors.bestCentroidsFromNeighbours(
//                    startOrd,
//                    endOrd,
//                    centroids,
//                    ordTranslator,
//                    centroidChanged,
//                    neighborhoods,
//                    assignments
//                );
//            } else {
//                return vectors.bestCentroids(startOrd, endOrd, centroids, ordTranslator, centroidChanged, assignments);
//            }
//            // No ordinal translation needed here, we are using the full set of vectors
//            if (stepLloyd(vectors, i -> i, centroids, centroidChangedSlices, assignments, neighborhoods)) {
//                sampledVectors.updateCentroids(centroids, ordTranslator, centroidChangedSlices, centroidCounts, assignments);
//            }
//        }
    }



    /**
     * helper that calls {@link BalancedKMeansLocal#cluster(ClusteringFloatVectorValues, KMeansIntermediate)} given a set of initialized centroids,
     * this call is not neighbor aware
     *
     * @param vectors the vectors to cluster
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static void cluster(ClusteringFloatVectorValues vectors, float[][] centroids, int sampleSize, int maxIterations)
        throws IOException {
//        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, new int[vectors.size()], vectors::ordToDoc);
//        BalancedKMeansLocal kMeans = new LloydKMeansLocalSerial(sampleSize, maxIterations);
//        kMeans.cluster(vectors, kMeansIntermediate);
    }
}
