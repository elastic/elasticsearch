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
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.MathUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Balanced k-means algorithm that uses a mini-batch approach with OT-based balancing on each mini-batch.
 * Implementation suited to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 */
abstract class BalancedKMeansLocal extends KMeansLocal {

    private final int sampleSize; // the number of training vectors to sample
    private final int maxIterations; // number of iterations, each covering sampleSize vectors divided in minibatches
    private final int sinkhornIterations; // the number of Sinkhorn iterations for the optimal transport problem
    private final float gamma; // initial value of the temperature parameter for the entropic regularization
    private final float alpha; // how much to decrease the temperature parameter for the entropic regularization between iterations
    private final float etaMin; // the minimum value of the temprerature parameter for the entropic regularization
    private final float forgettingFactor; // multiplicative factor in (0, 1], that allows forgetting the old soft assignments
    private final int miniBatchSize; // the mini-batch size
    private final float convergenceRelativeTolerance; // for the relative difference between centroids in two consecutive iterations


    BalancedKMeansLocal(int sampleSize, int maxIterations, float convergenceRelativeTolerance) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
        // These defaults seem stable enough so that we do not need to expose them externally.
        this.sinkhornIterations = 2;
        this.gamma = 0.1f;
        this.alpha = 0.8f;
        this.etaMin = 1e-5f;
        this.forgettingFactor = 0.9f;
        // A positive number means that the actual miniBatchSize will be set to that number.
        // A negative number means that the actual miniBatchSize will be set to k * abs(this.miniBatchSize).
        this.miniBatchSize = -2;
        this.convergenceRelativeTolerance = convergenceRelativeTolerance;
    }

    BalancedKMeansLocal(int sampleSize, int maxIterations) {
        // The convergenceRelativeTolerance default is reasonable for a number of dimensions in the 100s to 1000s.
        // For fewer dimensions, adjust it down.
        this(sampleSize, maxIterations, 1e-2f);
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
        for (int idx = 0; idx < vectors.size(); idx++) {
            float[] vec = vectors.vectorValue(idx);
            ESVectorUtil.linearCombination(1, softAssignments[idx], 1, cumulative_cluster_weights);

            for (int k = 0; k < centroids.length; k++) {
                float[] centroid = centroids[k];
                float learning_rate = 1.f / cumulative_cluster_weights[k];
                float w = learning_rate * softAssignments[idx][k];
                ESVectorUtil.linearCombination(w, vec, 1 - w, centroid);
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

        int[] assignments = kMeansIntermediate.assignments();

        if (k == 1) {
            Arrays.fill(assignments, 0);
            return;
        }

        int miniBatchSizeLocal = (miniBatchSize < 0)? Math.abs(miniBatchSize) * k:  miniBatchSize;

        float[][] distances = new float[miniBatchSizeLocal][k]; // distances from sampledVectors to centroids
        float[][] softAssignments = new float[miniBatchSizeLocal][k]; // soft-assignments of sampledVectors to centroids

        float[] cumulative_cluster_weights = new float[k]; // maintains soft cluster counts for each cluster.
                                                           // Used to compute the learning rate in the SGD update of the centroids
        Arrays.fill(cumulative_cluster_weights, 1); //  start with one to avoid 1 / epsilon numerical issues.

        float eta = this.gamma;

        SinkhornIterations sinkhorn = new SinkhornIterations(miniBatchSizeLocal, k);
        OnlineQuantileEstimator medianEstimator = null; // We cannot initialize the estimator now because we need to know its range.

        ClusteringFloatVectorValuesSlice sampledVectors = new ClusteringFloatVectorValuesSlice(vectors, miniBatchSizeLocal);

        float[][] oldCentroids = new float[k][vectors.dimension()];
        deepCopy(centroids, oldCentroids);

        for (int epoch = 0; epoch < maxIterations; epoch++) {
            for (int batch = 0; batch < sampleSize; batch += miniBatchSizeLocal) {
                // This simple version performs sampling with replacement (that is, two batches can share vectors but within a batch
                // the vectors are unique) for simplicity. To be more precise, we could sample without replacement but the current
                // approach seems good enough.
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
                sinkhorn.compute(distances, sinkhornIterations, eps, softAssignments);

                // Update the centroids using SGD.
                updateCentroids(sampledVectors, ordTranslator, cumulative_cluster_weights, softAssignments, centroids);
            }
            eta *= alpha;
            for (int kk = 0; kk < k; kk++) {
                cumulative_cluster_weights[kk] *= forgettingFactor;
            }

            if (normalizedFrobeniusNorm(centroids, oldCentroids) < convergenceRelativeTolerance) {
                break;
            } else {
                deepCopy(centroids, oldCentroids);
            }
        }

        assert assignments.length == n;
        FixedBitSet[] centroidChangedSlices = new FixedBitSet[numWorkers()];
        for (int i = 0; i < numWorkers(); i++) {
            centroidChangedSlices[i] = new FixedBitSet(centroids.length);
        }

        assign(vectors, i -> i, centroids, centroidChangedSlices, assignments, neighborhoods);
    }

    private static void deepCopy(float[][] source, float[][] destination) {
        for (int i = 0; i < source.length; i++) {
            System.arraycopy(source[i], 0, destination[i], 0, source[i].length);
        }
    }

    // Computes: (sum_i sum_j pow(vecs1[i][j] - vecs2[i][j], 2)) / (sum_i sum_j pow(vecs2[i][j], 2))
    private static float normalizedFrobeniusNorm(float[][] vecs1, float[][] vecs2) {
        assert vecs1.length == vecs2.length;
        float result = 0;
        float norm2 = 0;
        for (int i = 0; i < vecs1.length; i++) {
            result += ESVectorUtil.squareDistance(vecs1[i], vecs2[i]);
            norm2 += ESVectorUtil.dotProduct(vecs2[i], vecs2[i]);
        }
        return MathUtils.sqrt(result / norm2);
    }

    /**
     * helper that calls {@link BalancedKMeansLocal#cluster(ClusteringFloatVectorValues, KMeansIntermediate)} given a set of initialized
     * centroids, this call is not neighbor aware
     *
     * @param vectors the vectors to cluster
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static void cluster(ClusteringFloatVectorValues vectors, float[][] centroids, int sampleSize, int maxIterations)
        throws IOException {
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, new int[vectors.size()], vectors::ordToDoc);
        BalancedKMeansLocal kMeans = new BalancedKMeansLocalSerial(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }
}
