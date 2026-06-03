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

import java.io.IOException;
import java.util.Arrays;

/**
 * Balanced k-means algorithm that uses a mini-batch approach with OT-based balancing on each mini-batch.
 * Implementation suited to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
abstract class BalancedOTKMeansLocal<V> extends KMeansLocal<V> {

    private final int sampleSize; // the number of training vectors to sample
    private final int maxIterations; // number of iterations, each covering sampleSize vectors divided in minibatches
    private final int sinkhornIterations; // the number of Sinkhorn iterations for the optimal transport problem
    private final float etaInit; // initial value of the temperature parameter for the entropic regularization, should be larger than etaMin
    private final float etaMultiplicativeUpdate; // how much to decrease the temperature parameter for the entropic regularization between
                                                 // iterations, should be in (0, 1)
    private final float etaMin; // the minimum value of the temperature parameter for the entropic regularization, should be larger than 0
    private final float forgettingFactor; // multiplicative factor in (0, 1], that allows forgetting the old soft assignments
    private final int miniBatchSize; // the mini-batch size

    BalancedOTKMeansLocal(CentroidOps<V> ops, int sampleSize, int maxIterations) {
        super(ops);
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
        // These defaults seem stable enough so that we do not need to expose them externally.
        this.sinkhornIterations = 2;
        this.etaInit = 1e-2f;
        this.etaMultiplicativeUpdate = 0.8f;
        this.etaMin = 1e-5f;
        this.forgettingFactor = 0.9f;
        // A positive number means that the actual miniBatchSize will be set to that number.
        // A negative number means that the actual miniBatchSize will be set to k * abs(this.miniBatchSize).
        this.miniBatchSize = -2;
    }

    /** Number of workers to use for parallelism */
    protected abstract int numWorkers();

    /** compute the distance from every vector to every centroid */
    private void computeDistances(ClusteringVectorValues<V> vectors, V[] centroids, float[][] distances) throws IOException {
        CentroidAssignment.computeSquaredDistances(vectors, ops, 0, vectors.size(), centroids, distances);
    }

    /** update the centroids using stochastic gradient descent */
    protected void updateCentroids(
        ClusteringVectorValues<V> vectors,
        float[] cumulativeClusterWeights,
        float[][] softAssignments,
        V[] centroids,
        CentroidOps.MutationContext<V> sgdContext
    ) throws IOException {
        int k = centroids.length;
        int dim = vectors.dimension();

        float[][] batchSums = new float[k][dim];
        float[] batchWeights = new float[k];

        // Accumulate the raw Sinkhorn weights via fast FMA loop
        for (int idx = 0; idx < vectors.size(); idx++) {
            V vec = vectors.vectorValue(idx);
            for (int c = 0; c < k; c++) {
                float weight = softAssignments[idx][c];
                if (weight > 1e-7f) {
                    batchWeights[c] += weight;
                    ops.addScaled(weight, vec, batchSums[c]);
                }
            }
        }

        // Apply the k scaling and update
        for (int c = 0; c < k; c++) {
            if (batchWeights[c] > 0) {
                float scaledBatchWeight = batchWeights[c] * k;
                cumulativeClusterWeights[c] += scaledBatchWeight;
                float learningRate = scaledBatchWeight / cumulativeClusterWeights[c];
                float lrNorm = learningRate / batchWeights[c];
                ESVectorUtil.linearCombination(lrNorm, batchSums[c], 1.0f - learningRate, sgdContext.floatCentroid(c));
            }
        }

        sgdContext.syncToNative();
    }

    /** assign to each vector the closest centroid */
    protected abstract void assign(
        ClusteringVectorValues<V> vectors,
        IntToIntFunction ordTranslator,
        V[] centroids,
        FixedBitSet[] centroidChangedSlices,
        int[] assignments,
        NeighborHood[] neighborHoods
    ) throws IOException;

    @Override
    protected void innerCluster(ClusteringVectorValues<V> vectors, KMeansIntermediate<V> kMeansIntermediate, NeighborHood[] neighborhoods)
        throws IOException {
        assert neighborhoods == null;

        V[] centroids = kMeansIntermediate.centroids();
        int k = centroids.length;
        int n = vectors.size();

        int[] assignments = kMeansIntermediate.assignments();

        if (k == 1) {
            Arrays.fill(assignments, 0);
            return;
        }

        int miniBatchSizeLocal = (miniBatchSize < 0) ? Math.absExact(miniBatchSize) * k : miniBatchSize;
        miniBatchSizeLocal = Math.min(miniBatchSizeLocal, n);

        // Tolerance for the relative difference between centroids in two consecutive iterations. Used to check convergence
        float convergenceRelativeTolerance = (vectors.dimension() < 100) ? 1e-3f : 1e-2f;

        float[][] distances = new float[miniBatchSizeLocal][k]; // distances from sampledVectors in the mini batch to centroids
        float[][] softAssignments = new float[miniBatchSizeLocal][k]; // soft-assignments of sampledVectors in the mini batch to centroids
        int[] miniBatchSamples = new int[miniBatchSizeLocal]; // stores the samples in the mini batch

        float[] cumulativeClusterWeights = new float[k]; // maintains soft cluster counts for each cluster.
                                                         // Used to compute the learning rate in the SGD update of the centroids

        float eta = this.etaInit;

        SinkhornIterations sinkhorn = new SinkhornIterations(miniBatchSizeLocal, k);
        OnlineQuantileEstimator medianEstimator = null; // We cannot initialize the estimator now because we need to know its range.

        V[] oldCentroids = ops.newCentroidArray(k, vectors.dimension());
        ops.deepCopy(centroids, oldCentroids);

        // Scoped float-precision mutation context for SGD updates.
        // For float centroids this is zero-cost; for byte centroids it maintains a float shadow
        // that is synced back to native on each call to syncToNative() and released on close().
        try (var sgdContext = ops.newMutationContext(centroids, vectors.dimension())) {
            int t = 0;
            for (int epoch = 0; epoch < maxIterations; epoch++) {
                for (int batch = 0; batch < sampleSize; batch += miniBatchSizeLocal) {
                    // This simple version performs sampling with replacement (that is, two batches can share vectors but within a batch
                    // the vectors are unique) for simplicity. To be more precise, we could sample without replacement but the current
                    // approach seems good enough.
                    ClusteringVectorValues<V> sampledVectors = ClusteringVectorValuesSlice.createRandomSlice(
                        vectors,
                        miniBatchSizeLocal,
                        t++,
                        miniBatchSamples
                    );

                    computeDistances(sampledVectors, centroids, distances);

                    if (medianEstimator == null) {
                        // Getting the range of the median estimator from the first batch.
                        // Since the estimator snaps the values to the provided range, this is a safe operation.
                        float maxDistance = Float.NEGATIVE_INFINITY;
                        for (float[] dist : distances) {
                            for (float d : dist) {
                                maxDistance = Math.max(maxDistance, d);
                            }
                        }
                        medianEstimator = new OnlineQuantileEstimator(0.5f, 0, maxDistance, 0.0001f, 42L);
                    }

                    for (float[] dist : distances) {
                        medianEstimator.updateEstimate(dist);
                    }

                    float currentMedian = medianEstimator.getEstimate();
                    float eps = Math.max(eta * currentMedian, etaMin);
                    // Perform Shinkhorn iterations in log domain to obtain a balanced assignment.
                    sinkhorn.compute(distances, sinkhornIterations, eps, softAssignments);

                    // Update the centroids using SGD.
                    updateCentroids(sampledVectors, cumulativeClusterWeights, softAssignments, centroids, sgdContext);
                }
                eta *= etaMultiplicativeUpdate;
                for (int kk = 0; kk < k; kk++) {
                    cumulativeClusterWeights[kk] *= forgettingFactor;
                }

                if (ops.normalizedFrobeniusNorm(centroids, oldCentroids) < convergenceRelativeTolerance) {
                    break;
                } else {
                    ops.deepCopy(centroids, oldCentroids);
                }
            }
        }

        assert assignments.length == n;
        FixedBitSet[] centroidChangedSlices = new FixedBitSet[numWorkers()];
        for (int i = 0; i < numWorkers(); i++) {
            centroidChangedSlices[i] = new FixedBitSet(centroids.length);
        }

        assign(vectors, i -> i, centroids, centroidChangedSlices, assignments, neighborhoods);
        int[] centroidCounts = new int[centroids.length];
        CentroidOps.AccumulatorState<V> accumulatorState = ops.newAccumulatorState(centroids, centroids.length, vectors.dimension());
        CentroidAssignment.updateCentroids(
            vectors,
            centroids,
            i -> i,
            centroidChangedSlices,
            centroidCounts,
            assignments,
            accumulatorState
        );
    }

    /**
     * helper that calls {@link BalancedOTKMeansLocal#cluster(ClusteringVectorValues, KMeansIntermediate)} given a set of initialized
     * centroids, this call is not neighbor aware
     *
     * @param vectors the vectors to cluster
     * @param ops the type of vectors such as float and associated operations
     * @param centroids the initialized centroids to be shifted using k-means
     * @param sampleSize the subset of vectors to use when shifting centroids
     * @param maxIterations the max iterations to shift centroids
     */
    public static <V> void cluster(ClusteringVectorValues<V> vectors, CentroidOps<V> ops, V[] centroids, int sampleSize, int maxIterations)
        throws IOException {
        KMeansIntermediate<V> kMeansIntermediate = new KMeansIntermediate<>(centroids, new int[vectors.size()], vectors::ordToDoc);
        BalancedOTKMeansLocal<V> kMeans = new BalancedOTKMeansLocalSerial<>(ops, sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }
}
