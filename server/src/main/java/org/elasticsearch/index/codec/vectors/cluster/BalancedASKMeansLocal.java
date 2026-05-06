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
 * Balanced k-means algorithm that uses a mini-batch approach with an L2 regularization over the cluster sizes.
 * This algorithm should be used to refine a reasonably balanced solution, such as the ones delivered by BalancedOTKMeansLocal.
 * As a standalone and start-from-scratch algorithm, it is not very good. It does perform refinement well and fast.
 * Implementation suited to the needs of the {@link HierarchicalKMeans} algorithm that deals specifically
 * with finalizing nearby pre-established clusters and generate
 * <a href="https://research.google/blog/soar-new-algorithms-for-even-faster-vector-search-with-scann/">SOAR</a> assignments.
 * The implementation relies on the use of neighborhoods: only a subset of clusters, given by the neighborhood, is considered when
 * assigning each vector.
 * The regularization augments each distance by a component proportional to the current size of the corersponding cluster,
 * so we scale this additional term by the median distance to centroids in the mini batch to balance both terms.
 */
abstract class BalancedASKMeansLocal extends KMeansLocal {

    private final int sampleSize; // the number of training vectors to sample
    private final int maxIterations; // number of iterations, each covering sampleSize vectors divided in minibatches
    private final float beta; // the amount of regularization used
    private final float forgettingFactor; // multiplicative factor in (0, 1], that allows forgetting the old soft assignments
    private final int miniBatchSize; // the mini-batch size

    BalancedASKMeansLocal(int sampleSize, int maxIterations) {
        this.sampleSize = sampleSize;
        this.maxIterations = maxIterations;
        // These defaults seem stable enough so that we do not need to expose them externally.
        this.beta = 1f; // setting this parameter to 1 amounts to an equal balance of the distance and cluster-size-regularization terms
        this.forgettingFactor = 0.9f;
        // A positive number means that the actual miniBatchSize will be set to that number.
        // A negative number means that the actual miniBatchSize will be set to k * abs(this.miniBatchSize).
        this.miniBatchSize = -2;
    }

    /** Number of workers to use for parallelism **/
    protected abstract int numWorkers();

    /** compute the distance from every vector to every centroid **/
    private void computeDistances(
        ClusteringFloatVectorValues vectors,
        float[][] centroids,
        IntToIntFunction assigner,
        NeighborHood[] neighborhoods,
        float[][] distances
    ) throws IOException {
        if (neighborhoods == null) {
            vectors.computeSquaredDistances(0, vectors.size(), centroids, distances);
        } else {
            vectors.computeSquaredDistancesFromNeighbors(0, vectors.size(), centroids, assigner, neighborhoods, distances);
        }
    }

    private void assignMiniBatch(
        float[][] distances,
        float[] cumulativeClusterWeights,
        float weightClusterSizes,
        IntToIntFunction assigner,
        NeighborHood[] neighborhoods,
        int[] localAssignments
    ) {
        if (neighborhoods == null) {
            for (int i = 0; i < distances.length; i++) {
                ESVectorUtil.linearCombination(weightClusterSizes, cumulativeClusterWeights, 1, distances[i]);
                localAssignments[i] = argMin(distances[i]);
            }
        } else {
            for (int i = 0; i < distances.length; i++) {
                final int previouslySelected = assigner.apply(i);
                int[] neighbors = neighborhoods[previouslySelected].neighbors();

                distances[i][0] += weightClusterSizes * cumulativeClusterWeights[previouslySelected];
                for (int j = 1; j < distances[i].length; j++) {
                    distances[i][j] += weightClusterSizes * cumulativeClusterWeights[neighbors[j - 1]];
                }
                final int localSelected = argMin(distances[i]);
                if (localSelected == 0) {
                    localAssignments[i] = previouslySelected;
                } else {
                    localAssignments[i] = neighbors[localSelected - 1];
                }
            }
        }
    }

    private static int argMin(float[] vector) {
        float min = Float.MAX_VALUE;
        int argmin = 0;
        for (int i = 0; i < vector.length; i++) {
            if (vector[i] < min) {
                min = vector[i];
                argmin = i;
            }
        }
        return argmin;
    }

    /** update the centroids using stochastic gradient descent **/
    protected void updateCentroids(
        ClusteringFloatVectorValues vectors,
        float[] cumulativeClusterWeights,
        int[] assignments,
        float[][] centroids
    ) throws IOException {
        // The SGD learning rate is computed as 1 / (clusterCount + learningRateShift).
        // Using a shift so that the updates are gentle and small.
        float learningRateShift = 3.f * this.sampleSize / centroids.length;

        for (int idx = 0; idx < vectors.size(); idx++) {
            float[] vec = vectors.vectorValue(idx);
            int k = assignments[idx];
            cumulativeClusterWeights[k]++;
            float learning_rate = 1.f / (cumulativeClusterWeights[k] + learningRateShift);
            ESVectorUtil.linearCombination(learning_rate, vec, 1 - learning_rate, centroids[k]);
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

        int miniBatchSizeLocal = (miniBatchSize < 0) ? Math.absExact(miniBatchSize) * k : miniBatchSize;
        miniBatchSizeLocal = Math.min(miniBatchSizeLocal, n);

        int[] localAssignments; // assignments of sampledVectors in the mini batch to centroids
        float[][] distances; // distances from sampledVectors in the mini batch to centroids
        int[] miniBatchSamples = new int[miniBatchSizeLocal]; // stores the samples in the mini batch

        if (neighborhoods == null) {
            localAssignments = new int[miniBatchSizeLocal];
            distances = new float[miniBatchSizeLocal][k];
        } else {
            localAssignments = new int[miniBatchSizeLocal];
            final int nNeighbors = neighborhoods[0].neighbors().length;
            distances = new float[miniBatchSizeLocal][nNeighbors + 1];
        }

        float[] cumulativeClusterWeights = new float[k]; // maintains soft cluster counts for each cluster.
                                                         // Used to compute the learning rate in the SGD update of the centroids
        for (int idx = 0; idx < k; idx++) {
            if (assignments[idx] == -1) {
                cumulativeClusterWeights[assignments[idx]]++;
            }
        }

        OnlineQuantileEstimator medianEstimator = null; // We cannot initialize the estimator now because we need to know its range.

        int t = 0;
        for (int epoch = 0; epoch < maxIterations; epoch++) {
            for (int batch = 0; batch < sampleSize; batch += miniBatchSizeLocal) {
                // This simple version performs sampling with replacement (that is, two batches can share vectors but within a batch
                // the vectors are unique) for simplicity. To be more precise, we could sample without replacement but the current
                // approach seems good enough.
                ClusteringFloatVectorValues sampledVectors = ClusteringFloatVectorValuesSlice.createRandomSlice(
                    vectors,
                    miniBatchSizeLocal,
                    t++,
                    miniBatchSamples
                );

                IntToIntFunction assigner = i -> {
                    final int translatedOrd = sampledVectors.ordToDoc(i);
                    return assignments[translatedOrd];
                };

                computeDistances(sampledVectors, centroids, assigner, neighborhoods, distances);

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
                float alpha = beta * currentMedian * k / n;
                assignMiniBatch(distances, cumulativeClusterWeights, alpha, assigner, neighborhoods, localAssignments);

                // Update the centroids using SGD.
                updateCentroids(sampledVectors, cumulativeClusterWeights, localAssignments, centroids);
            }
            for (int kk = 0; kk < k; kk++) {
                cumulativeClusterWeights[kk] *= forgettingFactor;
            }
        }

        assert assignments.length == n;
        FixedBitSet[] centroidChangedSlices = new FixedBitSet[numWorkers()];
        for (int i = 0; i < numWorkers(); i++) {
            centroidChangedSlices[i] = new FixedBitSet(centroids.length);
        }

        assign(vectors, i -> i, centroids, centroidChangedSlices, assignments, neighborhoods);

        int[] centroidCounts = new int[centroids.length];
        vectors.updateCentroids(centroids, i -> i, centroidChangedSlices, centroidCounts, assignments);
    }

    /**
     * helper that calls {@link BalancedASKMeansLocal#cluster(ClusteringFloatVectorValues, KMeansIntermediate)} given a set of initialized
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
        BalancedASKMeansLocal kMeans = new BalancedASKMeansLocalSerial(sampleSize, maxIterations);
        kMeans.cluster(vectors, kMeansIntermediate);
    }
}
