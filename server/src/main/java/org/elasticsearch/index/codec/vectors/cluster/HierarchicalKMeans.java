/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.search.TaskExecutor;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * An implementation of the hierarchical k-means algorithm that better partitions data than naive k-means
 */
public class HierarchicalKMeans {

    public static final int MAXK = 128;
    public static final int SAMPLES_PER_CLUSTER_DEFAULT = 64;
    public static final float DEFAULT_SOAR_LAMBDA = 1.0f;
    public static final int NO_SOAR_ASSIGNMENT = -1;
    private static final int MIN_VECTORS_PRE_THREAD = 64;

    public static final boolean USE_BALANCING = true;
    public static final int MAX_ITERATIONS_DEFAULT = USE_BALANCING ? 2 : 6;

    final int dimension;
    final int maxIterations;
    final int samplesPerCluster;
    final int clustersPerNeighborhood;
    final float soarLambda;

    private final TaskExecutor executor;
    private final int numWorkers;

    private HierarchicalKMeans(
        int dimension,
        TaskExecutor executor,
        int numWorkers,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        this.dimension = dimension;
        this.executor = executor;
        this.numWorkers = numWorkers;
        this.maxIterations = maxIterations;
        this.samplesPerCluster = samplesPerCluster;
        this.clustersPerNeighborhood = clustersPerNeighborhood;
        this.soarLambda = soarLambda;
    }

    public static HierarchicalKMeans ofSerial(int dimension) {
        return ofSerial(dimension, MAX_ITERATIONS_DEFAULT, SAMPLES_PER_CLUSTER_DEFAULT, MAXK, DEFAULT_SOAR_LAMBDA);
    }

    public static HierarchicalKMeans ofSerial(
        int dimension,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        return new HierarchicalKMeans(dimension, null, 1, maxIterations, samplesPerCluster, clustersPerNeighborhood, soarLambda);
    }

    public static HierarchicalKMeans ofConcurrent(int dimension, TaskExecutor executor, int numWorkers) {
        return ofConcurrent(
            dimension,
            executor,
            numWorkers,
            MAX_ITERATIONS_DEFAULT,
            SAMPLES_PER_CLUSTER_DEFAULT,
            MAXK,
            DEFAULT_SOAR_LAMBDA
        );
    }

    public static HierarchicalKMeans ofConcurrent(
        int dimension,
        TaskExecutor executor,
        int numWorkers,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        return new HierarchicalKMeans(
            dimension,
            executor,
            numWorkers,
            maxIterations,
            samplesPerCluster,
            clustersPerNeighborhood,
            soarLambda
        );

    }

    /**
     * clusters the set of vectors by starting with a rough number of partitions and then recursively refining those
     * lastly a pass is made to adjust nearby neighborhoods and add an extra assignment per vector to nearby neighborhoods
     *
     * @param vectors the vectors to cluster
     * @param targetSize the rough number of vectors that should be attached to a cluster
     * @return the centroids and the vectors assignments and SOAR (spilled from nearby neighborhoods) assignments
     * @throws IOException is thrown if vectors is inaccessible
     */
    public KMeansResult cluster(ClusteringFloatVectorValues vectors, int targetSize) throws IOException {
        if (vectors.size() == 0) {
            return new KMeansIntermediate();
        }

        // if we have a small number of vectors calculate the centroid directly
        if (vectors.size() <= targetSize) {
            float[] centroid = new float[dimension];
            // sum the vectors
            for (int i = 0; i < vectors.size(); i++) {
                float[] vector = vectors.vectorValue(i);
                for (int j = 0; j < dimension; j++) {
                    centroid[j] += vector[j];
                }
            }
            // average the vectors
            for (int j = 0; j < dimension; j++) {
                centroid[j] /= vectors.size();
            }
            return new KMeansIntermediate(new float[][] { centroid }, new int[vectors.size()]);
        }

        // partition the space
        KMeansIntermediate kMeansIntermediate = clusterAndSplit(vectors, targetSize);

        if (kMeansIntermediate.centroids().length > 1 && kMeansIntermediate.centroids().length < vectors.size()) {
            int localSampleSize = Math.min(kMeansIntermediate.centroids().length * samplesPerCluster / 2, vectors.size());
            KMeansLocal kMeansLocal = buildKmeansLocalFinal(vectors.size(), localSampleSize);
            kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);
        }
        return kMeansIntermediate;
    }

    private KMeansIntermediate clusterAndSplit(final ClusteringFloatVectorValues vectors, final int targetSize) throws IOException {
        if (vectors.size() <= targetSize) {
            return new KMeansIntermediate();
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);
        int m = Math.min(k * samplesPerCluster, vectors.size());

        // TODO: instead of creating a sub-cluster assignments reuse the parent array each time
        int[] assignments = new int[vectors.size()];
        // ensure we don't over assign to cluster 0 without adjusting it
        Arrays.fill(assignments, -1);
        float[][] centroids = LloydKMeansLocal.pickInitialCentroids(vectors, k);
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments, vectors::ordToDoc);
        KMeansLocal kMeansLocal = buildKmeansLocal(vectors.size(), m);
        kMeansLocal.cluster(vectors, kMeansIntermediate);

        // TODO: consider adding cluster size counts to the kmeans algo
        // handle assignment here so we can track distance and cluster size
        int[] centroidVectorCount = new int[centroids.length];
        int effectiveCluster = -1;
        int effectiveK = 0;
        for (int assigment : assignments) {
            centroidVectorCount[assigment]++;
            // this cluster has received an assignment, its now effective, but only count it once
            if (centroidVectorCount[assigment] == 1) {
                effectiveK++;
                effectiveCluster = assigment;
            }
        }

        if (effectiveK == 1) {
            final float[][] singleClusterCentroid = new float[1][];
            singleClusterCentroid[0] = centroids[effectiveCluster];
            kMeansIntermediate.setCentroids(singleClusterCentroid);
            Arrays.fill(kMeansIntermediate.assignments(), 0);
            return kMeansIntermediate;
        }

        resolveOversizedAndEmptyClusters(kMeansIntermediate, vectors, centroidVectorCount, targetSize);

        return kMeansIntermediate;
    }

    private KMeansLocal buildKmeansLocal(int numVectors, int localSampleSize) {
        return buildKmeansLocal(numVectors, localSampleSize, maxIterations);
    }

    private KMeansLocal buildKmeansLocal(int numVectors, int localSampleSize, int iterations) {
        int numWorkers = Math.min(this.numWorkers, numVectors / MIN_VECTORS_PRE_THREAD);
        // if there is no executor or there is no enough vectors for more than one thread, use the serial version
        if (USE_BALANCING) {
            return executor == null || numWorkers <= 1
                ? new BalancedOTKMeansLocalSerial(localSampleSize, iterations)
                : new BalancedOTKMeansLocalConcurrent(executor, numWorkers, localSampleSize, iterations);
        } else {
            return executor == null || numWorkers <= 1
                ? new LloydKMeansLocalSerial(localSampleSize, iterations)
                : new LloydKMeansLocalConcurrent(executor, numWorkers, localSampleSize, iterations);
        }
    }

    private KMeansLocal buildKmeansLocalFinal(int numVectors, int localSampleSize) {
        int numWorkers = Math.min(this.numWorkers, numVectors / MIN_VECTORS_PRE_THREAD);
        // if there is no executor or there is no enough vectors for more than one thread, use the serial version
        if (USE_BALANCING) {
            return executor == null || numWorkers <= 1
                ? new BalancedASKMeansLocalSerial(localSampleSize, maxIterations)
                : new BalancedASKMeansLocalConcurrent(executor, numWorkers, localSampleSize, maxIterations);
        } else {
            return executor == null || numWorkers <= 1
                ? new LloydKMeansLocalSerial(localSampleSize, maxIterations)
                : new LloydKMeansLocalConcurrent(executor, numWorkers, localSampleSize, maxIterations);
        }
    }

    static ClusteringFloatVectorValues createClusterSlice(
        int clusterSize,
        int cluster,
        ClusteringFloatVectorValues vectors,
        int[] assignments
    ) {
        assert assignments.length == vectors.size();
        int[] slice = new int[clusterSize];
        int idx = 0;
        for (int i = 0; i < assignments.length; i++) {
            if (assignments[i] == cluster) {
                slice[idx] = i;
                idx++;
            }
        }
        assert idx == clusterSize;

        return new ClusteringFloatVectorValuesSlice(vectors, i -> slice[i], slice.length);
    }

    private static float[][] deepCopy(float[][] source) {
        float[][] copy = new float[source.length][];
        for (int i = 0; i < source.length; i++) {
            copy[i] = Arrays.copyOf(source[i], source[i].length);
        }
        return copy;
    }

    /**
     * Reduce a large set of centroids to targetCount using lightweight K-means.
     * Operates only on centroids (not full vectors), so it's very fast.
     */
    private float[][] reduceCentroids(float[][] centroids, int targetCount) throws IOException {
        ClusteringFloatVectorValues centroidValues = KMeansFloatVectorValues.build(java.util.Arrays.asList(centroids), null, dimension);
        float[][] reduced = KMeansLocal.pickInitialCentroids(centroidValues, targetCount);
        // Run a few Lloyd iterations to refine
        KMeansIntermediate intermediate = new KMeansIntermediate(reduced, new int[centroids.length], centroidValues::ordToDoc);
        KMeansLocal kMeansLocal = new LloydKMeansLocalSerial(centroids.length, 3);
        kMeansLocal.cluster(centroidValues, intermediate);
        return intermediate.centroids();
    }

    private static int findNearestCentroid(float[] vector, float[][] centroids, float[] distances) {
        int bestCentroid = 0;
        float minDistance = Float.MAX_VALUE;
        int i = 0;
        int limit = centroids.length - 3;
        for (; i < limit; i += 4) {
            ESVectorUtil.squareDistanceBulk(vector, centroids[i], centroids[i + 1], centroids[i + 2], centroids[i + 3], 0, distances);
            for (int j = 0; j < 4; j++) {
                float distance = distances[j];
                if (distance < minDistance) {
                    minDistance = distance;
                    bestCentroid = i + j;
                }
            }
        }
        for (; i < centroids.length; i++) {
            float distance = ESVectorUtil.squareDistance(vector, centroids[i]);
            if (distance < minDistance) {
                minDistance = distance;
                bestCentroid = i;
            }
        }
        return bestCentroid;
    }

    private void assignSoarOnly(ClusteringFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate) throws IOException {
        if (soarLambda < 0) {
            return;
        }

        NeighborHood[] neighborhoods = null;
        float[][] centroids = kMeansIntermediate.centroids();
        if (centroids.length > clustersPerNeighborhood) {
            neighborhoods = executor == null || numWorkers < 2
                ? NeighborHood.computeNeighborhoods(centroids, clustersPerNeighborhood)
                : NeighborHood.computeNeighborhoods(executor, numWorkers, centroids, clustersPerNeighborhood);
        }

        kMeansIntermediate.setSoarAssignments(new int[vectors.size()]);
        int effectiveWorkers = Math.min(numWorkers, vectors.size() / MIN_VECTORS_PRE_THREAD);
        if (executor != null && effectiveWorkers >= 2) {
            final int len = vectors.size() / effectiveWorkers;
            final List<Callable<Void>> runners = new ArrayList<>(effectiveWorkers);
            final int[] assignments = kMeansIntermediate.assignments();
            final int[] soarAssignments = kMeansIntermediate.soarAssignments();
            final NeighborHood[] nh = neighborhoods;
            for (int i = 0; i < effectiveWorkers; i++) {
                final int start = i * len;
                final int end = i == effectiveWorkers - 1 ? vectors.size() : (i + 1) * len;
                runners.add(() -> {
                    vectors.copy().assignSpilled(start, end, centroids, nh, soarLambda, assignments, soarAssignments);
                    return null;
                });
            }
            executor.invokeAll(runners);
        } else {
            vectors.assignSpilled(
                0,
                vectors.size(),
                centroids,
                neighborhoods,
                soarLambda,
                kMeansIntermediate.assignments(),
                kMeansIntermediate.soarAssignments()
            );
        }
    }

    /**
     * Supplement a small set of centroids with random vectors to reach targetCount.
     * Uses stride-based sampling to spread selected vectors evenly across the dataset.
     */
    private static float[][] supplementCentroids(float[][] existing, ClusteringFloatVectorValues vectors, int targetCount)
        throws IOException {
        float[][] augmented = new float[targetCount][];
        for (int i = 0; i < existing.length; i++) {
            augmented[i] = Arrays.copyOf(existing[i], existing[i].length);
        }
        int needed = targetCount - existing.length;
        // Stride-based sampling: pick every (size/needed)-th vector for even distribution
        int filled = 0;
        double stride = (double) vectors.size() / needed;
        for (int i = 0; filled < needed && i < vectors.size(); i++) {
            if (i >= (long) (filled * stride)) {
                float[] vector = vectors.vectorValue(i);
                augmented[existing.length + filled] = Arrays.copyOf(vector, vector.length);
                filled++;
            }
        }
        // Fallback: fill any remaining slots from the start (shouldn't happen with stride sampling)
        for (int i = 0; filled < needed && i < vectors.size(); i++) {
            float[] vector = vectors.vectorValue(i);
            augmented[existing.length + filled] = Arrays.copyOf(vector, vector.length);
            filled++;
        }
        return augmented;
    }

    /**
     * Cluster vectors by inserting them into an existing centroid structure.
     * Analogous to HNSW merge where the biggest graph is kept and new vectors are inserted:
     * uses the provided centroids as a near-final clustering and does minimal iteration —
     * one assignment pass to place all vectors, then one refinement pass for neighborhood
     * awareness and SOAR. This is significantly faster when the initial centroids already
     * represent the majority of the data (e.g., from a dominant segment).
     *
     * @param vectors          the vectors to cluster
     * @param initialCentroids centroids from the dominant segment
     * @param targetSize       target number of vectors per cluster
     * @return clustering result with assignments and SOAR assignments
     */
    public KMeansResult clusterByInsertion(ClusteringFloatVectorValues vectors, float[][] initialCentroids, int targetSize)
        throws IOException {
        if (vectors.size() == 0) {
            return new KMeansIntermediate();
        }

        if (vectors.size() <= targetSize) {
            float[] centroid = new float[dimension];
            for (int i = 0; i < vectors.size(); i++) {
                float[] vector = vectors.vectorValue(i);
                for (int j = 0; j < dimension; j++) {
                    centroid[j] += vector[j];
                }
            }
            for (int j = 0; j < dimension; j++) {
                centroid[j] /= vectors.size();
            }
            return new KMeansIntermediate(new float[][] { centroid }, new int[vectors.size()]);
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);

        float[][] seedCentroids;
        if (initialCentroids.length == k) {
            seedCentroids = deepCopy(initialCentroids);
        } else if (initialCentroids.length > k) {
            seedCentroids = reduceCentroids(initialCentroids, k);
        } else {
            seedCentroids = supplementCentroids(initialCentroids, vectors, k);
        }

        // Single assignment pass: assign all vectors to nearest seed centroid and update positions.
        // No sampling — with only 1 iteration, a full pass is both faster and more accurate than
        // a sampled pass followed by a final full pass.
        int[] assignments = new int[vectors.size()];
        Arrays.fill(assignments, -1);
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(seedCentroids, assignments, vectors::ordToDoc);
        KMeansLocal kMeansLocal = buildKmeansLocal(vectors.size(), vectors.size(), 1);
        kMeansLocal.cluster(vectors, kMeansIntermediate);

        // Handle oversized clusters by recursive splitting, same as clusterAndSplit
        int[] centroidVectorCount = new int[seedCentroids.length];
        for (int assignment : assignments) {
            if (assignment >= 0) {
                centroidVectorCount[assignment]++;
            }
        }

        resolveOversizedAndEmptyClusters(kMeansIntermediate, vectors, centroidVectorCount, targetSize);

        // Neighborhood-aware refinement + SOAR with minimal iteration.
        // One pass is sufficient since the initial assignment is already good.
        if (kMeansIntermediate.centroids().length > 1 && kMeansIntermediate.centroids().length < vectors.size()) {
            KMeansLocal refinementKMeans = buildKmeansLocal(vectors.size(), vectors.size(), 1);
            refinementKMeans.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);
        }

        return kMeansIntermediate;
    }

    /**
     * Cluster vectors by concatenating prior centroids and doing a single assignment pass.
     * <p>
     * For merges of balanced segments (no dominant segment) where prior centroids exist:
     * concatenate all prior centroids, reduce/expand to target K, then do a single full
     * assignment pass + SOAR. This eliminates iterative convergence entirely — the prior
     * centroids are already good approximations of the data distribution.
     *
     * @param vectors           the merged vectors
     * @param allPriorCentroids centroids concatenated from all input segments
     * @param targetSize        target vectors per cluster
     * @return clustering result
     */
    public KMeansResult clusterByConcatenation(ClusteringFloatVectorValues vectors, float[][] allPriorCentroids, int targetSize)
        throws IOException {
        if (vectors.size() == 0) {
            return new KMeansIntermediate();
        }
        if (vectors.size() <= targetSize) {
            float[] centroid = new float[dimension];
            for (int i = 0; i < vectors.size(); i++) {
                float[] vector = vectors.vectorValue(i);
                for (int j = 0; j < dimension; j++) {
                    centroid[j] += vector[j];
                }
            }
            for (int j = 0; j < dimension; j++) {
                centroid[j] /= vectors.size();
            }
            return new KMeansIntermediate(new float[][] { centroid }, new int[vectors.size()]);
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);

        // Fit prior centroids to target K
        float[][] seedCentroids;
        if (allPriorCentroids.length == k) {
            seedCentroids = deepCopy(allPriorCentroids);
        } else if (allPriorCentroids.length > k) {
            seedCentroids = reduceCentroids(allPriorCentroids, k);
        } else {
            seedCentroids = supplementCentroids(allPriorCentroids, vectors, k);
        }

        // Single assignment pass with NO iterative convergence (0 sampled iterations).
        int[] assignments = new int[vectors.size()];
        Arrays.fill(assignments, -1);
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(seedCentroids, assignments, vectors::ordToDoc);
        // maxIterations=0 means only the final full-pass assignment (no sampled iterations)
        KMeansLocal kMeansLocal = buildKmeansLocal(vectors.size(), vectors.size(), 0);
        kMeansLocal.cluster(vectors, kMeansIntermediate);

        // Handle oversized clusters
        int[] centroidVectorCount = new int[seedCentroids.length];
        for (int assignment : assignments) {
            if (assignment >= 0) {
                centroidVectorCount[assignment]++;
            }
        }
        resolveOversizedAndEmptyClusters(kMeansIntermediate, vectors, centroidVectorCount, targetSize);

        // SOAR assignment only — benchmarks showed refinement pass never justifies its cost
        if (kMeansIntermediate.centroids().length > 1 && kMeansIntermediate.centroids().length < vectors.size()) {
            assignSoarOnly(vectors, kMeansIntermediate);
        }

        return kMeansIntermediate;
    }

    private void resolveOversizedAndEmptyClusters(
        KMeansIntermediate kMeansIntermediate,
        ClusteringFloatVectorValues vectors,
        int[] centroidVectorCount,
        int targetSize
    ) throws IOException {
        int[] assignments = kMeansIntermediate.assignments();
        float[][] centroids = kMeansIntermediate.centroids();

        boolean[] isSplit = new boolean[centroids.length];
        boolean hasSplitOrEmpty = false;
        for (int c = 0; c < centroids.length; c++) {
            int count = centroidVectorCount[c];
            if (count == 0) {
                hasSplitOrEmpty = true;
            } else if (100 * count > 134 * targetSize) {
                isSplit[c] = true;
                hasSplitOrEmpty = true;
            }
        }

        if (!hasSplitOrEmpty) {
            return;
        }

        int[][] clusterSlices = new int[centroids.length][];
        int[] sliceIndex = new int[centroids.length];
        for (int c = 0; c < centroids.length; c++) {
            if (isSplit[c]) {
                clusterSlices[c] = new int[centroidVectorCount[c]];
            }
        }
        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            if (c >= 0 && isSplit[c]) {
                clusterSlices[c][sliceIndex[c]++] = i;
            }
        }

        KMeansIntermediate[] subPartitionsList = new KMeansIntermediate[centroids.length];
        int newCentroidsCount = 0;
        for (int c = 0; c < centroids.length; c++) {
            if (isSplit[c]) {
                int finalC = c;
                int[] slice = clusterSlices[c];
                ClusteringFloatVectorValues sample = new ClusteringFloatVectorValuesSlice(vectors, i -> slice[i], centroidVectorCount[c]);
                KMeansIntermediate subPartitions = clusterAndSplit(sample, targetSize);
                subPartitionsList[c] = subPartitions;
                newCentroidsCount += subPartitions.centroids().length;
            } else if (centroidVectorCount[c] > 0) {
                newCentroidsCount++;
            }
        }

        float[][] newCentroids = new float[newCentroidsCount][];
        int[] centroidOffsetMap = new int[centroids.length];
        int currentDest = 0;

        for (int c = 0; c < centroids.length; c++) {
            centroidOffsetMap[c] = currentDest;
            if (isSplit[c]) {
                KMeansIntermediate sub = subPartitionsList[c];
                System.arraycopy(sub.centroids(), 0, newCentroids, currentDest, sub.centroids().length);
                currentDest += sub.centroids().length;
            } else if (centroidVectorCount[c] > 0) {
                newCentroids[currentDest] = centroids[c];
                currentDest++;
            }
        }

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            if (c >= 0 && !isSplit[c]) {
                assignments[i] = centroidOffsetMap[c];
            }
        }

        for (int c = 0; c < centroids.length; c++) {
            if (isSplit[c]) {
                KMeansIntermediate sub = subPartitionsList[c];
                int baseOffset = centroidOffsetMap[c];
                int[] slice = clusterSlices[c];
                int[] subAssignments = sub.assignments();
                for (int j = 0; j < subAssignments.length; j++) {
                    int parentOrd = sub.ordToDoc(j);
                    assignments[parentOrd] = baseOffset + subAssignments[j];
                }
            }
        }

        kMeansIntermediate.setCentroids(newCentroids);
    }
}
