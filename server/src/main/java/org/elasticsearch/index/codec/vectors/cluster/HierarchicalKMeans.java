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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

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

        centroids = kMeansIntermediate.centroids();
        int[] centroidVectorCount = kMeansIntermediate.clusterCounts();

        if (centroids.length == 1) {
            return kMeansIntermediate;
        }

        int centroidIndexOffset = 0; // tracks the cumulative change in centroid indices due to splits and removals
        for (int c = 0; c < centroidVectorCount.length; c++) {
            // Recurse for each cluster which is larger than targetSize
            final int count = centroidVectorCount[c];
            final int adjustedCentroid = c + centroidIndexOffset;
            if (count > targetSize) {
                final ClusteringFloatVectorValues sample = createClusterSlice(count, adjustedCentroid, vectors, assignments);

                // TODO: consider iterative here instead of recursive
                // recursive call to build out the sub partitions around this centroid c
                // subsequently reconcile and flatten the space of all centroids and assignments into one structure we can return
                KMeansIntermediate subPartitions = clusterAndSplit(sample, targetSize);
                // Update offset: split replaces 1 centroid with subPartitions.centroids().length centroids
                centroidIndexOffset += updateAssignmentsWithRecursiveSplit(kMeansIntermediate, adjustedCentroid, subPartitions);
            }
        }

        return kMeansIntermediate;
    }

    private KMeansLocal buildKmeansLocal(int numVectors, int localSampleSize) {
        int numWorkers = Math.min(this.numWorkers, numVectors / MIN_VECTORS_PRE_THREAD);
        // if there is no executor or there is no enough vectors for more than one thread, use the serial version
        if (USE_BALANCING) {
            return executor == null || numWorkers <= 1
                ? new BalancedOTKMeansLocalSerial(localSampleSize, maxIterations)
                : new BalancedOTKMeansLocalConcurrent(executor, numWorkers, localSampleSize, maxIterations);
        } else {
            return executor == null || numWorkers <= 1
                ? new LloydKMeansLocalSerial(localSampleSize, maxIterations)
                : new LloydKMeansLocalConcurrent(executor, numWorkers, localSampleSize, maxIterations);
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

    /**
     * Merge the child centroids in {@code subPartitions} into the K means results.
     * {@code subPartitions} are inserted into the results next to the parent centroid
     * at position {@code cluster}.
     * @param current Current results
     * @param cluster Index of the split centroid
     * @param subPartitions The new centroids resulting from the split
     * @return The number of centroids added excluding the one that is replaced
     */
    int updateAssignmentsWithRecursiveSplit(KMeansIntermediate current, int cluster, KMeansIntermediate subPartitions) {
        if (subPartitions.centroids().length == 0) {
            return 0; // nothing to do, sub-partitions is empty
        }
        int orgCentroidsSize = current.centroids().length;
        int newCentroidsSize = current.centroids().length + subPartitions.centroids().length - 1;

        // update based on the outcomes from the split clusters recursion
        float[][] newCentroids = new float[newCentroidsSize][];
        int[] newClusterCounts = new int[newCentroidsSize];

        // copy centroids prior to the split
        System.arraycopy(current.centroids(), 0, newCentroids, 0, cluster);
        System.arraycopy(current.clusterCounts(), 0, newClusterCounts, 0, cluster);
        // insert the split partitions replacing the original cluster
        System.arraycopy(subPartitions.centroids(), 0, newCentroids, cluster, subPartitions.centroids().length);
        System.arraycopy(subPartitions.clusterCounts(), 0, newClusterCounts, cluster, subPartitions.centroids().length);
        // append the remainder
        System.arraycopy(
            current.centroids(),
            cluster + 1,
            newCentroids,
            cluster + subPartitions.centroids().length,
            orgCentroidsSize - cluster - 1
        );
        System.arraycopy(
            current.clusterCounts(),
            cluster + 1,
            newClusterCounts,
            cluster + subPartitions.centroids().length,
            orgCentroidsSize - cluster - 1
        );

        assert Arrays.stream(newCentroids).allMatch(Objects::nonNull);
        current.setCentroids(newCentroids, newClusterCounts);

        // update the remaining cluster assignments to point to the new centroids after the split cluster
        // IMPORTANT: Do this BEFORE updating split cluster assignments to avoid double-updating
        for (int i = 0; i < current.assignments().length; i++) {
            if (current.assignments()[i] > cluster) {
                current.assignments()[i] = current.assignments()[i] - 1 + subPartitions.centroids().length;
            }
        }
        // update the split cluster assignments to point to the new centroids from the split cluster
        for (int i = 0; i < subPartitions.assignments().length; i++) {
            int parentOrd = subPartitions.ordToDoc(i);
            assert current.assignments()[parentOrd] == cluster;
            current.assignments()[parentOrd] = cluster + subPartitions.assignments()[i];
        }

        // number of items inserted with 1 element replaced
        return subPartitions.centroids().length - 1;
    }
}
