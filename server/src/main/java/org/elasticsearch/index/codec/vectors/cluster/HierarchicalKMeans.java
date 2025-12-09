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
import org.apache.lucene.search.TaskExecutor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * An implementation of the hierarchical k-means algorithm that better partitions data than naive k-means
 */
public class HierarchicalKMeans {

    public static final int MAXK = 128;
    public static final int MAX_ITERATIONS_DEFAULT = 6;
    public static final int SAMPLES_PER_CLUSTER_DEFAULT = 64;
    public static final float DEFAULT_SOAR_LAMBDA = 1.0f;
    public static final int NO_SOAR_ASSIGNMENT = -1;

    final int dimension;
    final int maxIterations;
    final int samplesPerCluster;
    final int clustersPerNeighborhood;
    final float soarLambda;

    public HierarchicalKMeans(int dimension) {
        this(dimension, MAX_ITERATIONS_DEFAULT, SAMPLES_PER_CLUSTER_DEFAULT, MAXK, DEFAULT_SOAR_LAMBDA);
    }

    public HierarchicalKMeans(int dimension, int maxIterations, int samplesPerCluster, int clustersPerNeighborhood, float soarLambda) {
        this.dimension = dimension;
        this.maxIterations = maxIterations;
        this.samplesPerCluster = samplesPerCluster;
        this.clustersPerNeighborhood = clustersPerNeighborhood;
        this.soarLambda = soarLambda;
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
    public KMeansResult cluster(FloatVectorValues vectors, int targetSize) throws IOException {
        return cluster(vectors, targetSize, null, 1);
    }

    /**
     * Similar to {@link #cluster(FloatVectorValues, int)} but allows to specify a custom executor to use for parallelism.
     *
     * @param vectors the vectors to cluster
     * @param targetSize the rough number of vectors that should be attached to a cluster
     * @param executor the executor to use for parallelism, or null to use a single thread.
     * @param numWorkers the number of workers to use for parallelism. This parameter is ignored if executor is null.
     * @return the centroids and the vectors assignments and SOAR (spilled from nearby neighborhoods) assignments
     * @throws IOException is thrown if vectors is inaccessible
     */
    public KMeansResult cluster(FloatVectorValues vectors, int targetSize, TaskExecutor executor, int numWorkers) throws IOException {

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
        KMeansIntermediate kMeansIntermediate = clusterAndSplit(vectors, targetSize, executor, numWorkers);
        if (kMeansIntermediate.centroids().length > 1 && kMeansIntermediate.centroids().length < vectors.size()) {
            int localSampleSize = Math.min(kMeansIntermediate.centroids().length * samplesPerCluster / 2, vectors.size());
            KMeansLocal kMeansLocal = buildKmeansLocal(executor, numWorkers, localSampleSize, maxIterations, vectors.size());
            kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);
        }
        return kMeansIntermediate;
    }

    KMeansIntermediate clusterAndSplit(final FloatVectorValues vectors, final int targetSize, TaskExecutor executor, int numWorkers)
        throws IOException {
        if (vectors.size() <= targetSize) {
            return new KMeansIntermediate();
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);
        int m = Math.min(k * samplesPerCluster, vectors.size());

        // TODO: instead of creating a sub-cluster assignments reuse the parent array each time
        int[] assignments = new int[vectors.size()];
        // ensure we don't over assign to cluster 0 without adjusting it
        Arrays.fill(assignments, -1);
        float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, k);
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments, vectors::ordToDoc);
        KMeansLocal kMeansLocal = buildKmeansLocal(executor, numWorkers, m, maxIterations, vectors.size());
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

        int removedElements = 0;
        for (int c = 0; c < centroidVectorCount.length; c++) {
            // Recurse for each cluster which is larger than targetSize
            // Give ourselves 30% margin for the target size
            final int count = centroidVectorCount[c];
            final int adjustedCentroid = c - removedElements;
            if (100 * count > 134 * targetSize) {
                final FloatVectorValues sample = createClusterSlice(count, adjustedCentroid, vectors, assignments);
                // TODO: consider iterative here instead of recursive
                // recursive call to build out the sub partitions around this centroid c
                // subsequently reconcile and flatten the space of all centroids and assignments into one structure we can return
                updateAssignmentsWithRecursiveSplit(
                    kMeansIntermediate,
                    adjustedCentroid,
                    clusterAndSplit(sample, targetSize, executor, numWorkers)
                );
            } else if (count == 0) {
                // remove empty clusters
                final int newSize = kMeansIntermediate.centroids().length - 1;
                final float[][] newCentroids = new float[newSize][];
                System.arraycopy(kMeansIntermediate.centroids(), 0, newCentroids, 0, adjustedCentroid);
                System.arraycopy(
                    kMeansIntermediate.centroids(),
                    adjustedCentroid + 1,
                    newCentroids,
                    adjustedCentroid,
                    newSize - adjustedCentroid
                );
                // we need to update the assignments to reflect the new centroid ordinals
                for (int i = 0; i < kMeansIntermediate.assignments().length; i++) {
                    if (kMeansIntermediate.assignments()[i] > adjustedCentroid) {
                        kMeansIntermediate.assignments()[i]--;
                    }
                }
                kMeansIntermediate.setCentroids(newCentroids);
                removedElements++;
            }
        }

        return kMeansIntermediate;
    }

    private static KMeansLocal buildKmeansLocal(TaskExecutor executor, int numWorkers, int sampleSize, int maxIterations, int numVectors) {
        // if there is no executor or the number of vectors is small use a serial implementation
        return executor == null || numVectors < numVectors * 4
            ? new KMeansLocalSerial(sampleSize, maxIterations)
            : new KMeansLocalConcurrent(executor, numWorkers, sampleSize, maxIterations);
    }

    static FloatVectorValues createClusterSlice(int clusterSize, int cluster, FloatVectorValues vectors, int[] assignments) {
        int[] slice = new int[clusterSize];
        int idx = 0;
        for (int i = 0; i < assignments.length; i++) {
            if (assignments[i] == cluster) {
                slice[idx] = i;
                idx++;
            }
        }

        return new FloatVectorValuesSlice(vectors, slice);
    }

    void updateAssignmentsWithRecursiveSplit(KMeansIntermediate current, int cluster, KMeansIntermediate subPartitions) {
        if (subPartitions.centroids().length == 0) {
            return; // nothing to do, sub-partitions is empty
        }
        int orgCentroidsSize = current.centroids().length;
        int newCentroidsSize = current.centroids().length + subPartitions.centroids().length - 1;

        // update based on the outcomes from the split clusters recursion
        float[][] newCentroids = new float[newCentroidsSize][];
        System.arraycopy(current.centroids(), 0, newCentroids, 0, current.centroids().length);

        // replace the original cluster
        int origCentroidOrd = 0;
        newCentroids[cluster] = subPartitions.centroids()[0];

        // append the remainder
        System.arraycopy(subPartitions.centroids(), 1, newCentroids, current.centroids().length, subPartitions.centroids().length - 1);
        assert Arrays.stream(newCentroids).allMatch(Objects::nonNull);

        current.setCentroids(newCentroids);

        for (int i = 0; i < subPartitions.assignments().length; i++) {
            // this is a new centroid that was added, and so we'll need to remap it
            if (subPartitions.assignments()[i] != origCentroidOrd) {
                int parentOrd = subPartitions.ordToDoc(i);
                assert current.assignments()[parentOrd] == cluster;
                current.assignments()[parentOrd] = subPartitions.assignments()[i] + orgCentroidsSize - 1;
            }
        }
    }
}
