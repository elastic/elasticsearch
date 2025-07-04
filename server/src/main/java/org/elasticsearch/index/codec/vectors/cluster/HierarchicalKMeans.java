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

import java.io.IOException;
import java.util.Arrays;

/**
 * An implementation of the hierarchical k-means algorithm that better partitions data than naive k-means
 */
public class HierarchicalKMeans {

    static final int MAXK = 128;
    static final int MAX_ITERATIONS_DEFAULT = 6;
    static final int SAMPLES_PER_CLUSTER_DEFAULT = 64;
    static final float DEFAULT_SOAR_LAMBDA = 1.0f;

    final int dimension;
    final int maxIterations;
    final int samplesPerCluster;
    final int clustersPerNeighborhood;
    final float soarLambda;

    public HierarchicalKMeans(int dimension) {
        this(dimension, MAX_ITERATIONS_DEFAULT, SAMPLES_PER_CLUSTER_DEFAULT, MAXK, DEFAULT_SOAR_LAMBDA);
    }

    HierarchicalKMeans(int dimension, int maxIterations, int samplesPerCluster, int clustersPerNeighborhood, float soarLambda) {
        this.dimension = dimension;
        this.maxIterations = maxIterations;
        this.samplesPerCluster = samplesPerCluster;
        this.clustersPerNeighborhood = clustersPerNeighborhood;
        this.soarLambda = soarLambda;
    }

    /**
     * clusters or moreso partitions the set of vectors by starting with a rough number of partitions and then recursively refining those
     * lastly a pass is made to adjust nearby neighborhoods and add an extra assignment per vector to nearby neighborhoods
     *
     * @param vectors the vectors to cluster
     * @param targetSize the rough number of vectors that should be attached to a cluster
     * @return the centroids and the vectors assignments and SOAR (spilled from nearby neighborhoods) assignments
     * @throws IOException is thrown if vectors is inaccessible
     */
    public KMeansResult cluster(FloatVectorValues vectors, int targetSize) throws IOException {

        if (vectors.size() == 0) {
            return new KMeansIntermediate();
        }

        // if we have a small number of vectors pick one and output that as the centroid
        if (vectors.size() <= targetSize) {
            float[] centroid = new float[dimension];
            System.arraycopy(vectors.vectorValue(0), 0, centroid, 0, dimension);
            return new KMeansIntermediate(new float[][] { centroid }, new int[vectors.size()]);
        }

        // partition the space
        KMeansIntermediate kMeansIntermediate = clusterAndSplit(vectors, targetSize);
        if (kMeansIntermediate.centroids().length > 1 && kMeansIntermediate.centroids().length < vectors.size()) {
            int localSampleSize = Math.min(kMeansIntermediate.centroids().length * samplesPerCluster / 2, vectors.size());
            KMeansLocal kMeansLocal = new KMeansLocal(localSampleSize, maxIterations, clustersPerNeighborhood, DEFAULT_SOAR_LAMBDA);
            kMeansLocal.cluster(vectors, kMeansIntermediate, true);
        }

        return kMeansIntermediate;
    }

    KMeansIntermediate clusterAndSplit(final FloatVectorValues vectors, final int targetSize) throws IOException {
        if (vectors.size() <= targetSize) {
            return new KMeansIntermediate();
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);
        int m = Math.min(k * samplesPerCluster, vectors.size());

        // TODO: instead of creating a sub-cluster assignments reuse the parent array each time
        int[] assignments = new int[vectors.size()];
        // ensure we don't over assign to cluster 0 without adjusting it
        Arrays.fill(assignments, -1);
        KMeansLocal kmeans = new KMeansLocal(m, maxIterations);
        float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, k);
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments, vectors::ordToDoc);
        kmeans.cluster(vectors, kMeansIntermediate);

        // TODO: consider adding cluster size counts to the kmeans algo
        // handle assignment here so we can track distance and cluster size
        int[] centroidVectorCount = new int[centroids.length];
        for (int assigment : assignments) {
            centroidVectorCount[assigment]++;
        }

        int effectiveK = 0;
        for (int j : centroidVectorCount) {
            if (j > 0) {
                effectiveK++;
            }
        }

        if (effectiveK == 1) {
            return kMeansIntermediate;
        }

        for (int c = 0; c < centroidVectorCount.length; c++) {
            // Recurse for each cluster which is larger than targetSize
            // Give ourselves 30% margin for the target size
            if (100 * centroidVectorCount[c] > 134 * targetSize) {
                FloatVectorValues sample = createClusterSlice(centroidVectorCount[c], c, vectors, assignments);

                // TODO: consider iterative here instead of recursive
                // recursive call to build out the sub partitions around this centroid c
                // subsequently reconcile and flatten the space of all centroids and assignments into one structure we can return
                updateAssignmentsWithRecursiveSplit(kMeansIntermediate, c, clusterAndSplit(sample, targetSize));
            }
        }

        return kMeansIntermediate;
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
        int orgCentroidsSize = current.centroids().length;
        int newCentroidsSize = current.centroids().length + subPartitions.centroids().length - 1;

        // update based on the outcomes from the split clusters recursion
        if (subPartitions.centroids().length > 1) {
            float[][] newCentroids = new float[newCentroidsSize][dimension];
            System.arraycopy(current.centroids(), 0, newCentroids, 0, current.centroids().length);

            // replace the original cluster
            int origCentroidOrd = 0;
            newCentroids[cluster] = subPartitions.centroids()[0];

            // append the remainder
            System.arraycopy(subPartitions.centroids(), 1, newCentroids, current.centroids().length, subPartitions.centroids().length - 1);

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
}
