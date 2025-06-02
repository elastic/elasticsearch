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

import java.io.IOException;

/**
 * An implementation of the hierarchical k-means algorithm that better partitions data than naive k-means
 */
public class HierarchicalKMeans {

    static final int MAXK = 128;
    static final int MAX_ITERATIONS_DEFAULT = 6;
    static final int SAMPLES_PER_CLUSTER_DEFAULT = 256;

    final int maxIterations;
    final int samplesPerCluster;
    final short clustersPerNeighborhood;

    public HierarchicalKMeans() {
        this(MAX_ITERATIONS_DEFAULT, SAMPLES_PER_CLUSTER_DEFAULT, (short) MAXK);
    }

    HierarchicalKMeans(int maxIterations, int samplesPerCluster, short clustersPerNeighborhood) {
        this.maxIterations = maxIterations;
        this.samplesPerCluster = samplesPerCluster;
        this.clustersPerNeighborhood = clustersPerNeighborhood;
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
            return new KMeansResult();
        }

        // if we have a small number of vectors pick one and output that as the centroid
        if (vectors.size() < targetSize) {
            float[] centroid = new float[vectors.dimension()];
            System.arraycopy(vectors.vectorValue(0), 0, centroid, 0, vectors.dimension());
            return new KMeansResult(new float[][] { centroid }, new short[vectors.size()]);
        }

        // partition the space
        KMeansResult kMeansResult = kMeansHierarchical(new FloatVectorValuesSlice(vectors), targetSize);
        if (kMeansResult.centroids().length > 1 && kMeansResult.centroids().length < vectors.size()) {
            float f = Math.min((float) samplesPerCluster / targetSize, 1.0f);
            int localSampleSize = (int) (f * vectors.size());
            KMeansLocal kMeansLocal = new KMeansLocal(localSampleSize, maxIterations, clustersPerNeighborhood);
            kMeansLocal.cluster(vectors, kMeansResult);
        }

        return kMeansResult;
    }

    KMeansResult kMeansHierarchical(final FloatVectorValuesSlice vectors, final int targetSize) throws IOException {
        if (vectors.size() <= targetSize) {
            return new KMeansResult();
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);
        int m = Math.min(k * samplesPerCluster, vectors.size());

        // TODO: instead of creating a sub-cluster assignments reuse the parent array each time
        short[] assignments = new short[vectors.size()];

        KMeans kmeans = new KMeans(m, maxIterations);
        float[][] centroids = KMeans.pickInitialCentroids(vectors, m, k);
        KMeansResult kMeansResult = new KMeansResult(centroids);
        kmeans.cluster(vectors, kMeansResult);

        int[] clusterSizes = new int[centroids.length];

        // TODO: consider adding cluster size counts to the kmeans algo
        // handle assignment here so we can track distance and cluster size
        int[] centroidVectorCount = new int[centroids.length];
        float[][] nextCentroids = new float[centroids.length][vectors.dimension()];
        for (int i = 0; i < vectors.size(); i++) {
            float smallest = Float.MAX_VALUE;
            short centroidIdx = -1;
            float[] vector = vectors.vectorValue(i);
            for (short j = 0; j < centroids.length; j++) {
                float[] centroid = centroids[j];
                float d = VectorUtil.squareDistance(vector, centroid);
                if (d < smallest) {
                    smallest = d;
                    centroidIdx = j;
                }
            }
            centroidVectorCount[centroidIdx]++;
            for (int j = 0; j < vectors.dimension(); j++) {
                nextCentroids[centroidIdx][j] += vector[j];
            }
            assignments[i] = centroidIdx;
            clusterSizes[centroidIdx]++;
        }

        // update centroids based on assignments of all vectors
        for (int i = 0; i < centroids.length; i++) {
            if (centroidVectorCount[i] > 0) {
                for (int j = 0; j < vectors.dimension(); j++) {
                    centroids[i][j] = nextCentroids[i][j] / centroidVectorCount[i];
                }
            }
        }

        short effectiveK = 0;
        for (int i = 0; i < clusterSizes.length; i++) {
            if (clusterSizes[i] > 0) {
                effectiveK++;
            }
        }

        int[] assignmentOrdinals = new int[vectors.slice.length];
        System.arraycopy(vectors.slice, 0, assignmentOrdinals, 0, assignmentOrdinals.length);

        kMeansResult = new KMeansResult(centroids, assignments, assignmentOrdinals);

        if (effectiveK == 1) {
            return kMeansResult;
        }

        for (short c = 0; c < clusterSizes.length; c++) {
            // Recurse for each cluster which is larger than targetSize
            // Give ourselves 30% margin for the target size
            if (100 * clusterSizes[c] > 134 * targetSize) {
                FloatVectorValuesSlice sample = createClusterSlice(clusterSizes[c], c, vectors, assignments);

                // TODO: consider iterative here instead of recursive
                updateAssignmentsWithRecursiveSplit(kMeansResult, c, kMeansHierarchical(sample, targetSize));
            }
        }

        return kMeansResult;
    }

    static FloatVectorValuesSlice createClusterSlice(int clusterSize, int cluster, FloatVectorValuesSlice vectors, short[] assignments) {
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

    static void updateAssignmentsWithRecursiveSplit(KMeansResult current, short cluster, KMeansResult splitClusters) {
        int orgCentroidsSize = current.centroids().length;

        // update based on the outcomes from the split clusters recursion
        if (splitClusters.centroids().length > 1) {
            float[][] newCentroids = new float[current.centroids().length + splitClusters.centroids().length - 1][current
                .centroids()[0].length];
            System.arraycopy(current.centroids(), 0, newCentroids, 0, current.centroids().length);

            // replace the original cluster
            short origCentroidOrd = 0;
            newCentroids[cluster] = splitClusters.centroids()[0];

            // append the remainder
            System.arraycopy(splitClusters.centroids(), 1, newCentroids, current.centroids().length, splitClusters.centroids().length - 1);

            current.setCentroids(newCentroids);

            for (int i = 0; i < splitClusters.assignments().length; i++) {
                // this is a new centroid that was added, and so we'll need to remap it
                if (splitClusters.assignments()[i] != origCentroidOrd) {
                    int parentOrd = splitClusters.assignmentOrds()[i];
                    assert current.assignments()[parentOrd] == cluster;
                    current.assignments()[parentOrd] = (short) (splitClusters.assignments()[i] + orgCentroidsSize - 1);
                }
            }
        }
    }
}
