/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class BalancedKMeansLocalTests extends ESTestCase {

    public void testIllegalClustersPerNeighborhood() {
        KMeansLocal kMeansLocal = new BalancedKMeansLocalSerial(randomInt(), randomInt());
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(new float[0][], new int[0], i -> i);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> kMeansLocal.cluster(
                KMeansFloatVectorValues.build(List.of(), null, randomInt(1024)),
                kMeansIntermediate,
                randomIntBetween(Integer.MIN_VALUE, 1),
                randomFloat()
            )
        );
        assertThat(ex.getMessage(), containsString("clustersPerNeighborhood must be at least 2"));
    }

    public void testBalancedKMeans() throws IOException {
        // Test to compare LloydKMeansLocal and BalancedKMeansLocal.
        // We do not use clustersPerNeighborhood because there is clever centroid initialization here.

        int nClusters = random().nextInt(2, 10); // Pick the number of clusters between 1 and 10 in increments of 10
        int nVectors = nClusters * 100;
        final int sampleSize = nClusters * 100;
        int dims = random().nextInt(1, 20) * 100; // Pick the dimensions between 100 and 2000 in increments of 100
        int maxIterations = random().nextInt(20, 50);
        float soarLambda = -1;

        KMeansFloatVectorValues vectors = generateData(nVectors, dims, nClusters, 0.5f);

        var methods = List.of(
            new LloydKMeansLocalSerial(sampleSize, maxIterations), // reference method
            new BalancedKMeansLocalSerial(sampleSize, maxIterations)
        );

        float[] inertias = new float[methods.size()];
        float[] stdClusterSizes = new float[methods.size()];

        for (int j = 0; j < methods.size(); j++) {
            float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters);
            int[] assignments = new int[vectors.size()];
            KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments);
            methods.get(j).cluster(vectors, kMeansIntermediate, nClusters, soarLambda);

            inertias[j] = kMeansMeanInertia(vectors, kMeansIntermediate);
            int[] clusterSizes = clusterSizes(kMeansIntermediate);

            stdClusterSizes[j] = clusterSizesStandardDeviation(clusterSizes);
        }

        // We allow up to 1% increase in inertia:
        assertTrue(inertias[0] * 1.05 > inertias[1]);
        // We ask for a net improvement in the size distribution. In the vast majority of the runs,
        // the decrease in the standard deviation of the cluster sizes is very significant.
        // Once in a while, the stars align and the k-means result is pretty well-balanced. In those cases,
        // balanced k-means does not do worse but cannot improve any further.
        assertTrue(stdClusterSizes[0] >= stdClusterSizes[1]);
    }

    static float kMeansMeanInertia(KMeansFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate) throws IOException {
        int[] assignments = kMeansIntermediate.assignments();
        float[][] centroids = kMeansIntermediate.centroids();

        float mse = 0;
        for (int i = 0; i < vectors.size(); i++) {
            float[] vec = vectors.vectorValue(i);
            float[] cent = centroids[assignments[i]];
            float dist = VectorUtil.squareDistance(vec, cent);
            mse += dist / vectors.size();
        }
        return mse;
    }

    static int[] clusterSizes(KMeansIntermediate kMeansIntermediate) {
        int[] assignments = kMeansIntermediate.assignments();
        float[][] centroids = kMeansIntermediate.centroids();

        int[] clusterSizes = new int[centroids.length];
        for (int i = 0; i < assignments.length; i++) {
            clusterSizes[assignments[i]]++;
        }
        return clusterSizes;
    }

    static float clusterSizesStandardDeviation(int[] clusterSizes) {
        double avgSize = Arrays.stream(clusterSizes).asDoubleStream().sum() / clusterSizes.length;
        double varSize = Arrays.stream(clusterSizes).asDoubleStream().map(e -> Math.pow(e - avgSize, 2)).sum() / clusterSizes.length;
        return (float) Math.sqrt(varSize);
    }

    public void testKMeansNeighborsAllZero() throws IOException {
        int nClusters = 10;
        int maxIterations = 10;
        int clustersPerNeighborhood = 128;
        float soarLambda = 1.0f;
        int nVectors = 1000;
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < nVectors; i++) {
            float[] vector = new float[5];
            vectors.add(vector);
        }
        int sampleSize = vectors.size();
        KMeansFloatVectorValues fvv = KMeansFloatVectorValues.build(vectors, null, 5);

        float[][] centroids = KMeansLocal.pickInitialCentroids(fvv, nClusters);
        BalancedKMeansLocal.cluster(fvv, centroids, sampleSize, maxIterations);

        int[] assignments = new int[vectors.size()];
        int[] assignmentOrdinals = new int[vectors.size()];
        for (int i = 0; i < vectors.size(); i++) {
            float minDist = Float.MAX_VALUE;
            int ord = -1;
            for (int j = 0; j < centroids.length; j++) {
                float dist = VectorUtil.squareDistance(fvv.vectorValue(i), centroids[j]);
                if (dist < minDist) {
                    minDist = dist;
                    ord = j;
                }
            }
            assignments[i] = ord;
            assignmentOrdinals[i] = i;
        }

        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments, i -> assignmentOrdinals[i]);
        KMeansLocal kMeansLocal = new BalancedKMeansLocalSerial(sampleSize, maxIterations);
        kMeansLocal.cluster(fvv, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

        assertEquals(nClusters, centroids.length);
        assertNotNull(kMeansIntermediate.soarAssignments());
        for (float[] centroid : centroids) {
            for (float v : centroid) {
                if (v > 0.0000001f) {
                    assertEquals(0.0f, v, 0.00000001f);
                }
            }
        }
    }

    public void testKMeansNeighbors() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(100, nVectors + 1);
        int maxIterations = random().nextInt(0, 100);
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);
        KMeansFloatVectorValues vectors = generateData(nVectors, dims, nClusters, 0.5f);

        float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters);
        LloydKMeansLocal.cluster(vectors, centroids, sampleSize, maxIterations);

        int[] assignments = new int[vectors.size()];
        int[] assignmentOrdinals = new int[vectors.size()];
        for (int i = 0; i < vectors.size(); i++) {
            float minDist = Float.MAX_VALUE;
            int ord = -1;
            for (int j = 0; j < centroids.length; j++) {
                float dist = VectorUtil.squareDistance(vectors.vectorValue(i), centroids[j]);
                if (dist < minDist) {
                    minDist = dist;
                    ord = j;
                }
            }
            assignments[i] = ord;
            assignmentOrdinals[i] = i;
        }

        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments, i -> assignmentOrdinals[i]);
        KMeansLocal kMeansLocal = new BalancedKMeansLocalSerial(sampleSize, maxIterations);
        kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

        assertEquals(nClusters, centroids.length);
        assertNotNull(kMeansIntermediate.soarAssignments());
    }

    private static KMeansFloatVectorValues generateData(int nSamples, int nDims, int nClusters, float standardDeviationPreCluster) {
        List<float[]> vectors = new ArrayList<>(nSamples);
        float[][] centroids = new float[nClusters][nDims];
        // Generate random centroids
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = random().nextFloat();
            }
        }
        // Generate data points around centroids
        for (int i = 0; i < nSamples; i++) {
            int cluster = random().nextInt(nClusters);
            float[] vector = new float[nDims];
            for (int j = 0; j < nDims; j++) {
                vector[j] = centroids[cluster][j] + (float) random().nextGaussian() * standardDeviationPreCluster;
            }
            vectors.add(vector);
        }

        return KMeansFloatVectorValues.build(vectors, null, nDims);
    }
}
