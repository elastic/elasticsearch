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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.containsString;

public class BalancedKMeansLocalTests extends ESTestCase {

    public void testIllegalClustersPerNeighborhood() {
        KMeansLocal<float[]> kMeansLocal = new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, randomInt(), randomInt());
        KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(new float[0][], new int[0], i -> i);
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
        // This test uses float vectors specifically because it computes inertia via VectorUtil.squareDistance.

        int nClusters = 20;
        int nVectors = nClusters * 256;
        final int sampleSize = nClusters * 64;
        int dims = 256;
        int maxIterations = 20;
        float soarLambda = -1;

        var methods = List.of(
            new LloydKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations), // reference method
            new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations)
        );

        float[] inertias = new float[methods.size()];
        float[] stdClusterSizes = new float[methods.size()];
        // Both geometric means are computed in log domain for numerical stability.
        double geoMeanInertiaRatio = 0; // the geometric mean of the ratios between both inertias
        double geoMeanStdClusterSizes = 0; // the geometric mean of the ratios between the standard deviations of the cluster sizes

        int nTrials = 10;

        for (int trials = 0; trials < nTrials; trials++) {
            KMeansFloatVectorValues vectors = generateFloatData(nVectors, dims, nClusters, 0.5f);

            for (int j = 0; j < methods.size(); j++) {
                float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.FLOAT);
                int[] assignments = new int[vectors.size()];
                KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments);
                methods.get(j).cluster(vectors, kMeansIntermediate, nClusters, soarLambda);

                inertias[j] = kMeansMeanInertia(vectors, kMeansIntermediate);
                int[] clusterSizes = clusterSizes(kMeansIntermediate);
                stdClusterSizes[j] = clusterSizesStandardDeviation(clusterSizes);
            }

            geoMeanInertiaRatio += Math.log(inertias[0] / inertias[1]);
            geoMeanStdClusterSizes += Math.log((stdClusterSizes[1] + 1e-6f) / (stdClusterSizes[0] + 1e-6f));
        }

        geoMeanInertiaRatio = Math.exp(geoMeanInertiaRatio / nTrials);
        geoMeanStdClusterSizes = Math.exp(geoMeanStdClusterSizes / nTrials);

        // We allow up to 20% increase in inertia:
        assertTrue(geoMeanInertiaRatio > 0.8);
        // We ask for 3X decrease in the size distribution.
        assertTrue(geoMeanStdClusterSizes < 0.33);
    }

    public void testBalancedOTKMeansLocal() throws IOException {
        int nClusters = randomIntBetween(2, 10);
        int nVectors = nClusters * randomIntBetween(50, 200);
        int dims = randomIntBetween(4, 32);
        int sampleSize = randomIntBetween(100, nVectors);
        int maxIterations = randomIntBetween(2, 20);

        boolean useByte = randomBoolean();
        if (useByte) {
            KMeansByteVectorValues vectors = generateByteData(nVectors, dims, nClusters);
            byte[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.BYTE);
            int[] assignments = new int[nVectors];
            KMeansIntermediate<byte[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments);

            KMeansLocal<byte[]> kMeansLocal = new BalancedOTKMeansLocalSerial<>(CentroidOps.BYTE, sampleSize, maxIterations);
            kMeansLocal.cluster(vectors, kMeansIntermediate, nClusters, -1f);

            for (int a : kMeansIntermediate.assignments()) {
                assertTrue("Invalid assignment: " + a, a >= 0 && a < centroids.length);
            }
        } else {
            KMeansFloatVectorValues vectors = generateFloatData(nVectors, dims, nClusters, 0.5f);
            float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.FLOAT);
            int[] assignments = new int[nVectors];
            KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments);

            KMeansLocal<float[]> kMeansLocal = new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations);
            kMeansLocal.cluster(vectors, kMeansIntermediate, nClusters, -1f);

            for (int a : kMeansIntermediate.assignments()) {
                assertTrue("Invalid assignment: " + a, a >= 0 && a < centroids.length);
            }
        }
    }

    static float kMeansMeanInertia(KMeansFloatVectorValues vectors, KMeansIntermediate<float[]> kMeansIntermediate) throws IOException {
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

    static int[] clusterSizes(KMeansIntermediate<float[]> kMeansIntermediate) {
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

        boolean useByte = randomBoolean();
        if (useByte) {
            List<byte[]> vectors = new ArrayList<>();
            for (int i = 0; i < nVectors; i++) {
                vectors.add(new byte[5]);
            }
            int sampleSize = vectors.size();
            KMeansByteVectorValues bvv = KMeansByteVectorValues.build(vectors, null, 5);

            byte[][] centroids = KMeansLocal.pickInitialCentroids(bvv, nClusters, CentroidOps.BYTE);
            BalancedOTKMeansLocal.cluster(bvv, CentroidOps.BYTE, centroids, sampleSize, maxIterations);

            int[] assignments = new int[vectors.size()];
            int[] assignmentOrdinals = new int[vectors.size()];
            for (int i = 0; i < vectors.size(); i++) {
                assignments[i] = 0;
                assignmentOrdinals[i] = i;
            }

            KMeansIntermediate<byte[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
            KMeansLocal<byte[]> kMeansLocal = new BalancedOTKMeansLocalSerial<>(CentroidOps.BYTE, sampleSize, maxIterations);
            kMeansLocal.cluster(bvv, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

            assertEquals(nClusters, centroids.length);
            assertNotNull(kMeansIntermediate.soarAssignments());
            for (byte[] centroid : centroids) {
                for (byte v : centroid) {
                    assertEquals(0, v);
                }
            }
        } else {
            List<float[]> vectors = new ArrayList<>();
            for (int i = 0; i < nVectors; i++) {
                vectors.add(new float[5]);
            }
            int sampleSize = vectors.size();
            KMeansFloatVectorValues fvv = KMeansFloatVectorValues.build(vectors, null, 5);

            float[][] centroids = KMeansLocal.pickInitialCentroids(fvv, nClusters, CentroidOps.FLOAT);
            BalancedOTKMeansLocal.cluster(fvv, CentroidOps.FLOAT, centroids, sampleSize, maxIterations);

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

            KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
            KMeansLocal<float[]> kMeansLocal = new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations);
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
    }

    public void testKMeansNeighbors() throws IOException {
        int nClusters = random().nextInt(1, 10);
        int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int sampleSize = random().nextInt(100, nVectors + 1);
        int maxIterations = random().nextInt(0, 100);
        // We require clustersPerNeighborhood > nClusters so that neighborhoods are not used in BalancedOTKMeansLocalSerial
        int clustersPerNeighborhood = random().nextInt(11, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);

        boolean useByte = randomBoolean();
        if (useByte) {
            KMeansByteVectorValues vectors = generateByteData(nVectors, dims, nClusters);
            byte[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.BYTE);
            LloydKMeansLocal.cluster(vectors, CentroidOps.BYTE, centroids, sampleSize, maxIterations);

            int[] assignments = new int[vectors.size()];
            int[] assignmentOrdinals = new int[vectors.size()];
            for (int i = 0; i < vectors.size(); i++) {
                float minDist = Float.MAX_VALUE;
                int ord = -1;
                byte[] vec = vectors.vectorValue(i);
                for (int j = 0; j < centroids.length; j++) {
                    float dist = CentroidOps.BYTE.squareDistance(vec, centroids[j]);
                    if (dist < minDist) {
                        minDist = dist;
                        ord = j;
                    }
                }
                assignments[i] = ord;
                assignmentOrdinals[i] = i;
            }

            KMeansIntermediate<byte[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
            KMeansLocal<byte[]> kMeansLocal = new BalancedOTKMeansLocalSerial<>(CentroidOps.BYTE, sampleSize, maxIterations);
            kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

            assertEquals(nClusters, centroids.length);
            assertNotNull(kMeansIntermediate.soarAssignments());
        } else {
            KMeansFloatVectorValues vectors = generateFloatData(nVectors, dims, nClusters, 0.5f);
            float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.FLOAT);
            LloydKMeansLocal.cluster(vectors, CentroidOps.FLOAT, centroids, sampleSize, maxIterations);

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

            KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, i -> assignmentOrdinals[i]);
            KMeansLocal<float[]> kMeansLocal = new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, sampleSize, maxIterations);
            kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);

            assertEquals(nClusters, centroids.length);
            assertNotNull(kMeansIntermediate.soarAssignments());
        }
    }

    public void testConcurrency() throws IOException {
        // Test to compare BalancedKMeansLocalSerial and BalancedKMeansLocalConcurrent.
        final int nClusters = 16;
        final int nVectors = nClusters * 500;
        final int nSamples = nClusters * 256;
        final int dims = 1024;
        final int maxIterations = 10;
        final float soarLambda = -1;
        final int numThreads = randomIntBetween(2, 8);

        KMeansFloatVectorValues vectors = generateFloatData(nVectors, dims, 1, 0.2f);

        try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
            TaskExecutor taskExecutor = new TaskExecutor(executorService);

            var methods = List.of(
                new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, nSamples / 2, maxIterations),
                new BalancedOTKMeansLocalConcurrent<>(CentroidOps.FLOAT, taskExecutor, numThreads, nSamples / 2, maxIterations)
            );

            double[] inertias = new double[methods.size()];
            double[] stdClusterSizes = new double[methods.size()];
            int[] minClusterSizes = new int[methods.size()];

            for (int j = 0; j < methods.size(); j++) {
                float[][] centroids = KMeansLocal.pickInitialCentroids(vectors, nClusters, CentroidOps.FLOAT);
                int[] assignments = new int[vectors.size()];
                KMeansIntermediate<float[]> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments);
                methods.get(j).cluster(vectors, kMeansIntermediate, nClusters, soarLambda);

                inertias[j] = kMeansMeanInertia(vectors, kMeansIntermediate);
                int[] clusterSizesArr = clusterSizes(kMeansIntermediate);

                stdClusterSizes[j] = clusterSizesStandardDeviation(clusterSizesArr);
                minClusterSizes[j] = Arrays.stream(clusterSizesArr).min().getAsInt();
            }

            assertEquals(inertias[0], inertias[1], 2e-4 * inertias[0]);
            assertEquals(stdClusterSizes[0], stdClusterSizes[1], 2e-1 * stdClusterSizes[0]);
            assertEquals(minClusterSizes[0], minClusterSizes[1], 2e-1 * minClusterSizes[0]);
        }
    }

    // ---- Data generators ----

    private static KMeansFloatVectorValues generateFloatData(int nSamples, int nDims, int nClusters, float standardDeviationPreCluster) {
        List<float[]> vectors = new ArrayList<>(nSamples);
        float[][] centroids = new float[nClusters][nDims];
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = random().nextFloat();
            }
        }
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

    private static KMeansByteVectorValues generateByteData(int nSamples, int nDims, int nClusters) {
        List<byte[]> vectors = new ArrayList<>(nSamples);
        byte[][] centroids = new byte[nClusters][nDims];
        for (int i = 0; i < nClusters; i++) {
            for (int j = 0; j < nDims; j++) {
                centroids[i][j] = (byte) randomIntBetween(-128, 127);
            }
        }
        for (int i = 0; i < nSamples; i++) {
            int cluster = random().nextInt(nClusters);
            byte[] vector = new byte[nDims];
            for (int j = 0; j < nDims; j++) {
                vector[j] = (byte) Math.clamp(centroids[cluster][j] + randomIntBetween(-10, 10), -128, 127);
            }
            vectors.add(vector);
        }
        return KMeansByteVectorValues.build(vectors, null, nDims);
    }
}
