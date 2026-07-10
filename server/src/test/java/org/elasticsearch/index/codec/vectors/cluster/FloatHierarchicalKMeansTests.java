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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FloatHierarchicalKMeansTests extends AbstractHierarchicalKMeansTestCase<float[]> {

    @Override
    protected CentroidOps<float[]> centroidOps() {
        return CentroidOps.FLOAT;
    }

    @Override
    protected ClusteringVectorValues<float[]> generateData(int nSamples, int nDims, int nClusters) {
        return KMeansTestData.generateFloatData(nSamples, nDims, nClusters);
    }

    @Override
    protected ClusteringVectorValues<float[]> wrapAsView(float[][] centroids, int dim) {
        return KMeansFloatVectorValues.build(Arrays.asList(centroids), null, dim);
    }

    @Override
    protected ClusteringVectorValues<float[]> generateFewDistinctData(int nVectors, int dims, int diffValues) {
        float[][] values = new float[diffValues][dims];
        for (int i = 0; i < diffValues; i++) {
            for (int j = 0; j < dims; j++) {
                values[i][j] = random().nextFloat();
            }
        }
        List<float[]> vectorList = new ArrayList<>(nVectors);
        for (int i = 0; i < nVectors; i++) {
            vectorList.add(values[random().nextInt(diffValues)]);
        }
        return KMeansFloatVectorValues.build(vectorList, null, dims);
    }

    /**
     * Proves that the serial and concurrent clustering paths produce identical results when
     * JIT compilation is stable. The Panama Vector API SIMD operations (linearCombination,
     * squareDistanceBulk, blendBatchIntoCentroid) can produce different floating-point results
     * depending on whether they execute in interpreted mode (scalar) vs JIT-compiled mode (SIMD),
     * because fused multiply-add in SIMD lanes uses higher intermediate precision than separate
     * multiply+add in scalar code. As the JVM warms up, methods transition from interpreted to
     * compiled mid-computation, causing the SGD trajectory to diverge between invocations.
     * <p>
     * This test forces JIT compilation via 3 warmup passes, then verifies:
     * 1. Serial-serial: two serial runs produce byte-for-byte identical centroids and assignments
     * 2. Serial-concurrent: concurrent produces byte-for-byte identical results to serial
     * <p>
     * This confirms there is no concurrency bug — the divergence observed in {@code testHKmeans}
     * (which uses structural bounds instead of exact equality) is solely due to JIT warmup
     * non-determinism, not a thread-safety issue.
     */
    public void testSgdDeterministicAfterWarmup() throws IOException {
        int nClusters = randomIntBetween(4, 9);
        int nVectors = randomIntBetween(200, 800);
        int dims = randomIntBetween(3, 16);
        int sampleSize = randomIntBetween(Math.min(nVectors, 100), nVectors);
        int maxIterations = randomIntBetween(2, 20);
        int targetSize = (int) ((float) nVectors / (float) nClusters);
        int k = Math.clamp((int) ((nVectors + targetSize / 2.0f) / (float) targetSize), 2, 128);
        int m = Math.min(k * sampleSize, nVectors);

        KMeansFloatVectorValues vectors = KMeansTestData.generateFloatData(nVectors, dims, nClusters);
        float[][] initialCentroids = KMeansLocal.pickInitialCentroids(vectors, k, CentroidOps.FLOAT);

        // === Warmup pass: force JIT compilation of all SIMD hot paths ===
        // Multiple warmup iterations to ensure all code paths are fully compiled and stable
        for (int w = 0; w < 3; w++) {
            float[][] warmupCentroids = new float[k][dims];
            for (int i = 0; i < k; i++) {
                System.arraycopy(initialCentroids[i], 0, warmupCentroids[i], 0, dims);
            }
            int[] warmupAssignments = new int[nVectors];
            Arrays.fill(warmupAssignments, -1);
            KMeansIntermediate<float[]> warmupIntermediate = new KMeansIntermediate<>(
                warmupCentroids,
                warmupAssignments,
                vectors::ordToDoc
            );
            new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, m, maxIterations).cluster(vectors, warmupIntermediate);
        }

        // === Run 1: Serial ===
        float[][] serialCentroids1 = new float[k][dims];
        for (int i = 0; i < k; i++) {
            System.arraycopy(initialCentroids[i], 0, serialCentroids1[i], 0, dims);
        }
        int[] serialAssignments1 = new int[nVectors];
        Arrays.fill(serialAssignments1, -1);
        KMeansIntermediate<float[]> serialIntermediate1 = new KMeansIntermediate<>(serialCentroids1, serialAssignments1, vectors::ordToDoc);
        new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, m, maxIterations).cluster(vectors, serialIntermediate1);

        // === Run 2: Serial again ===
        float[][] serialCentroids2 = new float[k][dims];
        for (int i = 0; i < k; i++) {
            System.arraycopy(initialCentroids[i], 0, serialCentroids2[i], 0, dims);
        }
        int[] serialAssignments2 = new int[nVectors];
        Arrays.fill(serialAssignments2, -1);
        KMeansIntermediate<float[]> serialIntermediate2 = new KMeansIntermediate<>(serialCentroids2, serialAssignments2, vectors::ordToDoc);
        new BalancedOTKMeansLocalSerial<>(CentroidOps.FLOAT, m, maxIterations).cluster(vectors, serialIntermediate2);

        // Verify serial is deterministic after warmup
        assertEquals(
            "Serial runs produced different centroid counts after warmup",
            serialIntermediate1.centroids().length,
            serialIntermediate2.centroids().length
        );
        for (int c = 0; c < serialIntermediate1.centroids().length; c++) {
            assertArrayEquals(
                "Serial centroid "
                    + c
                    + " differs between two runs after warmup (nVectors="
                    + nVectors
                    + ", dims="
                    + dims
                    + ", k="
                    + k
                    + ", maxIters="
                    + maxIterations
                    + ")",
                serialIntermediate1.centroids()[c],
                serialIntermediate2.centroids()[c],
                0f
            );
        }
        assertArrayEquals("Serial assignments differ between two runs after warmup", serialAssignments1, serialAssignments2);

        // === Run 3: Concurrent ===
        int numWorkers = randomIntBetween(2, 8);
        try (ExecutorService service = Executors.newFixedThreadPool(numWorkers)) {
            TaskExecutor executor = new TaskExecutor(service);

            float[][] concurrentCentroids = new float[k][dims];
            for (int i = 0; i < k; i++) {
                System.arraycopy(initialCentroids[i], 0, concurrentCentroids[i], 0, dims);
            }
            int[] concurrentAssignments = new int[nVectors];
            Arrays.fill(concurrentAssignments, -1);
            KMeansIntermediate<float[]> concurrentIntermediate = new KMeansIntermediate<>(
                concurrentCentroids,
                concurrentAssignments,
                vectors::ordToDoc
            );
            new BalancedOTKMeansLocalConcurrent<>(CentroidOps.FLOAT, executor, numWorkers, m, maxIterations).cluster(
                vectors,
                concurrentIntermediate
            );

            // Verify concurrent matches serial after warmup
            assertEquals(
                "Serial and concurrent produced different centroid counts after warmup",
                serialIntermediate1.centroids().length,
                concurrentIntermediate.centroids().length
            );
            for (int c = 0; c < serialIntermediate1.centroids().length; c++) {
                assertArrayEquals(
                    "Serial vs concurrent centroid "
                        + c
                        + " differs after warmup (nVectors="
                        + nVectors
                        + ", dims="
                        + dims
                        + ", k="
                        + k
                        + ", numWorkers="
                        + numWorkers
                        + ", maxIters="
                        + maxIterations
                        + ")",
                    serialIntermediate1.centroids()[c],
                    concurrentIntermediate.centroids()[c],
                    0f
                );
            }
            assertArrayEquals("Serial vs concurrent assignments differ after warmup", serialAssignments1, concurrentAssignments);
        }
    }
}
