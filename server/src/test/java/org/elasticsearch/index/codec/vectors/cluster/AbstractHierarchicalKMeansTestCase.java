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
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Abstract base class for HierarchicalKMeans tests, parameterized by vector type.
 */
public abstract class AbstractHierarchicalKMeansTestCase<V> extends ESTestCase {

    protected abstract CentroidOps<V> centroidOps();

    protected abstract ClusteringVectorValues<V> generateData(int nSamples, int nDims, int nClusters);

    public void testNumClustersForTargetSize() {
        assertEquals(32, HierarchicalKMeans.numClustersForTargetSize(8192, 256));
        assertEquals(2, HierarchicalKMeans.numClustersForTargetSize(400, 256));
    }

    public void testWarmStartMatchesColdStartClusterCount() throws IOException {
        float[][] rows = {
            { 1f, 0f, 0f, 0f },
            { 0.9f, 0.1f, 0f, 0f },
            { 0.8f, 0.2f, 0f, 0f },
            { 0.7f, 0.3f, 0f, 0f },
            { 0.6f, 0.4f, 0f, 0f },
            { 0.5f, 0.5f, 0f, 0f },
            { 0.4f, 0.6f, 0f, 0f },
            { 0.3f, 0.7f, 0f, 0f },
            { 0.2f, 0.8f, 0f, 0f },
            { 0.1f, 0.9f, 0f, 0f },
            { 0f, 1f, 0f, 0f },
            { -0.1f, 0.9f, 0f, 0f } };
        KMeansFloatVectorValues vectors = KMeansFloatVectorValues.build(List.of(rows), null, 4);
        HierarchicalKMeans<float[]> kmeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, 4);
        KMeansNeighbors<float[]> cold = kmeans.cluster(vectors, 4);
        KMeansNeighbors<float[]> warm = kmeans.cluster(vectors, 4, cold.centroids());
        assertEquals(cold.centroids().length, warm.centroids().length);
        assertEquals(cold.assignments().length, warm.assignments().length);
    }

    /**
     * Verifies that passing warm-start centroids to {@link HierarchicalKMeans#cluster} does not
     * mutate the caller's arrays. Lloyd passes update centroids in place, so
     * {@code HierarchicalKMeans#initialCentroidsForClustering} must deep-copy the warm-start
     * contents rather than copying references.
     */
    public void testWarmStartCentroidsAreNotMutated() throws IOException {
        float[][] rows = {
            { 1f, 0f, 0f, 0f },
            { 0.9f, 0.1f, 0f, 0f },
            { 0.8f, 0.2f, 0f, 0f },
            { 0.7f, 0.3f, 0f, 0f },
            { 0.6f, 0.4f, 0f, 0f },
            { 0.5f, 0.5f, 0f, 0f },
            { 0.4f, 0.6f, 0f, 0f },
            { 0.3f, 0.7f, 0f, 0f },
            { 0.2f, 0.8f, 0f, 0f },
            { 0.1f, 0.9f, 0f, 0f },
            { 0f, 1f, 0f, 0f },
            { -0.1f, 0.9f, 0f, 0f } };
        KMeansFloatVectorValues vectors = KMeansFloatVectorValues.build(List.of(rows), null, 4);
        HierarchicalKMeans<float[]> kmeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, 4);
        KMeansNeighbors<float[]> first = kmeans.cluster(vectors, 4);
        float[][] warmStart = first.centroids();

        // snapshot contents before the warm-start call
        float[][] snapshot = new float[warmStart.length][4];
        for (int i = 0; i < warmStart.length; i++) {
            System.arraycopy(warmStart[i], 0, snapshot[i], 0, 4);
        }

        kmeans.cluster(vectors, 4, warmStart);

        for (int i = 0; i < warmStart.length; i++) {
            assertArrayEquals("warmStart[" + i + "] was mutated by cluster()", snapshot[i], warmStart[i], 0f);
        }
    }

    public void testGrowingWarmStartMatchesColdStartClusterCount() throws IOException {
        int dim = 4;
        int targetSize = 128;
        float[][] rows = syntheticClusteredRows(5200, dim, 8);
        FloatVectorValues full = KMeansFloatVectorValues.build(List.of(rows), null, dim);
        int[] ordinals4096 = new int[4096];
        int[] ordinals5120 = new int[5120];
        for (int i = 0; i < ordinals4096.length; i++) {
            ordinals4096[i] = i;
        }
        for (int i = 0; i < ordinals5120.length; i++) {
            ordinals5120[i] = i;
        }
        KMeansFloatVectorValues prefix4096 = KMeansFloatVectorValues.wrap(full, ordinals4096, ordinals4096.length);
        KMeansFloatVectorValues prefix5120 = KMeansFloatVectorValues.wrap(full, ordinals5120, ordinals5120.length);

        HierarchicalKMeans<float[]> kmeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, dim);
        KMeansNeighbors<float[]> small = kmeans.cluster(prefix4096, targetSize);
        KMeansNeighbors<float[]> coldLarge = kmeans.cluster(prefix5120, targetSize);
        KMeansNeighbors<float[]> warmLarge = kmeans.cluster(prefix5120, targetSize, small.centroids());
        assertEquals(coldLarge.centroids().length, warmLarge.centroids().length);
        assertEquals(coldLarge.assignments().length, warmLarge.assignments().length);
    }

    private static float[][] syntheticClusteredRows(int count, int dim, int numClusters) {
        float[][] centroids = new float[numClusters][dim];
        for (int c = 0; c < numClusters; c++) {
            for (int d = 0; d < dim; d++) {
                centroids[c][d] = (c + 1) * 0.1f + d * 0.01f;
            }
            float norm = 0;
            for (int d = 0; d < dim; d++) {
                norm += centroids[c][d] * centroids[c][d];
            }
            norm = (float) Math.sqrt(norm);
            for (int d = 0; d < dim; d++) {
                centroids[c][d] /= norm;
            }
        }
        float[][] rows = new float[count][dim];
        for (int i = 0; i < count; i++) {
            System.arraycopy(centroids[i % numClusters], 0, rows[i], 0, dim);
            rows[i][i % dim] += 0.001f * (i % 5);
            float norm = 0;
            for (int d = 0; d < dim; d++) {
                norm += rows[i][d] * rows[i][d];
            }
            norm = (float) Math.sqrt(norm);
            for (int d = 0; d < dim; d++) {
                rows[i][d] /= norm;
            }
        }
        return rows;
    }

    /** Wraps an array of centroids into a ClusteringVectorValues view for testing. */
    protected abstract ClusteringVectorValues<V> wrapAsView(V[] centroids, int dim);

    public void testHKmeans() throws IOException {
        int nClusters = random().nextInt(2, 10);
        int nVectors = random().nextInt(nClusters * 100, nClusters * 200);
        int dims = random().nextInt(8, 64);
        int sampleSize = random().nextInt(Math.min(nVectors, 100), nVectors + 1);
        int maxIterations = HierarchicalKMeans.MAX_ITERATIONS_DEFAULT;
        int clustersPerNeighborhood = random().nextInt(2, 512);
        float soarLambda = random().nextFloat(0.5f, 1.5f);
        int targetSize = (int) ((float) nVectors / (float) nClusters);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);

        // Warmup passes: the Panama Vector API SIMD operations (linearCombination, squareDistanceBulk,
        // blendBatchIntoCentroid) produce different floating-point results in interpreted mode (scalar)
        // vs JIT-compiled mode (SIMD) due to fused multiply-add using higher intermediate precision
        // in SIMD lanes. Without warmup, the JIT compilation threshold can be crossed mid-SGD,
        // causing serial and concurrent runs to produce different centroid positions — not due to a
        // concurrency bug, but due to different JIT compilation states between invocations.
        // We warm up both the serial path and the concurrent path on the same thread pool that will
        // be used for the real comparison, because JIT compilation is triggered per-method by
        // invocation counts — thread-pool threads that haven't executed the SIMD methods yet may
        // still be running interpreted code while the main thread is fully compiled.
        int numWorker = randomIntBetween(2, 8);
        try (ExecutorService service = Executors.newFixedThreadPool(numWorker)) {
            TaskExecutor executor = new TaskExecutor(service);
            for (int w = 0; w < 3; w++) {
                HierarchicalKMeans.ofSerial(ops, dims, maxIterations, sampleSize, clustersPerNeighborhood).cluster(vectors, targetSize);
                HierarchicalKMeans.ofConcurrent(ops, dims, executor, numWorker, maxIterations, sampleSize, clustersPerNeighborhood)
                    .cluster(vectors, targetSize);
            }

            HierarchicalKMeans<V> hkmeansSerial = HierarchicalKMeans.ofSerial(
                ops,
                dims,
                maxIterations,
                sampleSize,
                clustersPerNeighborhood
            );
            var serialResult = hkmeansSerial.cluster(vectors, targetSize);
            var serialOverspill = hkmeansSerial.computeSoar(vectors, serialResult.result(), serialResult.neighborHoods(), soarLambda);
            assertKMeansResultValid(serialResult.result(), serialOverspill, nVectors, nClusters);

            int[] serialClusterSizes = new int[serialResult.centroids().length];
            for (int k : serialResult.assignments()) {
                serialClusterSizes[k]++;
            }

            HierarchicalKMeans<V> hkmeansConcurrent = HierarchicalKMeans.ofConcurrent(
                ops,
                dims,
                executor,
                numWorker,
                maxIterations,
                sampleSize,
                clustersPerNeighborhood
            );
            var concurrentResult = hkmeansConcurrent.cluster(vectors, targetSize);
            var concurrentOverspill = hkmeansSerial.computeSoar(
                vectors,
                concurrentResult.result(),
                concurrentResult.neighborHoods(),
                soarLambda
            );
            assertKMeansResultValid(concurrentResult.result(), concurrentOverspill, nVectors, nClusters);

            // After JIT warmup, serial and concurrent produce identical results.
            assertEquals(
                "Serial and concurrent produced different centroid counts",
                serialResult.centroids().length,
                concurrentResult.centroids().length
            );
            assertArrayEquals(
                "Serial and concurrent produced different assignments",
                serialResult.assignments(),
                concurrentResult.assignments()
            );

            // Quality bound: with production-realistic parameters (sufficient vectors,
            // reasonable dimensionality, production maxIterations), no cluster should
            // exceed 25% over target size.
            int maxAllowed = (int) (targetSize * 1.25) + 5;
            int maxClusterSize = Arrays.stream(serialClusterSizes).max().orElse(0);
            assertTrue(
                "Max cluster size " + maxClusterSize + " exceeds allowed bound " + maxAllowed + " (targetSize=" + targetSize + ")",
                maxClusterSize <= maxAllowed || serialResult.centroids().length == 1
            );
        }
    }

    public void testFewDifferentValues() throws IOException {
        int nVectors = random().nextInt(100, 1000);
        int targetSize = random().nextInt(4, 64);
        int dims = random().nextInt(2, 20);
        int diffValues = randomIntBetween(1, 5);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateFewDistinctData(nVectors, dims, diffValues);

        HierarchicalKMeans<V> hkmeans = HierarchicalKMeans.ofSerial(
            ops,
            dims,
            random().nextInt(1, 100),
            random().nextInt(Math.min(nVectors, 100), nVectors + 1),
            random().nextInt(2, 512)
        );

        var result = hkmeans.cluster(vectors, targetSize);
        var overspill = hkmeans.computeSoar(vectors, result.result(), result.neighborHoods(), random().nextFloat(0.5f, 1.5f));
        assertKMeansResultValid(result.result(), overspill, nVectors, -1);
    }

    /**
     * Verify that SOAR assignments never collide with primary assignments after empty clusters
     * are removed. This exercises the neighborhood remapping in removeEmptyClusters: when empty
     * centroids are compacted out and neighbor indices are remapped, no neighbor should be mapped
     * to a vector's own primary centroid.
     *
     * The test creates a dataset with fewer natural clusters than what the algorithm targets,
     * uses a small clustersPerNeighborhood to force neighborhood-aware SOAR, and repeats across
     * random parameters to cover different empty-cluster scenarios.
     */
    public void testSoarAssignmentsValidAfterEmptyClusterRemoval() throws IOException {
        CentroidOps<V> ops = centroidOps();
        for (int trial = 0; trial < 200; trial++) {
            // Use few natural clusters but many vectors, so the algorithm over-partitions
            // and some clusters end up empty after refinement.
            int naturalClusters = randomIntBetween(2, 4);
            int nVectors = randomIntBetween(200, 1000);
            int dims = randomIntBetween(4, 32);

            ClusteringVectorValues<V> vectors = generateData(nVectors, dims, naturalClusters);

            // Small clustersPerNeighborhood ensures neighborhoods are active when centroids > this value
            int clustersPerNeighborhood = 2;
            // Very small target size forces many centroids, maximizing chance of empty clusters
            int targetSize = randomIntBetween(3, 10);
            float soarLambda = randomFloat() * 0.5f + 0.5f;
            // Low maxIterations increases chance of poorly-converged clusters that become empty
            int maxIterations = randomIntBetween(1, 5);

            HierarchicalKMeans<V> hkmeans = HierarchicalKMeans.ofSerial(
                ops,
                dims,
                maxIterations,
                randomIntBetween(50, nVectors),
                clustersPerNeighborhood
            );

            var result = hkmeans.cluster(vectors, targetSize);
            var overspill = hkmeans.computeSoar(vectors, result.result(), result.neighborHoods(), soarLambda);

            int[] assignments = result.assignments();

            if (result.centroids().length > 1 && result.centroids().length < nVectors) {
                assertEquals(nVectors, overspill.size());
                for (int i = 0; i < assignments.length; i++) {
                    var it = overspill.getAssignmentsFor(i);
                    if (it.hasNext()) {
                        assertNotEquals(
                            "SOAR assignment collides with primary assignment for vector "
                                + i
                                + " (both assigned to centroid "
                                + assignments[i]
                                + ")",
                            assignments[i],
                            it.nextInt()
                        );
                    }
                }
            }
        }
    }

    protected abstract ClusteringVectorValues<V> generateFewDistinctData(int nVectors, int dims, int diffValues);

    public void testClusterByInsertion() throws IOException {
        int nClusters = random().nextInt(2, 8);
        int nVectors = random().nextInt(nClusters * 10, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int targetSize = (int) ((float) nVectors / nClusters);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);

        // First, do a full cluster to get "initial centroids" (simulating a dominant segment's priors)
        HierarchicalKMeans<V> hkmeans = HierarchicalKMeans.ofSerial(ops, dims);
        KMeansNeighbors<V> result = hkmeans.cluster(vectors, targetSize);
        var overspill = hkmeans.computeSoar(vectors, result.result(), result.neighborHoods());
        assertKMeansResultValid(result.result(), overspill, nVectors, nClusters);

        // Now use those centroids as initial seeds for clusterByInsertion
        ClusteringVectorValues<V> priorView = wrapAsView(result.centroids(), dims);
        KMeansWithOverspill<V> insertionResult = hkmeans.clusterByInsertion(vectors, priorView, targetSize);
        assertKMeansResultValid(insertionResult.result(), insertionResult.overspill(), nVectors, nClusters);
    }

    public void testClusterByConcatenation() throws IOException {
        int nClusters = random().nextInt(2, 8);
        int nVectors = random().nextInt(nClusters * 10, nClusters * 200);
        int dims = random().nextInt(2, 20);
        int targetSize = (int) ((float) nVectors / nClusters);

        CentroidOps<V> ops = centroidOps();
        ClusteringVectorValues<V> vectors = generateData(nVectors, dims, nClusters);

        // Full cluster to get "prior centroids" simulating concatenated priors from multiple segments
        HierarchicalKMeans<V> hkmeans = HierarchicalKMeans.ofSerial(ops, dims);
        var fullResult = hkmeans.cluster(vectors, targetSize);
        var overspill = hkmeans.computeSoar(vectors, fullResult.result(), fullResult.neighborHoods());
        assertKMeansResultValid(fullResult.result(), overspill, nVectors, nClusters);

        int[] clusterSizes = fullResult.result().clusterCounts();
        ClusteringVectorValues<V> priorView = wrapAsView(fullResult.centroids(), dims);

        KMeansWithOverspill<V> concatResult = hkmeans.clusterByConcatenation(vectors, priorView, clusterSizes, nVectors, targetSize);
        assertKMeansResultValid(concatResult.result(), concatResult.overspill(), nVectors, nClusters);
    }

    // ---- Helpers ----

    protected static <V> void assertKMeansResultValid(
        KMeansResult<V> result,
        OverspillAssignments overspill,
        int nVectors,
        int expectedClusters
    ) {
        V[] centroids = result.centroids();
        int[] assignments = result.assignments();

        if (expectedClusters > 0) {
            assertEquals(Math.min(expectedClusters, nVectors), centroids.length, 25);
        }
        assertThat(centroids, not(emptyArray()));

        for (int assignment : assignments) {
            assertThat(assignment, both(greaterThanOrEqualTo(0)).and(lessThan(centroids.length)));
        }

        // Verify no empty clusters
        int[] counts = new int[centroids.length];
        for (int a : assignments) {
            counts[a]++;
        }
        for (int count : counts) {
            assertThat("Empty cluster found", count, greaterThan(0));
        }
        assertArrayEquals(counts, result.clusterCounts());

        if (centroids.length > 1 && centroids.length < nVectors) {
            // verify no duplicates exist
            for (int i = 0; i < assignments.length; i++) {
                for (var it = overspill.getAssignmentsFor(i); it.hasNext();) {
                    int os = it.nextInt();
                    assertThat(os, both(greaterThanOrEqualTo(0)).and(lessThan(centroids.length)));
                    assertNotEquals(assignments[i], os);
                }
            }
        } else {
            assertThat(overspill.size(), is(0));
        }
    }

}
