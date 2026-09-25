/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Head-to-head comparison benchmark: old Gaussian i.i.d. random projection
 * vs new orthogonal (Gram-Schmidt) random projection.
 * <p>
 * Both implementations are tested on identical data and sample sets.
 * Metrics: precision, recall, distance-ratio variance, and wall-clock time.
 */
public class ProjectionComparisonBenchmarkTests extends ESTestCase {

    private static final Logger benchLogger = LogManager.getLogger(ProjectionComparisonBenchmarkTests.class);

    /** Accumulates results across all test methods for final summary */
    private static final StringBuilder ALL_RESULTS = new StringBuilder();

    private void emit(StringBuilder sb) {
        String text = sb.toString();
        benchLogger.info(() -> text);
        synchronized (ALL_RESULTS) {
            ALL_RESULTS.append(text);
        }
    }

    // ────────────────────────────────────────────────────────
    // Old Gaussian projection (inlined from commit 037119c)
    // ────────────────────────────────────────────────────────
    private static float[][] buildGaussianMatrix(int originalDim, int projectedDim, long seed) {
        Random rng = new Random(seed);
        float scale = (float) Math.sqrt(1.0 / projectedDim);
        float[][] matrix = new float[projectedDim][originalDim];
        for (int i = 0; i < projectedDim; i++) {
            for (int j = 0; j < originalDim; j++) {
                matrix[i][j] = (float) rng.nextGaussian() * scale;
            }
        }
        return matrix;
    }

    private static float[] projectGaussian(float[][] matrix, float[] vector) {
        float[] result = new float[matrix.length];
        for (int i = 0; i < matrix.length; i++) {
            float dot = 0f;
            for (int j = 0; j < vector.length; j++) {
                dot += matrix[i][j] * vector[j];
            }
            result[i] = dot;
        }
        return result;
    }

    private static float[][] projectBatchGaussian(float[][] matrix, float[][] vectors) {
        float[][] results = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            results[i] = projectGaussian(matrix, vectors[i]);
        }
        return results;
    }

    // ════════════════════════════════════════════════════════
    // Scenario 1: Gaussian cluster + isolated outliers
    // ════════════════════════════════════════════════════════
    public void testGaussianClusterComparison() {
        int[] sizes = { 500, 1000, 2000, 5000 };
        int dims = 50;
        int projDims = 15;
        int numOutliers = 10;
        int k = 5;
        long seed = 42L;

        StringBuilder report = header("GAUSSIAN CLUSTER + ISOLATED OUTLIERS (D=" + dims + " -> D'=" + projDims + ")");

        for (int n : sizes) {
            float[][] vectors = generateGaussianWithOutliers(n, dims, numOutliers, seed);
            Set<Integer> trueOutliers = outlierIndices(n, numOutliers);
            appendComparison(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed);
        }

        footer(report, dims, projDims, k, numOutliers);
        emit(report);
    }

    // ════════════════════════════════════════════════════════
    // Scenario 2: Multi-cluster + inter-cluster outliers
    // ════════════════════════════════════════════════════════
    public void testMultiClusterComparison() {
        int[] sizes = { 500, 1000, 2000, 5000 };
        int dims = 40;
        int projDims = 12;
        int numOutliers = 6;
        int k = 5;
        long seed = 99L;
        int numClusters = 3;

        StringBuilder report = header("MULTI-CLUSTER + INTER-CLUSTER OUTLIERS (D=" + dims + " -> D'=" + projDims + ")");

        for (int n : sizes) {
            float[][] vectors = generateMultiCluster(n, dims, numOutliers, numClusters, seed);
            Set<Integer> trueOutliers = outlierIndices(n, numOutliers);
            appendComparison(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed);
        }

        footer(report, dims, projDims, k, numOutliers);
        emit(report);
    }

    // ════════════════════════════════════════════════════════
    // Scenario 3: High-dimensional (D=200 -> D'=30)
    // ════════════════════════════════════════════════════════
    public void testHighDimensionalComparison() {
        int[] sizes = { 500, 1000, 2000 };
        int dims = 200;
        int projDims = 30;
        int numOutliers = 10;
        int k = 5;
        long seed = 42L;

        StringBuilder report = header("HIGH-DIMENSIONAL (D=" + dims + " -> D'=" + projDims + ")");

        for (int n : sizes) {
            float[][] vectors = generateGaussianWithOutliers(n, dims, numOutliers, seed);
            Set<Integer> trueOutliers = outlierIndices(n, numOutliers);
            appendComparison(report, vectors, trueOutliers, n, dims, projDims, numOutliers, k, seed);
        }

        footer(report, dims, projDims, k, numOutliers);
        emit(report);
    }

    // ════════════════════════════════════════════════════════
    // Scenario 4: Projection dimension sweep
    // ════════════════════════════════════════════════════════
    public void testProjectionDimensionSweep() {
        int n = 2000;
        int dims = 50;
        int[] projDimsArray = { 3, 5, 10, 15, 25, 40 };
        int numOutliers = 10;
        int k = 5;
        long seed = 42L;

        float[][] vectors = generateGaussianWithOutliers(n, dims, numOutliers, seed);
        Set<Integer> trueOutliers = outlierIndices(n, numOutliers);

        // Brute-force baseline
        double[] bfScores = OutlierDetectionAggregator.computeKnnDistances(vectors, k);
        Set<Integer> bfTop = topNSet(bfScores, numOutliers);
        double bfPrec = precision(bfTop, trueOutliers);
        double bfRec = recall(bfTop, trueOutliers);

        int sampleSize = Math.max(k + 1, (int) Math.ceil(Math.sqrt(n)));
        int[] sampleIdx = reservoirSample(n, sampleSize, seed);

        StringBuilder report = new StringBuilder();
        report.append("\n").append("=".repeat(130)).append("\n");
        report.append("PROJECTION DIM SWEEP: Gaussian vs Orthogonal (N=").append(n);
        report.append(", D=").append(dims).append(", k=").append(k);
        report.append(", outliers=").append(numOutliers).append(", sample=").append(sampleSize).append(")\n");
        report.append(
            String.format("%-4s | %-22s | %-30s | %-30s | %s%n", "D'", "Brute-Force", "Gaussian (old)", "Orthogonal (new)", "Improvement")
        );
        report.append("-".repeat(130)).append("\n");

        // Brute-force row
        report.append(
            String.format(
                "%-4s | %8s  P=%.2f R=%.2f | %8s  %18s | %8s  %18s | %s%n",
                "—",
                "exact",
                bfPrec,
                bfRec,
                "—",
                "—",
                "—",
                "—",
                "baseline"
            )
        );

        for (int projDims : projDimsArray) {
            // Old Gaussian projection
            float[][] gaussMatrix = buildGaussianMatrix(dims, projDims, seed);
            long t0 = System.nanoTime();
            float[][] gaussAll = projectBatchGaussian(gaussMatrix, vectors);
            float[][] gaussSample = new float[sampleSize][];
            for (int i = 0; i < sampleSize; i++) {
                gaussSample[i] = gaussAll[sampleIdx[i]];
            }
            double[] gaussScores = new double[n];
            for (int i = 0; i < n; i++) {
                gaussScores[i] = InternalOutlierDetection.computeKthNnDistance(gaussAll[i], gaussSample, k);
            }
            long gaussNs = System.nanoTime() - t0;
            Set<Integer> gaussTop = topNSet(gaussScores, numOutliers);
            double gaussPrec = precision(gaussTop, trueOutliers);
            double gaussRec = recall(gaussTop, trueOutliers);
            double gaussDistVar = computeDistanceRatioVariance(vectors, gaussAll, seed);

            // New orthogonal projection
            RandomProjection orthoProj = new RandomProjection(dims, projDims, seed);
            long t1 = System.nanoTime();
            float[][] orthoAll = orthoProj.projectBatch(vectors);
            float[][] orthoSample = new float[sampleSize][];
            for (int i = 0; i < sampleSize; i++) {
                orthoSample[i] = orthoAll[sampleIdx[i]];
            }
            double[] orthoScores = new double[n];
            for (int i = 0; i < n; i++) {
                orthoScores[i] = InternalOutlierDetection.computeKthNnDistance(orthoAll[i], orthoSample, k);
            }
            long orthoNs = System.nanoTime() - t1;
            Set<Integer> orthoTop = topNSet(orthoScores, numOutliers);
            double orthoPrec = precision(orthoTop, trueOutliers);
            double orthoRec = recall(orthoTop, trueOutliers);
            double orthoDistVar = computeDistanceRatioVariance(vectors, orthoAll, seed);

            String improvement;
            if (orthoRec > gaussRec) {
                improvement = String.format("+%.0f%% recall", (orthoRec - gaussRec) * 100);
            } else if (orthoRec == gaussRec && orthoDistVar < gaussDistVar) {
                improvement = String.format("%.1f%% lower var", (1 - orthoDistVar / gaussDistVar) * 100);
            } else if (orthoRec == gaussRec) {
                improvement = "equal";
            } else {
                improvement = String.format("-%.0f%% recall", (gaussRec - orthoRec) * 100);
            }

            report.append(
                String.format(
                    "%-4d | %8.2f  P=%.2f R=%.2f | %8.2f  P=%.2f R=%.2f v=%.4f | %8.2f  P=%.2f R=%.2f v=%.4f | %s%n",
                    projDims,
                    bfScores.length > 0 ? 0.0 : 0.0,
                    bfPrec,
                    bfRec,
                    gaussNs / 1e6,
                    gaussPrec,
                    gaussRec,
                    gaussDistVar,
                    orthoNs / 1e6,
                    orthoPrec,
                    orthoRec,
                    orthoDistVar,
                    improvement
                )
            );
        }

        report.append("-".repeat(130)).append("\n");
        report.append("=".repeat(130)).append("\n\n");
        emit(report);
    }

    // ════════════════════════════════════════════════════════
    // Scenario 5: Distance ratio variance comparison
    // Measures how faithfully each projection preserves
    // pairwise distances (lower variance = better)
    // ════════════════════════════════════════════════════════
    public void testDistanceRatioVariance() {
        int n = 1000;
        int dims = 50;
        int[] projDimsArray = { 5, 10, 15, 25, 40 };
        long seed = 42L;
        int numPairs = 200;

        float[][] vectors = generateGaussianWithOutliers(n, dims, 10, seed);

        StringBuilder report = new StringBuilder();
        report.append("\n").append("=".repeat(80)).append("\n");
        report.append("DISTANCE RATIO VARIANCE (N=").append(n).append(", D=").append(dims);
        report.append(", pairs=").append(numPairs).append(")\n");
        report.append("Lower variance = projection preserves distances more faithfully.\n");
        report.append(String.format("%-6s | %12s | %12s | %12s%n", "D'", "Gauss var", "Ortho var", "Improvement"));
        report.append("-".repeat(80)).append("\n");

        for (int projDims : projDimsArray) {
            float[][] gaussMatrix = buildGaussianMatrix(dims, projDims, seed);
            float[][] gaussAll = projectBatchGaussian(gaussMatrix, vectors);
            double gaussVar = computeDistanceRatioVariance(vectors, gaussAll, seed, numPairs);

            RandomProjection orthoProj = new RandomProjection(dims, projDims, seed);
            float[][] orthoAll = orthoProj.projectBatch(vectors);
            double orthoVar = computeDistanceRatioVariance(vectors, orthoAll, seed, numPairs);

            double improvement = gaussVar > 0 ? (1 - orthoVar / gaussVar) * 100 : 0;
            report.append(String.format("%-6d | %12.6f | %12.6f | %+10.1f%%%n", projDims, gaussVar, orthoVar, improvement));
        }

        report.append("-".repeat(80)).append("\n");
        report.append("=".repeat(80)).append("\n\n");
        emit(report);
    }

    // ════════════════════════════════════════════════════════
    // Core comparison runner
    // ════════════════════════════════════════════════════════

    private void appendComparison(
        StringBuilder report,
        float[][] vectors,
        Set<Integer> trueOutliers,
        int n,
        int dims,
        int projDims,
        int numOutliers,
        int k,
        long seed
    ) {
        int sampleSize = Math.max(k + 1, (int) Math.ceil(Math.sqrt(n)));
        int[] sampleIdx = reservoirSample(n, sampleSize, seed);

        // Brute-force baseline
        long t0 = System.nanoTime();
        double[] bfScores = OutlierDetectionAggregator.computeKnnDistances(vectors, k);
        long bfNs = System.nanoTime() - t0;
        Set<Integer> bfTop = topNSet(bfScores, numOutliers);
        double bfPrec = precision(bfTop, trueOutliers);
        double bfRec = recall(bfTop, trueOutliers);

        // Old Gaussian projection
        float[][] gaussMatrix = buildGaussianMatrix(dims, projDims, seed);
        long t1 = System.nanoTime();
        float[][] gaussAll = projectBatchGaussian(gaussMatrix, vectors);
        float[][] gaussSample = new float[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            gaussSample[i] = gaussAll[sampleIdx[i]];
        }
        double[] gaussScores = new double[n];
        for (int i = 0; i < n; i++) {
            gaussScores[i] = InternalOutlierDetection.computeKthNnDistance(gaussAll[i], gaussSample, k);
        }
        long gaussNs = System.nanoTime() - t1;
        Set<Integer> gaussTop = topNSet(gaussScores, numOutliers);
        double gaussPrec = precision(gaussTop, trueOutliers);
        double gaussRec = recall(gaussTop, trueOutliers);

        // New orthogonal projection
        RandomProjection orthoProj = new RandomProjection(dims, projDims, seed);
        long t2 = System.nanoTime();
        float[][] orthoAll = orthoProj.projectBatch(vectors);
        float[][] orthoSample = new float[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            orthoSample[i] = orthoAll[sampleIdx[i]];
        }
        double[] orthoScores = new double[n];
        for (int i = 0; i < n; i++) {
            orthoScores[i] = InternalOutlierDetection.computeKthNnDistance(orthoAll[i], orthoSample, k);
        }
        long orthoNs = System.nanoTime() - t2;
        Set<Integer> orthoTop = topNSet(orthoScores, numOutliers);
        double orthoPrec = precision(orthoTop, trueOutliers);
        double orthoRec = recall(orthoTop, trueOutliers);

        double speedupGauss = (double) bfNs / Math.max(gaussNs, 1);
        double speedupOrtho = (double) bfNs / Math.max(orthoNs, 1);

        report.append(
            String.format(
                "%-6d | %8.2f  P=%.2f R=%.2f | %8.2f  P=%.2f R=%.2f %5.1fx | %8.2f  P=%.2f R=%.2f %5.1fx%n",
                n,
                bfNs / 1e6,
                bfPrec,
                bfRec,
                gaussNs / 1e6,
                gaussPrec,
                gaussRec,
                speedupGauss,
                orthoNs / 1e6,
                orthoPrec,
                orthoRec,
                speedupOrtho
            )
        );
    }

    // ════════════════════════════════════════════════════════
    // Data generation
    // ════════════════════════════════════════════════════════

    private static float[][] generateGaussianWithOutliers(int n, int dims, int numOutliers, long seed) {
        Random rng = new Random(seed);
        float[][] vectors = new float[n][dims];

        for (int i = 0; i < n - numOutliers; i++) {
            for (int d = 0; d < dims; d++) {
                vectors[i][d] = (float) (rng.nextGaussian() * 0.5);
            }
        }
        for (int i = n - numOutliers; i < n; i++) {
            int oi = i - (n - numOutliers);
            for (int d = 0; d < dims; d++) {
                float offset = (d % numOutliers == oi) ? 50.0f : 0.0f;
                vectors[i][d] = offset + (float) (rng.nextGaussian() * 0.1);
            }
        }
        return vectors;
    }

    private static float[][] generateMultiCluster(int n, int dims, int numOutliers, int numClusters, long seed) {
        Random rng = new Random(seed);
        float[][] vectors = new float[n][dims];
        float[][] centers = new float[numClusters][dims];

        for (int c = 0; c < numClusters; c++) {
            for (int d = 0; d < dims; d++) {
                centers[c][d] = (d % numClusters == c) ? 30.0f : 0.0f;
            }
        }

        int normalCount = n - numOutliers;
        for (int i = 0; i < normalCount; i++) {
            int cluster = i % numClusters;
            for (int d = 0; d < dims; d++) {
                vectors[i][d] = centers[cluster][d] + (float) (rng.nextGaussian() * 0.4);
            }
        }

        for (int i = normalCount; i < n; i++) {
            int oi = i - normalCount;
            for (int d = 0; d < dims; d++) {
                vectors[i][d] = 80.0f + ((d % numOutliers == oi) ? 40.0f : 0.0f) + (float) (rng.nextGaussian() * 0.05);
            }
        }
        return vectors;
    }

    private static Set<Integer> outlierIndices(int n, int numOutliers) {
        Set<Integer> indices = new HashSet<>();
        for (int i = n - numOutliers; i < n; i++) {
            indices.add(i);
        }
        return indices;
    }

    // ════════════════════════════════════════════════════════
    // Distance ratio variance
    // ════════════════════════════════════════════════════════

    private static double computeDistanceRatioVariance(float[][] original, float[][] projected, long seed) {
        return computeDistanceRatioVariance(original, projected, seed, 100);
    }

    private static double computeDistanceRatioVariance(float[][] original, float[][] projected, long seed, int numPairs) {
        Random rng = new Random(seed + 7);
        int n = original.length;
        double[] ratios = new double[numPairs];
        int valid = 0;

        for (int p = 0; p < numPairs; p++) {
            int i = rng.nextInt(n);
            int j = rng.nextInt(n);
            if (i == j) {
                j = (j + 1) % n;
            }
            double origDist = OutlierDetectionAggregator.squaredEuclidean(original[i], original[j]);
            double projDist = OutlierDetectionAggregator.squaredEuclidean(projected[i], projected[j]);
            if (origDist > 0) {
                ratios[valid++] = projDist / origDist;
            }
        }

        if (valid == 0) return 0;

        double mean = 0;
        for (int i = 0; i < valid; i++) {
            mean += ratios[i];
        }
        mean /= valid;

        double variance = 0;
        for (int i = 0; i < valid; i++) {
            double diff = ratios[i] - mean;
            variance += diff * diff;
        }
        return variance / valid;
    }

    // ════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════

    private static StringBuilder header(String title) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n").append("=".repeat(120)).append("\n");
        sb.append(title).append("\n");
        sb.append(String.format("%-6s | %-22s | %-30s | %-30s%n", "N", "Brute-Force", "Gaussian (old)", "Orthogonal (new)"));
        sb.append("-".repeat(120)).append("\n");
        return sb;
    }

    private static void footer(StringBuilder sb, int dims, int projDims, int k, int numOutliers) {
        sb.append("-".repeat(120)).append("\n");
        sb.append(String.format("dims=%d -> %d | k=%d | outliers=%d | sample=ceil(sqrt(N))%n", dims, projDims, k, numOutliers));
        sb.append("=".repeat(120)).append("\n\n");
    }

    private static Set<Integer> topNSet(double[] scores, int n) {
        int[][] indexed = new int[scores.length][1];
        for (int i = 0; i < scores.length; i++) {
            indexed[i][0] = i;
        }
        Arrays.sort(indexed, (a, b) -> Double.compare(scores[b[0]], scores[a[0]]));
        Set<Integer> result = new HashSet<>();
        for (int i = 0; i < Math.min(n, scores.length); i++) {
            result.add(indexed[i][0]);
        }
        return result;
    }

    private static double precision(Set<Integer> predicted, Set<Integer> actual) {
        if (predicted.isEmpty()) return 0.0;
        long tp = predicted.stream().filter(actual::contains).count();
        return (double) tp / predicted.size();
    }

    private static double recall(Set<Integer> predicted, Set<Integer> actual) {
        if (actual.isEmpty()) return 1.0;
        long tp = predicted.stream().filter(actual::contains).count();
        return (double) tp / actual.size();
    }

    private static int[] reservoirSample(int n, int k, long seed) {
        Random rng = new Random(seed);
        int[] reservoir = new int[k];
        for (int i = 0; i < k; i++) {
            reservoir[i] = i;
        }
        for (int i = k; i < n; i++) {
            int r = rng.nextInt(i + 1);
            if (r < k) {
                reservoir[r] = i;
            }
        }
        return reservoir;
    }
}
