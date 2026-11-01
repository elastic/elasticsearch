/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.search.TaskExecutor;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.oneOf;

/**
 * Tests for the core ASH algorithm components: subspace training, quantizers, and the full pipeline.
 */
public class AsymmetricHashingQuantizerTests extends ESTestCase {

    private static ExecutorService executor;

    @BeforeClass
    public static void setup() {
        executor = Executors.newFixedThreadPool(16);
    }

    @AfterClass
    public static void stop() {
        executor.close();
    }

    private TaskExecutor executor() {
        return randomBoolean() ? new TaskExecutor(executor) : null;
    }

    public void testProcrustesOrthogonal() {
        // Procrustes of a random matrix should return orthogonal matrix (R^T R = I)
        int k = 5;
        float[] m = AshUtils.randomGaussians(random(), k * k);
        float[] r = new float[k * k];
        AshUtils.procrustes(m, k, r, executor());
        // Check R^T R ~= I
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                float dot = 0;
                for (int l = 0; l < k; l++) {
                    dot = Math.fma(r[l * k + i], r[l * k + j], dot);
                }
                float expected = (i == j) ? 1.0f : 0.0f;
                assertEquals(expected, dot, 1e-4f);
            }
        }
    }

    public void testProcrustesIsOrthogonalAndKeepsTheObjective() {
        // M = U diag(s) V^T with a known spectrum, so the exact polar factor U V^T and its objective sum(s) are known. The
        // singular values fall from 1 to 1e-3, as they do for the matrices the Procrustes loop works on, and it is the small
        // ones that take Newton-Schulz the most steps to bring up to 1
        int k = 128;
        float[] u = AshUtils.randomOrthogonal(random(), k, executor());
        float[] v = AshUtils.randomOrthogonal(random(), k, executor());
        double[] s = new double[k];
        double sumOfSingularValues = 0;
        for (int i = 0; i < k; i++) {
            s[i] = Math.pow(1e-3, i / (double) (k - 1));
            sumOfSingularValues += s[i];
        }
        float[] m = new float[k * k];
        float[] exact = new float[k * k];
        for (int a = 0; a < k; a++) {
            for (int b = 0; b < k; b++) {
                double polar = 0;
                double matrix = 0;
                for (int i = 0; i < k; i++) {
                    double product = (double) u[a * k + i] * v[b * k + i];
                    polar += product;
                    matrix += product * s[i];
                }
                exact[a * k + b] = (float) polar;
                m[a * k + b] = (float) matrix;
            }
        }

        float[] r = new float[k * k];
        AshUtils.procrustes(m, k, r, executor());

        // R^T R = I, to the tolerance the iteration stops at
        double worstOffOrthogonal = 0;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                double dot = 0;
                for (int l = 0; l < k; l++) {
                    dot += (double) r[l * k + i] * r[l * k + j];
                }
                worstOffOrthogonal = Math.max(worstOffOrthogonal, Math.abs(dot - (i == j ? 1 : 0)));
            }
        }
        assertThat("distance of R^T R from I", worstOffOrthogonal, lessThan(2.0 * AshUtils.ORTHOGONALITY_TOLERANCE));
        // the objective tr(R^T M) is that of the exact polar factor, which maximizes it over all orthogonal matrices
        double objective = 0;
        double worstDifference = 0;
        for (int i = 0; i < k * k; i++) {
            objective += (double) r[i] * m[i];
            worstDifference = Math.max(worstDifference, Math.abs(r[i] - exact[i]));
        }
        assertEquals("objective relative to the exact polar factor's", 1.0, objective / sumOfSingularValues, 1e-5);
        assertThat("largest entry of R minus the exact polar factor", worstDifference, lessThan(2e-3));
    }

    public void testProcrustesOfAWellConditionedMatrixIsOrthogonal() {
        int k = 96;
        float[] m = AshUtils.randomGaussians(random(), k * k);
        float[] r = new float[k * k];

        AshUtils.procrustes(m, k, r, executor());

        // The columns of R, since the iteration bounds the largest entry of R^T R - I. Nothing bounds R R^T, which for a
        // random matrix can be about three times worse and made this test fail for about one seed in a thousand
        assertOrthonormalRows(ESVectorUtil.transposeMatrix(r, k, k), k, k, 2.0f * AshUtils.ORTHOGONALITY_TOLERANCE);
    }

    public void testProcrustesDoesNotStopWhileASmallSingularValueIsBeingBroughtUp() {
        // M = I - 0.99 s s^T with s spread evenly over the coordinates has one singular value of 0.01, along s, and all the
        // others 1, so its exact polar factor is I. The largest entry of X^T X - I stays at about 1 / k while that singular
        // value is brought up, which at k = 1024 is below a stopping gate of 1e-3: stopping on "the error stopped
        // decreasing" there returned the matrix with the singular value still at 0.01
        int k = 1024;
        float[] s = new float[k];
        Arrays.fill(s, (float) (1.0 / Math.sqrt(k)));
        float[] m = new float[k * k];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                m[i * k + j] = (i == j ? 1f : 0f) - 0.99f * s[i] * s[j];
            }
        }
        float[] r = new float[k * k];

        AshUtils.procrustes(m, k, r, executor());

        // R is symmetric here, so R s is the singular value of R along s times s
        float[] rs = ESVectorUtil.matrixVectorMultiply(r, k, k, s);
        double singularValue = Math.sqrt(ESVectorUtil.dotProduct(rs, rs));
        assertThat("singular value of R along the direction that was small", singularValue, greaterThan(0.9));
    }

    public void testRandomOrthogonal() {
        int n = 5;
        float[] r = AshUtils.randomOrthogonal(random(), n, executor());
        assertOrthonormalRows(r, n, n, 1e-4f);
    }

    public void testDominantSubspaceOfDiagonalMatrix() {
        // Second-moment matrix with descending diagonal: the dominant 2-d subspace is the first two axes
        int n = 4;
        int k = 2;
        float[] c = new float[n * n];
        c[0 * n + 0] = 16f;
        c[1 * n + 1] = 9f;
        c[2 * n + 2] = 4f;
        c[3 * n + 3] = 1f;

        float[] bT = AshUtils.dominantSubspace(c, n, k, 42L, executor());
        assertEquals(k * n, bT.length);
        assertOrthonormalRows(bT, k, n, 1e-4f);
        for (int j = 0; j < k; j++) {
            // all of each basis vector's energy sits in the first two coordinates
            float inSpan = bT[j * n] * bT[j * n] + bT[j * n + 1] * bT[j * n + 1];
            assertEquals("row " + j + " leaves the dominant subspace", 1f, inSpan, 1e-3f);
        }
    }

    public void testDominantSubspaceRecoversPlantedSubspace() {
        int n = 64;
        int rank = 8;
        float[] planted = plantedSubspace(n, rank);
        // C = 10 * sum(u u^T) + 0.01 * I: a clear gap, so the planted subspace is the dominant one
        float[] c = new float[n * n];
        for (int u = 0; u < rank; u++) {
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    c[i * n + j] += 10f * planted[u * n + i] * planted[u * n + j];
                }
            }
        }
        for (int i = 0; i < n; i++) {
            c[i * n + i] += 0.01f;
        }

        float[] bT = AshUtils.dominantSubspace(c, n, rank, randomLong(), executor());
        assertOrthonormalRows(bT, rank, n, 1e-3f);
        assertThat(capturedEnergy(planted, rank, bT, rank, n), greaterThan(rank - 1e-2f));
    }

    public void testOrthonormalizeRowsKeepsSpan() {
        // more rows than one block, so the blocked path with its inter-block projections is exercised
        int n = 300;
        int k = AshUtils.ORTHONORMALIZE_BLOCK + 72;
        float[] original = AshUtils.randomGaussians(random(), k * n);
        float[] rows = original.clone();

        AshUtils.orthonormalizeRows(rows, n, k, executor());

        assertOrthonormalRows(rows, k, n, 1e-4f);
        // every original row lies in the span of the result: projecting onto it keeps the whole norm
        for (int i = 0; i < k; i++) {
            float[] row = Arrays.copyOfRange(original, i * n, (i + 1) * n);
            float[] coordinates = ESVectorUtil.matrixVectorMultiply(rows, k, n, row);
            float norm = ESVectorUtil.dotProduct(row, row);
            assertEquals("row " + i + " left the span", norm, ESVectorUtil.dotProduct(coordinates, coordinates), norm * 1e-4f);
        }
        // and the leading rows still span what the leading original rows spanned
        int leading = 10;
        float[] leadingRows = Arrays.copyOf(rows, leading * n);
        for (int i = 0; i < leading; i++) {
            float[] row = Arrays.copyOfRange(original, i * n, (i + 1) * n);
            float[] coordinates = ESVectorUtil.matrixVectorMultiply(leadingRows, leading, n, row);
            float norm = ESVectorUtil.dotProduct(row, row);
            assertEquals("leading row " + i, norm, ESVectorUtil.dotProduct(coordinates, coordinates), norm * 1e-4f);
        }
    }

    public void testOrthonormalizeRowsOfNearlyParallelRows() {
        // what repeated multiplication by a second-moment matrix produces: every row is dominated by the same direction
        int n = 512;
        int k = AshUtils.ORTHONORMALIZE_BLOCK * 2 + 30;
        float[] common = AshUtils.randomGaussians(random(), n);
        float[] rows = AshUtils.randomGaussians(random(), k * n);
        for (int i = 0; i < k; i++) {
            ESVectorUtil.linearCombination(300f, common, 0, rows, i * n, n);
        }

        AshUtils.orthonormalizeRows(rows, n, k, executor());

        assertOrthonormalRows(rows, k, n, 1e-3f);
    }

    public void testDominantSubspaceOfSlowlyDecayingSpectrum() {
        // eigenvalues 0.97^i: no clear gap at k, like the edge of the subspace in real embeddings. The subspace is not
        // resolved there, but the energy it captures must be close to the best possible
        int n = 128;
        int k = 32;
        float[] c = new float[n * n];
        double total = 0;
        double best = 0;
        for (int i = 0; i < n; i++) {
            double lambda = Math.pow(0.97, i);
            c[i * n + i] = (float) lambda;
            total += lambda;
            if (i < k) {
                best += lambda;
            }
        }

        float[] bT = AshUtils.dominantSubspace(c, n, k, randomLong(), executor());

        assertOrthonormalRows(bT, k, n, 1e-3f);
        double captured = 0;
        for (int j = 0; j < k; j++) {
            for (int i = 0; i < n; i++) {
                captured += (double) c[i * n + i] * bT[j * n + i] * bT[j * n + i];
            }
        }
        assertThat("captured energy fraction", captured / total, greaterThan(best / total - 2e-3));
    }

    public void testRefineSubspaceWithSingleMultiplyIsOrthonormal() {
        int n = 40;
        int k = 10;
        float[] planted = plantedSubspace(n, k);
        float[] c = new float[n * n];
        for (int u = 0; u < k; u++) {
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    c[i * n + j] += planted[u * n + i] * planted[u * n + j];
                }
            }
        }
        float[] start = AshUtils.randomGaussians(random(), k * n);
        AshUtils.orthonormalizeRows(start, n, k, executor());

        float[] refined = AshUtils.refineSubspace(c, n, k, start, 1, executor());

        assertOrthonormalRows(refined, k, n, 1e-4f);
    }

    public void testMaxProjectedDims() {
        final int maxDims = 1024;
        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            maxDims,
            1,
            AsymmetricHashingQuantizer.Method.RANDOM,
            0,
            1,
            42L,
            executor()
        );

        assertThat(quantizer.nDims(128), equalTo(64));
        assertThat(quantizer.nDims(1024), equalTo(512));
        assertThat(quantizer.nDims(2046), equalTo(1023));
        assertThat(quantizer.nDims(2048), equalTo(maxDims));
        assertThat(quantizer.nDims(2050), equalTo(maxDims));
        assertThat(quantizer.nDims(4096), equalTo(maxDims));
    }

    public void testReconstructedDotProductApproximatesTrueDotProduct() throws IOException {
        // With no dimensionality reduction (projectedDimsFraction=1.0, so nDims == originalDim and W
        // is a random orthogonal matrix -- a pure rotation, not a projection), the only source of
        // reconstruction error is the quantization of the residual (vector - centroid). This
        // isolates the quantizer's fidelity, matching the near-linear ⟨q, x⟩ ~ ⟨q, quant(x)⟩
        // relationship reported for ASH (see https://arxiv.org/pdf/2606.07870, Figure 4 and the
        // surrounding "estimator bias" discussion, which notes the fit is not exact -- there's a
        // small, bitsPerDim-dependent bias -- but the pairs are tightly clustered around the line).
        //
        // Thresholds below were calibrated empirically (aggregate relative RMSE over many random
        // query/vector/centroid trials, stable across seeds) with a safety margin of roughly
        // 1.5x (3 bits), 2x (4 bits), 5x (8 bits) over the observed values, to catch a real regression without being
        // flaky.
        int dim = 128;
        int nVectors = 200;
        // does not divide nVectors, so each run encodes full blocks and one partial block
        int maxBlockSize = 32;

        record Config(int bitsPerDim, double relRmseThreshold) {}

        for (var config : List.of(new Config(3, 0.9), new Config(4, 0.35), new Config(8, 0.05))) {
            AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
                1.0f,
                Integer.MAX_VALUE,
                config.bitsPerDim(),
                AsymmetricHashingQuantizer.Method.RANDOM,
                0,
                1,
                42L,
                executor()
            );
            var w = train(quantizer, new float[][] { new float[dim] }, i -> new float[dim]);
            int nDims = quantizer.nDims(dim); // == dim since projectedDimsFraction=1.0

            float[] centroid = AshUtils.randomGaussians(random(), dim);
            float[] query = AshUtils.randomGaussians(random(), dim);
            float[][] vectors = new float[nVectors][];
            for (int i = 0; i < nVectors; i++) {
                vectors[i] = AshUtils.randomGaussians(random(), dim);
            }

            // Raw query projection: qt = wT @ query
            float[] qt = ESVectorUtil.matrixVectorMultiply(w.wT(), nDims, dim, query);
            float queryDotCentroid = ESVectorUtil.dotProduct(query, centroid, dim);
            AsymmetricHashingQuantizer.VectorAndNorm precomputed = AsymmetricHashingQuantizer.precomputeCentroid(centroid, w.wT());

            try (AsymmetricHashingQuantizer.BlockEncoder encoder = quantizer.newBlockEncoder(w.w(), dim, maxBlockSize)) {
                double sumSqErr = 0;
                double sumSqTrue = 0;
                for (int start = 0; start < nVectors; start += maxBlockSize) {
                    int blockSize = Math.min(maxBlockSize, nVectors - start);
                    encoder.reset();
                    for (int j = 0; j < blockSize; j++) {
                        encoder.add(vectors[start + j], centroid);
                    }
                    encoder.encode(precomputed);

                    for (int j = 0; j < blockSize; j++) {
                        float trueDot = ESVectorUtil.dotProduct(query, vectors[start + j], dim);

                        byte[] packed = ESVectorUtil.ashPack(encoder.code(j), config.bitsPerDim());
                        float reconstructed = referenceScore(
                            qt,
                            new float[] { queryDotCentroid },
                            packed,
                            0,
                            nDims,
                            config.bitsPerDim(),
                            encoder.scale(j),
                            encoder.offset(j)
                        );

                        double err = reconstructed - trueDot;
                        sumSqErr += err * err;
                        sumSqTrue += (double) trueDot * trueDot;
                    }
                }
                double relRmse = Math.sqrt(sumSqErr / sumSqTrue);
                assertThat("bitsPerDim=" + config.bitsPerDim() + " relative RMSE too high", relRmse, lessThan(config.relRmseThreshold()));
            }
        }
    }

    public void testScorerSingleVector() {
        int nDims = 2;
        int bitsPerDim = 2;
        // Valid 2-bit centered codes: values from {-1.5, -0.5, 0.5, 1.5}
        float[] encodedVector = { 0.5f, -0.5f };
        float scale = 1.0f;
        float offset = 0.0f;

        // queryTransformed = [1.0, 0.5] (raw q @ W with zero centroid)
        // dot = 1.0*0.5 + 0.5*(-0.5) = 0.25
        // result = 0.25 * 1.0 + 0.0 + 0.0 = 0.25
        byte[] packed = ESVectorUtil.ashPack(encodedVector, bitsPerDim);
        float score = referenceScore(new float[] { 1.0f, 0.5f }, new float[] { 0.0f }, packed, 0, nDims, bitsPerDim, scale, offset);
        assertEquals(0.25f, score, 1e-4f);
    }

    public void testFallbackToRandomWhenTooFewVectors() throws IOException {
        // With only 2 vectors and nDims=4, learned method should fall back to random
        int dim = 16;
        float[][] vectors = {
            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
            { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 } };
        float[][] centroids = { new float[dim] };
        int[] assignments = { 0, 0 };

        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroids[assignments[i]];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.25f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );

        // Should not throw -- falls back to random
        AsymmetricHashingQuantizer.TrainedProjection trained = quantizer.train(ord -> vectors[ord], vectors.length, dim, centroidGetter);
        assertNotNull(trained.wT());
        int nDims = quantizer.nDims(dim);
        assertEquals(dim * nDims, trained.wT().length);
        // The random fallback must be reported as not-learned so it is not inherited/warm-started at merge.
        assertFalse(trained.learned());
    }

    public void testTrainReportsLearnedFlag() throws IOException {
        int dim = 16;
        int nVectors = 200;
        float[][] vectors = new float[nVectors][dim];
        for (int i = 0; i < nVectors; i++) {
            vectors[i] = VectorTestUtils.randomFloatVector(random(), dim);
        }
        float[] centroid = new float[dim];
        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroid;

        // Method.RANDOM always yields a non-learned matrix.
        AsymmetricHashingQuantizer randomQuantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.RANDOM,
            0,
            10,
            42L,
            executor()
        );
        assertFalse(randomQuantizer.train(ord -> vectors[ord], nVectors, dim, centroidGetter).learned());

        // Method.LEARNED with enough vectors yields a genuinely learned matrix.
        AsymmetricHashingQuantizer learnedQuantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        assertTrue(learnedQuantizer.train(ord -> vectors[ord], nVectors, dim, centroidGetter).learned());

        // Method.LEARNED with too few vectors falls back to random and reports not-learned.
        assertFalse(learnedQuantizer.train(ord -> vectors[ord], 1, dim, centroidGetter).learned());
    }

    public void testMultiBitPackAndScore() {
        // 2-bit quantizer: levels are -1.5, -0.5, 0.5, 1.5
        int bitsPerDim = 2;
        int nDims = 10;
        float[] codes = { 0.5f, -1.5f, 1.5f, -0.5f, 0.5f, 1.5f, -0.5f, -1.5f, 0.5f, 1.5f };
        byte[] packed = ESVectorUtil.ashPack(codes, bitsPerDim);
        assertEquals(bitsPerDim * ((nDims + 7) >>> 3), packed.length);

        float[] qt = { 0.5f, 0.3f, -0.2f, 0.8f, 0.1f, -0.4f, 0.6f, -0.7f, 0.9f, -0.1f };
        float scale = 1.2f;
        float offset = -0.1f;
        float qdc = 0.4f;

        // Compute reference score via plain float dot product
        double dot = ESVectorUtil.dotProduct(qt, codes, nDims);
        float floatScore = (float) dot * scale + qdc + offset;
        float multiBitScore = referenceScore(qt, new float[] { qdc }, packed, 0, nDims, bitsPerDim, scale, offset);
        assertEquals(floatScore, multiBitScore, 1e-4f);
    }

    public void testTrainRecoversPlantedSubspace() throws IOException {
        int dim = 32;
        int rank = 8;
        float[] planted = plantedSubspace(dim, rank);
        float[][] vectors = plantedVectors(dim, rank, 400, planted);
        float[] centroid = new float[dim];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.25f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        var trained = train(quantizer, vectors, i -> centroid);

        int nDims = quantizer.nDims(dim);
        assertEquals(rank, nDims);
        assertTrue(trained.learned());
        assertEquals(dim * nDims, trained.wT().length);
        assertOrthonormalRows(trained.wT(), nDims, dim, 1e-3f);
        for (int i = 0; i < dim; i++) {
            for (int j = 0; j < nDims; j++) {
                assertEquals(trained.wT()[j * dim + i], trained.w()[i * nDims + j], 0f);
            }
        }
        assertThat(capturedEnergy(planted, rank, trained.wT(), nDims, dim), greaterThan(rank * 0.99f));
    }

    public void testTrainWarmStartKeepsPlantedSubspace() throws IOException {
        int dim = 32;
        int rank = 8;
        float[] planted = plantedSubspace(dim, rank);
        float[][] vectors = plantedVectors(dim, rank, 400, planted);
        float[] centroid = new float[dim];
        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroid;

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.25f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        float[] inherited = train(quantizer, vectors, centroidGetter).wT();
        float[] inheritedCopy = inherited.clone();

        var refined = quantizer.trainWarmStart(ord -> vectors[ord], vectors.length, dim, centroidGetter, inherited, 2);

        assertArrayEquals("the inherited matrix is owned by the caller and must not be modified", inheritedCopy, inherited, 0f);
        assertTrue(refined.learned());
        assertOrthonormalRows(refined.wT(), rank, dim, 1e-3f);
        assertThat(capturedEnergy(planted, rank, refined.wT(), rank, dim), greaterThan(rank * 0.99f));
    }

    public void testTrainWithProcrustesIterationsKeepsTheSubspace() throws IOException {
        // The Procrustes iterations only rotate within the PCA subspace, so the subspace is what it is without them
        int dim = 32;
        int rank = 8;
        float[] planted = plantedSubspace(dim, rank);
        float[][] vectors = plantedVectors(dim, rank, 400, planted);
        float[] centroid = new float[dim];

        for (int iterations : new int[] { 1, 3 }) {
            AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
                0.25f,
                Integer.MAX_VALUE,
                2,
                AsymmetricHashingQuantizer.Method.LEARNED,
                iterations,
                10,
                42L,
                executor()
            );
            var trained = train(quantizer, vectors, i -> centroid);

            int nDims = quantizer.nDims(dim);
            assertTrue(trained.learned());
            assertEquals(dim * nDims, trained.wT().length);
            assertOrthonormalRows(trained.wT(), nDims, dim, 1e-3f);
            for (int i = 0; i < dim; i++) {
                for (int j = 0; j < nDims; j++) {
                    assertEquals(trained.wT()[j * dim + i], trained.w()[i * nDims + j], 0f);
                }
            }
            assertThat("iterations=" + iterations, capturedEnergy(planted, rank, trained.wT(), nDims, dim), greaterThan(rank * 0.99f));
        }
    }

    public void testNegativeProcrustesIterationsAreRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new AsymmetricHashingQuantizer(
                0.5f,
                Integer.MAX_VALUE,
                2,
                AsymmetricHashingQuantizer.Method.LEARNED,
                -1,
                10,
                42L,
                executor()
            )
        );
    }

    public void testTrainWithoutDimensionalityReductionIsRotation() throws IOException {
        int dim = 16;
        float[][] vectors = new float[200][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = VectorTestUtils.randomFloatVector(random(), dim);
        }
        float[] centroid = new float[dim];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            1.0f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        var trained = train(quantizer, vectors, i -> centroid);

        assertTrue("a full-dimension rotation is as good as a learned one", trained.learned());
        assertOrthonormalRows(trained.wT(), dim, dim, 1e-4f);
    }

    public void testTrainFromFewerVectorsThanProjectedDimensionsStillLearnsTheData() throws IOException {
        // 20 vectors for 16 projected dimensions: fewer than the 2 * 16 needed to be inherited, but plenty to find the
        // 4-dimensional subspace the vectors live in, which a random projection would only half capture
        int dim = 32;
        int rank = 4;
        float[] planted = plantedSubspace(dim, rank);
        float[][] vectors = plantedVectors(dim, rank, 20, planted);
        float[] centroid = new float[dim];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        var trained = train(quantizer, vectors, i -> centroid);

        int nDims = quantizer.nDims(dim);
        assertEquals(16, nDims);
        assertFalse("too few vectors for the projection to be inherited at merge", trained.learned());
        assertOrthonormalRows(trained.wT(), nDims, dim, 1e-3f);
        assertThat(capturedEnergy(planted, rank, trained.wT(), nDims, dim), greaterThan(rank * 0.95f));
    }

    public void testTrainWithFewerVectorsThanProjectedDimensionsCompletesTheBasis() throws IOException {
        // fewer vectors than projected dimensions: the sample supplies only as many directions as it has vectors
        int dim = 64;
        float[][] vectors = new float[AsymmetricHashingQuantizer.MIN_PCA_VECTORS][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = VectorTestUtils.randomFloatVector(random(), dim);
        }
        float[] centroid = new float[dim];

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        var trained = train(quantizer, vectors, i -> centroid);

        int nDims = quantizer.nDims(dim);
        assertTrue("more projected dimensions than vectors", nDims > vectors.length);
        assertEquals(dim * nDims, trained.wT().length);
        assertOrthonormalRows(trained.wT(), nDims, dim, 1e-3f);
        assertFalse(trained.learned());
    }

    public void testLearnedFlagNeedsTwiceAsManyVectorsAsProjectedDimensions() throws IOException {
        int dim = 32;
        int rank = 4;
        float[] planted = plantedSubspace(dim, rank);
        float[][] vectors = plantedVectors(dim, rank, 40, planted);
        float[] centroid = new float[dim];
        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroid;
        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        int nDims = quantizer.nDims(dim);

        assertFalse(quantizer.train(ord -> vectors[ord], 2 * nDims - 1, dim, centroidGetter).learned());
        assertTrue(quantizer.train(ord -> vectors[ord], 2 * nDims, dim, centroidGetter).learned());
        // below the floor there is nothing to learn from
        var tiny = quantizer.train(ord -> vectors[ord], AsymmetricHashingQuantizer.MIN_PCA_VECTORS - 1, dim, centroidGetter);
        assertFalse(tiny.learned());
        assertOrthonormalRows(tiny.wT(), nDims, dim, 1e-4f);
    }

    public void testOrthonormalizeRowsRepairsRowsInTheSpanOfEarlierOnes() {
        // Axis vectors are exactly representable, so duplicates of earlier rows cancel to exactly zero when projected off.
        // Small blocks put the duplicates in blocks other than the one holding the row they duplicate
        int n = 64;
        int k = 20;
        float[] rows = new float[k * n];
        for (int i = 0; i < k; i++) {
            rows[i * n + (i % 3)] = 1f;
        }
        // and some zero rows
        Arrays.fill(rows, 7 * n, 8 * n, 0f);
        Arrays.fill(rows, 13 * n, 14 * n, 0f);

        AshUtils.orthonormalizeRows(rows, n, k, 4, executor());

        assertOrthonormalRows(rows, k, n, 1e-4f);
        // the first three rows are the first three axes, and keep spanning them
        float[] axes = new float[3 * n];
        for (int i = 0; i < 3; i++) {
            axes[i * n + i] = 1f;
        }
        assertThat(capturedEnergy(axes, 3, Arrays.copyOf(rows, 3 * n), 3, n), greaterThan(3 - 1e-4f));
    }

    public void testTrainWithExactlyRankDeficientDataIsOrthonormal() throws IOException {
        // exactly representable vectors: three distinct axes, and then a single axis repeated. Rounding can give no
        // direction to the dimensions the data does not span
        int dim = 64;
        float[] zero = new float[dim];
        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        int nDims = quantizer.nDims(dim);
        float[][] axes = new float[3][dim];
        for (int i = 0; i < 3; i++) {
            axes[i][i] = 1f;
        }
        var threeAxes = quantizer.train(ord -> axes[ord % 3], 300, dim, ord -> zero);
        assertOrthonormalRows(threeAxes.wT(), nDims, dim, 1e-3f);
        assertThat(capturedEnergy(identityRows(3, dim), 3, threeAxes.wT(), nDims, dim), greaterThan(3 - 1e-3f));

        var oneAxis = quantizer.train(ord -> axes[0], 300, dim, ord -> zero);
        assertOrthonormalRows(oneAxis.wT(), nDims, dim, 1e-3f);
        assertThat(capturedEnergy(identityRows(1, dim), 1, oneAxis.wT(), nDims, dim), greaterThan(1 - 1e-3f));
    }

    public void testWarmStartRotatesTheRefinedBasis() throws IOException {
        // Variance falls geometrically with the coordinate. The multiplications of the warm start keep the subspace but
        // pull the basis toward the leading eigenvectors, which, without a new rotation, leaves the first projected
        // coordinates with hundreds of times the variance of the last. A rotation spreads it
        int dim = 32;
        float[][] vectors = new float[400][dim];
        for (float[] vector : vectors) {
            for (int j = 0; j < dim; j++) {
                vector[j] = (float) (random().nextGaussian() * Math.pow(0.8, j));
            }
        }
        float[] centroid = new float[dim];
        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroid;
        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        int nDims = quantizer.nDims(dim);
        var cold = quantizer.train(ord -> vectors[ord], vectors.length, dim, centroidGetter);

        var warm = quantizer.trainWarmStart(ord -> vectors[ord], vectors.length, dim, centroidGetter, cold.wT(), 2);

        assertTrue(warm.learned());
        assertOrthonormalRows(warm.wT(), nDims, dim, 1e-3f);
        double coldRatio = coordinateVarianceRatio(cold.wT(), nDims, dim, vectors);
        double warmRatio = coordinateVarianceRatio(warm.wT(), nDims, dim, vectors);
        assertThat("cold-trained variance ratio", coldRatio, lessThan(100.0));
        assertThat("warm-started variance ratio (cold was " + coldRatio + ")", warmRatio, lessThan(100.0));
    }

    public void testWarmStartWithoutVarianceReturnsTheInheritedMatrix() throws IOException {
        // every merged vector is its own centroid: there is no second moment to refine the inherited matrix with
        int dim = 32;
        float[] planted = plantedSubspace(dim, 4);
        float[][] vectors = plantedVectors(dim, 4, 100, planted);
        float[] zero = new float[dim];
        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        float[] inherited = train(quantizer, vectors, i -> zero).wT();
        float[] inheritedCopy = inherited.clone();

        var warm = quantizer.trainWarmStart(ord -> zero, 100, dim, ord -> zero, inherited, 2);

        assertArrayEquals(inheritedCopy, warm.wT(), 0f);
        assertTrue(warm.learned());
        for (float v : warm.wT()) {
            assertTrue(Float.isFinite(v));
        }
    }

    public void testProcrustesWithFewerVectorsThanProjectedDimensionsStaysOrthonormal() throws IOException {
        // the Procrustes step needs at least as many vectors as projected dimensions; below that the rotation is random
        int dim = 32;
        float[] zero = new float[dim];
        for (int count : new int[] { 2, 3, 5 }) {
            float[][] vectors = new float[count][];
            for (int i = 0; i < count; i++) {
                vectors[i] = VectorTestUtils.randomFloatVector(random(), dim);
            }
            AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
                0.5f,
                Integer.MAX_VALUE,
                2,
                AsymmetricHashingQuantizer.Method.LEARNED,
                2,
                10,
                42L,
                executor()
            );

            var trained = train(quantizer, vectors, i -> zero);

            assertOrthonormalRows(trained.wT(), quantizer.nDims(dim), dim, 1e-3f);
        }
    }

    public void testProcrustesIterationsApplyWithoutDimensionalityReduction() throws IOException {
        // With no dimensionality reduction W is just a rotation, which without iterations is random. Configured iterations
        // must still refine it, so with the same seed and data they must change the result
        int dim = 16;
        float[][] vectors = new float[200][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = VectorTestUtils.randomFloatVector(random(), dim);
        }
        float[] zero = new float[dim];
        var randomRotation = train(
            new AsymmetricHashingQuantizer(1.0f, Integer.MAX_VALUE, 2, AsymmetricHashingQuantizer.Method.LEARNED, 0, 10, 42L, executor()),
            vectors,
            i -> zero
        );
        var refined = train(
            new AsymmetricHashingQuantizer(1.0f, Integer.MAX_VALUE, 2, AsymmetricHashingQuantizer.Method.LEARNED, 2, 10, 42L, executor()),
            vectors,
            i -> zero
        );

        assertTrue(refined.learned());
        assertOrthonormalRows(refined.wT(), dim, dim, 1e-3f);
        assertFalse("Procrustes iterations were ignored", Arrays.equals(randomRotation.wT(), refined.wT()));
    }

    public void testSecondMomentOfAMultiBlockSampleMatchesAnIndependentSum() throws IOException {
        // 1280 vectors are one full block of 1024 rows and a partial block of 256. The provider hands out one reused
        // buffer, as the merge path does, so a vector must be copied out before the next is requested, and the vectors
        // have different centroids
        int dim = 128;
        int count = 1280;
        float[][] source = new float[count][];
        for (int i = 0; i < count; i++) {
            source[i] = VectorTestUtils.randomFloatVector(random(), dim);
        }
        float[][] centroids = new float[3][];
        for (int c = 0; c < centroids.length; c++) {
            centroids[c] = VectorTestUtils.randomFloatVector(random(), dim);
        }
        float[] sharedBuffer = new float[dim];
        CheckedIntFunction<float[], IOException> sharedBufferVectors = ord -> {
            System.arraycopy(source[ord], 0, sharedBuffer, 0, dim);
            return sharedBuffer;
        };
        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );

        // as many vectors as the sample needs, so all of them are used
        float[] actual = quantizer.buildSecondMoment(sharedBufferVectors, count, ord -> centroids[ord % 3], dim, count);

        double[] expected = new double[dim * dim];
        for (int i = 0; i < count; i++) {
            double[] residual = new double[dim];
            double normSq = 0;
            for (int j = 0; j < dim; j++) {
                residual[j] = (double) source[i][j] - centroids[i % 3][j];
                normSq += residual[j] * residual[j];
            }
            for (int a = 0; a < dim; a++) {
                for (int b = 0; b < dim; b++) {
                    expected[a * dim + b] += residual[a] * residual[b] / normSq;
                }
            }
        }
        double differenceSq = 0;
        double expectedSq = 0;
        for (int i = 0; i < expected.length; i++) {
            double difference = actual[i] - expected[i];
            differenceSq += difference * difference;
            expectedSq += expected[i] * expected[i];
        }
        assertThat("relative error of the second-moment matrix", Math.sqrt(differenceSq / expectedSq), lessThan(1e-4));
    }

    public void testSegmentWithNoMoreVectorsThanProjectedDimensionsIsProjectedWithoutLoss() throws IOException {
        // A flat segment is centered on the mean of its own vectors and encoded with a projection learned from them.
        // Their residuals span at most (count - 1) dimensions, so with that few vectors the projection keeps all of every
        // residual; a random projection would keep about half of each
        int dim = 64;
        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        int nDims = quantizer.nDims(dim);
        for (int count : new int[] { AsymmetricHashingQuantizer.MIN_PCA_VECTORS, 3, 10, nDims - 1, nDims }) {
            float[][] vectors = new float[count][];
            float[] mean = new float[dim];
            for (int i = 0; i < count; i++) {
                vectors[i] = VectorTestUtils.randomFloatVector(random(), dim);
                ESVectorUtil.linearCombination(1f / count, vectors[i], mean);
            }

            var trained = train(quantizer, vectors, i -> mean);

            assertOrthonormalRows(trained.wT(), nDims, dim, 1e-3f);
            for (int i = 0; i < count; i++) {
                float[] residual = vectors[i].clone();
                ESVectorUtil.linearCombination(-1f, mean, residual);
                float[] projected = ESVectorUtil.matrixVectorMultiply(trained.wT(), nDims, dim, residual);
                float kept = ESVectorUtil.dotProduct(projected, projected);
                float total = ESVectorUtil.dotProduct(residual, residual);
                assertEquals("count " + count + ", vector " + i, total, kept, total * 1e-3f);
            }
        }
    }

    public void testTrainWithoutAnyVarianceFallsBackToRandom() throws IOException {
        // every vector is its own centroid, so every residual is zero
        int dim = 32;
        float[][] vectors = new float[100][dim];
        float[] centroid = new float[dim];
        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            0.5f,
            Integer.MAX_VALUE,
            2,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        var trained = train(quantizer, vectors, i -> centroid);

        assertFalse(trained.learned());
        assertOrthonormalRows(trained.wT(), quantizer.nDims(dim), dim, 1e-4f);
        for (float v : trained.wT()) {
            assertTrue(Float.isFinite(v));
        }
    }

    public void testCompleteBasisKeepsTheLeadingSpan() {
        int n = 40;
        int have = 6;
        int want = 20;
        float[] leading = plantedSubspace(n, have);

        float[] full = AshUtils.completeBasis(leading, n, have, want, random(), executor());

        assertEquals(want * n, full.length);
        assertOrthonormalRows(full, want, n, 1e-4f);
        assertThat(capturedEnergy(leading, have, full, want, n), greaterThan(have - 1e-3f));
        // the extra rows lie outside the span of the first ones
        float[] firstRows = Arrays.copyOf(full, have * n);
        for (int i = have; i < want; i++) {
            float[] row = Arrays.copyOfRange(full, i * n, (i + 1) * n);
            float[] coordinates = ESVectorUtil.matrixVectorMultiply(firstRows, have, n, row);
            assertThat(ESVectorUtil.dotProduct(coordinates, coordinates), lessThan(1e-4f));
        }
    }

    public void testScoreReconstructsDotProduct() throws IOException {
        int dim = 128;
        int nVectors = 1000;
        int nQueries = 100;
        int nClusters = 4;
        int bitsPerDim = 2;
        float projectedDimsFraction = 0.5f;
        long seed = 42L;
        // Thresholds chosen so a correct implementation passes comfortably but a
        // broken one (missing offset, wrong sign, double-subtracted cross-term) fails.
        double pearsonThreshold = 0.6;
        double recallThreshold = 0.2;
        int k = 10;

        // Use non-unit vectors with meaningful magnitude to stress the offset formula.
        // Unit vectors make centroids near-zero which can mask offset bugs.
        float[][] vectors = new float[nVectors][];
        for (int i = 0; i < nVectors; i++) {
            vectors[i] = AshUtils.randomGaussians(random(), dim);
        }
        float[][] queries = new float[nQueries][];
        for (int i = 0; i < nQueries; i++) {
            queries[i] = AshUtils.randomGaussians(random(), dim);
        }

        // Non-trivial centroids with significant magnitude (shifted clusters)
        int[] assignments = new int[nVectors];
        float[][] centroids = new float[nClusters][dim];
        int[] counts = new int[nClusters];
        for (int i = 0; i < nVectors; i++) {
            assignments[i] = random().nextInt(nClusters);
            counts[assignments[i]]++;
            for (int d = 0; d < dim; d++) {
                centroids[assignments[i]][d] += vectors[i][d];
            }
        }
        for (int c = 0; c < nClusters; c++) {
            for (int d = 0; d < dim; d++) {
                centroids[c][d] /= Math.max(counts[c], 1);
            }
        }
        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroids[assignments[i]];

        // Train
        AsymmetricHashingQuantizer ash = new AsymmetricHashingQuantizer(
            projectedDimsFraction,
            Integer.MAX_VALUE,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            seed,
            executor()
        );
        var w = train(ash, vectors, centroidGetter);
        int nDims = ash.nDims(dim);

        // Precompute per-cluster values
        // Pre-transform each query: qt = wT @ q
        float[][] qt = new float[nQueries][];
        for (int q = 0; q < nQueries; q++) {
            qt[q] = ESVectorUtil.matrixVectorMultiply(w.wT(), nDims, dim, queries[q]);
        }

        // Score matrices: approx[q][i] = ASH-approximated dot(q, v_i), exact[q][i] = true dot
        double[][] exact = new double[nQueries][nVectors];
        double[][] approx = new double[nQueries][nVectors];
        AsymmetricHashingQuantizer.VectorAndNorm[] precomputedPerCluster = new AsymmetricHashingQuantizer.VectorAndNorm[nClusters];
        for (int c = 0; c < nClusters; c++) {
            precomputedPerCluster[c] = AsymmetricHashingQuantizer.precomputeCentroid(centroids[c], w.wT());
        }

        // Group the vectors by cluster, so that a block of encodes shares a single centroid the way a
        // posting list does, then encode cluster by cluster through the production block path.
        int[][] clusterOrds = new int[nClusters][];
        for (int c = 0; c < nClusters; c++) {
            clusterOrds[c] = new int[counts[c]];
        }
        int[] filled = new int[nClusters];
        for (int i = 0; i < nVectors; i++) {
            clusterOrds[assignments[i]][filled[assignments[i]]++] = i;
        }

        int maxBlockSize = 32;
        try (AsymmetricHashingQuantizer.BlockEncoder encoder = ash.newBlockEncoder(w.w(), dim, maxBlockSize)) {
            for (int c = 0; c < nClusters; c++) {
                int[] cluster = clusterOrds[c];
                for (int start = 0; start < cluster.length; start += maxBlockSize) {
                    int blockSize = Math.min(maxBlockSize, cluster.length - start);
                    encoder.reset();
                    for (int j = 0; j < blockSize; j++) {
                        encoder.add(vectors[cluster[start + j]], centroids[c]);
                    }
                    encoder.encode(precomputedPerCluster[c]);

                    for (int j = 0; j < blockSize; j++) {
                        int i = cluster[start + j];
                        byte[] packed = ESVectorUtil.ashPack(encoder.code(j), bitsPerDim);

                        for (int q = 0; q < nQueries; q++) {
                            double exactDot = ESVectorUtil.dotProduct(queries[q], vectors[i]);
                            double qDotC = ESVectorUtil.dotProduct(queries[q], centroids[c]);

                            float approxScore = referenceScore(
                                qt[q],
                                new float[] { (float) qDotC },
                                packed,
                                0,
                                nDims,
                                bitsPerDim,
                                encoder.scale(j),
                                encoder.offset(j)
                            );

                            exact[q][i] = exactDot;
                            approx[q][i] = approxScore;
                        }
                    }
                }
            }
        }

        // Pearson correlation across all (query, vector) pairs
        double sumE = 0, sumA = 0, sumEE = 0, sumAA = 0, sumEA = 0;
        long n = 0;
        for (int q = 0; q < nQueries; q++) {
            for (int i = 0; i < nVectors; i++) {
                double e = exact[q][i], a = approx[q][i];
                sumE += e;
                sumA += a;
                sumEE += e * e;
                sumAA += a * a;
                sumEA += e * a;
                n++;
            }
        }
        double meanE = sumE / n, meanA = sumA / n;
        double varE = sumEE / n - meanE * meanE;
        double varA = sumAA / n - meanA * meanA;
        double covEA = sumEA / n - meanE * meanA;
        double pearson = covEA / Math.sqrt(varE * varA);
        double recall = recallAtK(approx, exact, k);

        assertThat(pearson, greaterThan(pearsonThreshold));
        assertThat("recall@" + k, recall, greaterThan(recallThreshold));
    }

    /**
     * Covers the block encoder over both a full and a partial block: every gathered vector must come
     * back with a code of the right width holding valid levels, and with the uncentered
     * {@code ⟨x, μ⟩} that the EUCLIDEAN corrections are written from.
     */
    public void testBlockEncoderCoversFullAndPartialBlocks() throws IOException {
        int nVectors = 100;
        int dim = 32;
        float projectedDimsFraction = 0.25f;
        int bitsPerDim = 2;
        int nDims = (int) (dim * projectedDimsFraction);
        // deliberately does not divide nVectors, so the last block of the run is a partial one
        int maxBlockSize = 7;

        float[][] vectors = new float[nVectors][];
        for (int i = 0; i < nVectors; i++) {
            vectors[i] = AshUtils.randomGaussians(random(), dim);
        }
        float[] centroid = AshUtils.randomGaussians(random(), dim);
        CheckedIntFunction<float[], IOException> centroidGetter = i -> centroid;

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            projectedDimsFraction,
            Integer.MAX_VALUE,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.LEARNED,
            0,
            10,
            42L,
            executor()
        );
        var w = train(quantizer, vectors, centroidGetter);
        AsymmetricHashingQuantizer.VectorAndNorm precomputed = AsymmetricHashingQuantizer.precomputeCentroid(centroid, w.wT());

        try (AsymmetricHashingQuantizer.BlockEncoder encoder = quantizer.newBlockEncoder(w.w(), dim, maxBlockSize)) {
            Float[] validLevels = new Float[] { -1.5f, -0.5f, 0.5f, 1.5f };

            for (int start = 0; start < nVectors; start += maxBlockSize) {
                int blockSize = Math.min(maxBlockSize, nVectors - start);
                encoder.reset();
                assertThat(encoder.size(), equalTo(0));
                for (int j = 0; j < blockSize; j++) {
                    encoder.add(vectors[start + j], centroid);
                }
                encoder.encode(precomputed);

                assertThat(encoder.size(), equalTo(blockSize));
                for (int j = 0; j < blockSize; j++) {
                    float[] code = encoder.code(j);
                    assertThat(code.length, equalTo(nDims));
                    for (float level : code) {
                        assertThat(level, oneOf(validLevels));
                    }
                    assertThat(encoder.scale(j), greaterThan(0f));
                    assertTrue(Float.isFinite(encoder.offset(j)));
                    // the dot product is taken before centering, so it must match the original vector
                    assertThat(encoder.vecCentroidDot(j), equalTo(ESVectorUtil.dotProduct(vectors[start + j], centroid)));
                }
            }
        }
    }

    public void testInt8BlockEncoderMatchesFloatBlockEncoder() throws IOException {
        int dim = randomIntBetween(128, 512);
        int bitsPerDim = randomIntBetween(1, 4);
        int nVectors = 100;
        // does not divide nVectors, so the last block is a partial one
        int maxBlockSize = 32;

        AsymmetricHashingQuantizer quantizer = new AsymmetricHashingQuantizer(
            randomFrom(0.5f, 1.0f),
            Integer.MAX_VALUE,
            bitsPerDim,
            AsymmetricHashingQuantizer.Method.RANDOM,
            0,
            1,
            42L,
            executor()
        );
        var w = train(quantizer, new float[][] { new float[dim] }, i -> new float[dim]);
        float[] centroid = AshUtils.randomGaussians(random(), dim);
        var precomputed = AsymmetricHashingQuantizer.precomputeCentroid(centroid, w.wT());

        double cosineSum = 0;
        var floatEncoder = quantizer.newBlockEncoder(w.w(), dim, maxBlockSize, false);
        var int8Encoder = quantizer.newBlockEncoder(w.w(), dim, maxBlockSize, true);
        for (int start = 0; start < nVectors; start += maxBlockSize) {
            int blockSize = Math.min(maxBlockSize, nVectors - start);
            floatEncoder.reset();
            int8Encoder.reset();
            for (int j = 0; j < blockSize; j++) {
                float[] vector = AshUtils.randomGaussians(random(), dim);
                floatEncoder.add(vector, centroid);
                int8Encoder.add(vector, centroid);
            }
            floatEncoder.encode(precomputed);
            int8Encoder.encode(precomputed);

            for (int j = 0; j < blockSize; j++) {
                float[] floatCode = floatEncoder.code(j);
                float[] int8Code = int8Encoder.code(j);
                double cosine = ESVectorUtil.dotProduct(floatCode, int8Code) / Math.sqrt(
                    (double) ESVectorUtil.dotProduct(floatCode, floatCode) * ESVectorUtil.dotProduct(int8Code, int8Code)
                );
                cosineSum += cosine;
                assertThat(int8Encoder.vecCentroidDot(j), equalTo(floatEncoder.vecCentroidDot(j)));
                assertThat(int8Encoder.scale(j), greaterThan(0f));
                assertTrue(Float.isFinite(int8Encoder.offset(j)));
            }
        }
        // a single vector can differ by a few levels, so the check is on the mean
        assertThat(cosineSum / nVectors, greaterThan(0.98));
    }

    /** An orthonormal set of {@code rank} vectors of length {@code dim}, one per row. */
    private float[] plantedSubspace(int dim, int rank) {
        float[] rows = AshUtils.randomGaussians(random(), rank * dim);
        AshUtils.qrOrthogonalize(rows, dim, rank);
        return rows;
    }

    /** Vectors that are strong in the planted subspace plus a little isotropic noise. */
    private float[][] plantedVectors(int dim, int rank, int count, float[] planted) {
        float[][] vectors = new float[count][dim];
        for (float[] vector : vectors) {
            for (int u = 0; u < rank; u++) {
                float coefficient = 3f * (float) random().nextGaussian();
                ESVectorUtil.linearCombination(coefficient, planted, u * dim, vector, 0, dim);
            }
            for (int i = 0; i < dim; i++) {
                vector[i] += 0.05f * (float) random().nextGaussian();
            }
        }
        return vectors;
    }

    /**
     * How much of the {@code rank} orthonormal vectors in {@code planted} lies in the span of the
     * {@code nDims} orthonormal rows of {@code basisT}: {@code rank} when the span contains them all.
     */
    private static float capturedEnergy(float[] planted, int rank, float[] basisT, int nDims, int dim) {
        float captured = 0;
        for (int u = 0; u < rank; u++) {
            float[] vector = Arrays.copyOfRange(planted, u * dim, (u + 1) * dim);
            float[] coordinates = ESVectorUtil.matrixVectorMultiply(basisT, nDims, dim, vector);
            captured += ESVectorUtil.dotProduct(coordinates, coordinates);
        }
        return captured;
    }

    private static void assertOrthonormalRows(float[] rows, int count, int length, float tolerance) {
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < count; j++) {
                float dot = ESVectorUtil.dotProduct(rows, i * length, rows, j * length, length);
                assertEquals("rows " + i + " and " + j, i == j ? 1f : 0f, dot, tolerance);
            }
        }
    }

    /** The first {@code count} axes as rows of length {@code dim}. */
    private static float[] identityRows(int count, int dim) {
        float[] rows = new float[count * dim];
        for (int i = 0; i < count; i++) {
            rows[i * dim + i] = 1f;
        }
        return rows;
    }

    /**
     * The ratio between the largest and smallest variance over the coordinates of the projection {@code wT} of the vectors
     * once normalized to unit length (zero centroid): how unevenly the variance is spread over the projected coordinates.
     */
    private static double coordinateVarianceRatio(float[] wT, int nDims, int dim, float[][] vectors) {
        double[] variance = new double[nDims];
        for (float[] vector : vectors) {
            float[] normalized = vector.clone();
            ESVectorUtil.l2Normalize(normalized, 0, dim);
            float[] projected = ESVectorUtil.matrixVectorMultiply(wT, nDims, dim, normalized);
            for (int j = 0; j < nDims; j++) {
                variance[j] += (double) projected[j] * projected[j];
            }
        }
        double max = Arrays.stream(variance).max().orElseThrow();
        double min = Arrays.stream(variance).min().orElseThrow();
        return max / min;
    }

    private static AsymmetricHashingQuantizer.TrainedProjection train(
        AsymmetricHashingQuantizer quantizer,
        float[][] vectors,
        CheckedIntFunction<float[], IOException> centroids
    ) throws IOException {
        return quantizer.train(ord -> vectors[ord], vectors.length, vectors[0].length, centroids);
    }

    /** Average overlap@k between approx-top-k and exact-top-k, per query. */
    private static double recallAtK(double[][] approx, double[][] exact, int k) {
        int nQueries = approx.length;
        long hits = 0;
        for (int q = 0; q < nQueries; q++) {
            int[] approxTop = topKIndices(approx[q], k);
            int[] exactTop = topKIndices(exact[q], k);
            Set<Integer> e = Arrays.stream(exactTop).boxed().collect(Collectors.toSet());
            hits += Arrays.stream(approxTop).filter(e::contains).count();
        }
        return (double) hits / ((long) nQueries * k);
    }

    /** Returns indices of the k largest values in scores, unordered. */
    private static int[] topKIndices(double[] scores, int k) {
        return IntStream.range(0, scores.length)
            .boxed()
            .sorted(Comparator.comparingDouble(i -> scores[i]))
            .limit(k)
            .mapToInt(Integer::intValue)
            .toArray();
    }

    /**
     * Reference scorer for test verification: computes the ASH approximate dot product
     * from packed bit-plane codes and corrections.
     */
    private static float referenceScore(
        float[] queryTransformed,
        float[] queryConstants,
        byte[] packedCodes,
        int codeOffset,
        int nDims,
        int bitsPerDim,
        float scale,
        float offset
    ) {
        int planeBytes = (nDims + 7) >>> 3;
        int numLevels = 1 << bitsPerDim;
        float centerOffset = (numLevels - 1) / 2.0f;
        float sum = ESVectorUtil.sum(queryTransformed, nDims);

        float dot = -centerOffset * sum;
        for (int p = 0; p < bitsPerDim; p++) {
            float planeSum = ESVectorUtil.ipFloatBit(queryTransformed, 0, packedCodes, codeOffset + p * planeBytes, nDims);
            dot = Math.fma(1 << p, planeSum, dot);
        }

        return Math.fma(dot, scale, queryConstants[0] + offset);
    }
}
