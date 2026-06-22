/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.lucene.util.VectorUtil.l2normalize;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ManifoldModelTests extends ESTestCase {

    public void testIthDistanceIsNonDecreasingWithRank() throws IOException {
        float[] query = { 1f, 0f, 0f };
        float[][] corpus = {
            { 0.9f, 0.1f, 0f },
            { 0.8f, 0.2f, 0f },
            { 0.7f, 0.3f, 0f },
            { 0.6f, 0.4f, 0f },
            { 0.5f, 0.5f, 0f },
            { 0.4f, 0.6f, 0f },
            { 0.3f, 0.7f, 0f },
            { 0.2f, 0.8f, 0f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, 3);
        int[] ordinals = { 0, 1, 2, 3, 4, 5, 6, 7 };

        ManifoldModel.ManifoldTopK topK = new ManifoldModel.ManifoldTopK(VectorSimilarityFunction.EUCLIDEAN, 6);
        topK.add(query, fvv, ordinals, 0, corpus.length);

        float d1 = topK.ithDistance(1);
        float d3 = topK.ithDistance(3);
        float d5 = topK.ithDistance(5);
        assertThat(d1, greaterThan(0f));
        assertThat(d3, greaterThan(d1));
        assertThat(d5, greaterThan(d3));
    }

    public void testIthDistanceMatchesExactRankForKnownCorpus() throws IOException {
        float[] query = { 1f, 0f };
        float[][] corpus = { { 1f, 0f }, { 0.9f, 0.1f }, { 0.8f, 0.2f }, { 0.7f, 0.3f }, { 0.6f, 0.4f }, { 0.5f, 0.5f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, 2);
        int[] ordinals = { 0, 1, 2, 3, 4, 5 };

        float[] expected = new float[corpus.length];
        for (int i = 0; i < corpus.length; i++) {
            expected[i] = ESVectorUtil.squareDistance(query, corpus[i]);
        }
        Arrays.sort(expected);

        ManifoldModel.ManifoldTopK topK = new ManifoldModel.ManifoldTopK(VectorSimilarityFunction.EUCLIDEAN, 6);
        topK.add(query, fvv, ordinals, 0, corpus.length);

        for (int rank = 1; rank <= expected.length; rank++) {
            assertEquals(expected[rank - 1], topK.ithDistance(rank), 1e-5f);
        }
    }

    public void testIthDistanceMatchesExactRankForDotProduct() throws IOException {
        float[] query = { 1f, 0f };
        float[][] corpus = { { 1f, 0f }, { 0.9f, 0.1f }, { 0.8f, 0.2f }, { 0.7f, 0.3f }, { 0.6f, 0.4f }, { 0.5f, 0.5f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, 2);
        int[] ordinals = { 0, 1, 2, 3, 4, 5 };

        float[] expected = new float[corpus.length];
        for (int i = 0; i < corpus.length; i++) {
            expected[i] = ESVectorUtil.dotProduct(query, corpus[i]);
        }
        Arrays.sort(expected);

        ManifoldModel.ManifoldTopK topK = new ManifoldModel.ManifoldTopK(VectorSimilarityFunction.DOT_PRODUCT, 6);
        topK.add(query, fvv, ordinals, 0, corpus.length);

        for (int rank = 1; rank <= expected.length; rank++) {
            assertEquals(expected[expected.length - rank], topK.ithDistance(rank), 1e-5f);
        }
    }

    public void testEstimateManifoldParametersReturnsFiniteCoefficients() throws IOException {
        float[][] rows = syntheticClusteredRows(512, 8, 8);
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(rows), null, 8);
        int[] corpusOrdinals = new int[256];
        for (int i = 0; i < corpusOrdinals.length; i++) {
            corpusOrdinals[i] = 32 + i;
        }
        int[] queryOrdinals = new int[32];
        for (int i = 0; i < queryOrdinals.length; i++) {
            queryOrdinals[i] = i;
        }
        CalibrationQueries queries = new CalibrationQueries(fvv, queryOrdinals, 8, false, false, null, 8);
        double[] params = ManifoldModel.estimateManifoldParameters(VectorSimilarityFunction.EUCLIDEAN, 8, queries, fvv, corpusOrdinals, 10);
        assertTrue(Double.isFinite(params[0]));
        assertTrue(Double.isFinite(params[1]));
    }

    /**
     * Corpus points on a sorted ray from the origin produce a clean power-law relationship
     * between rank, corpus size, and squared Euclidean distance. Verifies the OLS fit quality,
     * agreement with an independent re-measurement of the sweep, and out-of-sample predictions.
     */
    public void testEstimateManifoldParametersFitsColinearCorpus() throws IOException {
        int dim = 16;
        int calibrationK = 10;
        int corpusSize = 16_384;
        int numQueries = 64;
        ColinearFixture fixture = newColinearFixture(dim, corpusSize, numQueries, new Random(17));

        CalibrationQueries queries = new CalibrationQueries(fixture.fvv(), fixture.queryOrdinals(), dim, false, false, null, dim);
        double[] params = ManifoldModel.estimateManifoldParameters(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            queries,
            fixture.fvv(),
            fixture.corpusOrdinals(),
            calibrationK
        );
        double logAlpha = params[0];
        double invDim = params[1];

        assertThat(invDim, greaterThan(0.0));

        SweepDistances sweep = collectSweepDistances(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            queries,
            fixture.fvv(),
            fixture.corpusOrdinals(),
            calibrationK,
            ranksForCalibrationK(calibrationK)
        );
        assertThat(sweep.count(), greaterThan(20));

        double[] x = Arrays.copyOf(sweep.x(), sweep.count());
        double[] y = Arrays.copyOf(sweep.logY(), sweep.count());
        Regression.OLSResult independentFit = Regression.fitOls(x, y);
        assertThat(logAlpha, closeTo(independentFit.beta0(), 1e-6));
        assertThat(invDim, closeTo(independentFit.beta1(), 1e-6));
        assertThat(Regression.rSquared(x, y, independentFit), greaterThan(0.95));

        double meanAbsLogResidual = 0;
        int[] ranks = ranksForCalibrationK(calibrationK);
        for (int i = 0; i < sweep.count(); i++) {
            double predictedLog = logAlpha + invDim * x[i];
            meanAbsLogResidual += Math.abs(predictedLog - y[i]);
            double predictedDist = ManifoldModel.expectedRankDistance(
                VectorSimilarityFunction.EUCLIDEAN,
                logAlpha,
                invDim,
                MANIFOLD_SAMPLE_SIZES[i],
                ranks[i]
            );
            assertThat(predictedDist, closeTo(Math.exp(predictedLog), 1e-12));
        }
        meanAbsLogResidual /= sweep.count();
        assertThat(meanAbsLogResidual, lessThan(0.16));
    }

    /**
     * {@link CalibrationQueries} with cosine normalization must use a query buffer sized to
     * {@link CalibrationQueries#dimension()}, not the raw embedding dimension alone.
     */
    public void testEstimateManifoldParametersWithCosineCalibrationQueries() throws IOException {
        int dim = 8;
        int calibrationK = 10;
        int corpusSize = 8192;
        int numQueries = 32;
        ColinearFixture fixture = newColinearFixture(dim, corpusSize, numQueries, new Random(19), false);

        CalibrationQueries queries = new CalibrationQueries(fixture.fvv(), fixture.queryOrdinals(), dim, true, false, null, dim);
        double[] params = ManifoldModel.estimateManifoldParameters(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            queries,
            fixture.fvv(),
            fixture.corpusOrdinals(),
            calibrationK
        );
        assertTrue(Double.isFinite(params[0]));
        assertTrue(Double.isFinite(params[1]));
        assertThat(params[1], greaterThan(0.0));
    }

    /**
     * Neyshabur lift adds one working dimension; corpus vectors must be stored with the same
     * lifted dimension so query-corpus distances are well defined.
     */
    public void testEstimateManifoldParametersWithNeyshaburCalibrationQueries() throws IOException {
        int dim = 8;
        int calibrationK = 10;
        int corpusSize = 8192;
        int numQueries = 32;
        int liftedDim = dim + 1;
        ColinearFixture fixture = newColinearLiftedFixture(dim, corpusSize, numQueries, new Random(23));

        CalibrationQueries queries = new CalibrationQueries(
            fixture.fvv(),
            fixture.queryOrdinals(),
            dim,
            false,
            true,
            null,
            liftedDim
        );
        double[] params = ManifoldModel.estimateManifoldParameters(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            queries,
            fixture.fvv(),
            fixture.corpusOrdinals(),
            calibrationK
        );
        assertTrue(Double.isFinite(params[0]));
        assertTrue(Double.isFinite(params[1]));
        assertThat(params[1], greaterThan(0.0));
    }

    public void testExpectedRankDistanceIncreasesWithRankForEuclidean() {
        double alpha = -1.0;
        double invDim = 0.4;
        int n = 10_000;
        double d1 = ManifoldModel.expectedRankDistance(VectorSimilarityFunction.EUCLIDEAN, alpha, invDim, n, 1);
        double d100 = ManifoldModel.expectedRankDistance(VectorSimilarityFunction.EUCLIDEAN, alpha, invDim, n, 100);
        assertThat(d100, greaterThan(d1));
    }

    public void testExpectedRankDistanceDecreasesWithRankForDotProduct() {
        double alpha = -1.0;
        double invDim = 0.4;
        int n = 10_000;
        double d1 = ManifoldModel.expectedRankDistance(VectorSimilarityFunction.DOT_PRODUCT, alpha, invDim, n, 1);
        double d100 = ManifoldModel.expectedRankDistance(VectorSimilarityFunction.DOT_PRODUCT, alpha, invDim, n, 100);
        assertThat(d100, lessThan(d1));
    }

    private static float[][] syntheticClusteredRows(int count, int dim, int numClusters) {
        float[][] centroids = new float[numClusters][dim];
        for (int c = 0; c < numClusters; c++) {
            for (int d = 0; d < dim; d++) {
                centroids[c][d] = (c + 1) * 0.1f + d * 0.01f;
            }
            l2normalize(centroids[c]);
        }
        float[][] rows = new float[count][dim];
        for (int i = 0; i < count; i++) {
            System.arraycopy(centroids[i % numClusters], 0, rows[i], 0, dim);
            rows[i][i % dim] += 0.001f * (i % 5);
            l2normalize(rows[i]);
        }
        return rows;
    }

    /**
     * Must stay aligned with {@link ManifoldModel}'s internal rank multipliers.
     */
    private static int[] ranksForCalibrationK(int k) {
        int[] multipliers = {
            29,
            28,
            27,
            26,
            25,
            24,
            23,
            22,
            21,
            20,
            19,
            18,
            17,
            16,
            15,
            14,
            13,
            12,
            11,
            10,
            9,
            8,
            7,
            6,
            5 };
        int[] ranks = new int[multipliers.length];
        for (int i = 0; i < multipliers.length; i++) {
            ranks[i] = Math.max(1, (multipliers[i] * k) / 5);
        }
        return ranks;
    }

    /**
     * Must stay aligned with {@link ManifoldModel}'s internal sweep sizes.
     */
    private static final int[] MANIFOLD_SAMPLE_SIZES = {
        4096,
        4608,
        5120,
        5632,
        6144,
        6656,
        7168,
        7680,
        8192,
        8704,
        9216,
        9728,
        10240,
        10752,
        11264,
        11776,
        12288,
        12800,
        13312,
        13824,
        14336,
        14848,
        15360,
        15872,
        16384 };

    private record ColinearFixture(FloatVectorValues fvv, int[] corpusOrdinals, int[] queryOrdinals) {}

    private static ColinearFixture newColinearFixture(int dim, int corpusSize, int numQueries, Random random) {
        return newColinearFixture(dim, corpusSize, numQueries, random, true);
    }

    private static ColinearFixture newColinearFixture(
        int dim,
        int corpusSize,
        int numQueries,
        Random random,
        boolean queriesAtOrigin
    ) {
        float[] direction = new float[dim];
        for (int d = 0; d < dim; d++) {
            direction[d] = random.nextFloat();
        }
        l2normalize(direction);

        List<float[]> rows = new ArrayList<>(corpusSize + numQueries);
        // Corpus on a ray with scalars increasing in ordinal order so cumulative sweep prefixes
        // contain the closest points first and rank-k distance scales as k/N.
        for (int i = 0; i < corpusSize; i++) {
            rows.add(scale(direction, (i + 1f) / corpusSize));
        }
        int[] queryOrdinals = new int[numQueries];
        for (int i = 0; i < numQueries; i++) {
            queryOrdinals[i] = corpusSize + i;
            if (queriesAtOrigin) {
                rows.add(new float[dim]);
            } else {
                rows.add(scale(direction, 0.01f + 0.02f * i));
            }
        }
        int[] corpusOrdinals = new int[corpusSize];
        for (int i = 0; i < corpusSize; i++) {
            corpusOrdinals[i] = i;
        }
        return new ColinearFixture(KMeansFloatVectorValues.build(rows, null, dim), corpusOrdinals, queryOrdinals);
    }

    private static ColinearFixture newColinearLiftedFixture(int dim, int corpusSize, int numQueries, Random random) {
        int liftedDim = dim + 1;
        float[] direction = new float[dim];
        for (int d = 0; d < dim; d++) {
            direction[d] = random.nextFloat();
        }
        l2normalize(direction);

        List<float[]> rows = new ArrayList<>(corpusSize + numQueries);
        for (int i = 0; i < corpusSize; i++) {
            rows.add(withLiftDimension(scale(direction, (i + 1f) / corpusSize), liftedDim));
        }
        int[] queryOrdinals = new int[numQueries];
        for (int i = 0; i < numQueries; i++) {
            queryOrdinals[i] = corpusSize + i;
            rows.add(withLiftDimension(scale(direction, 0.01f + 0.02f * i), liftedDim));
        }
        int[] corpusOrdinals = new int[corpusSize];
        for (int i = 0; i < corpusSize; i++) {
            corpusOrdinals[i] = i;
        }
        return new ColinearFixture(KMeansFloatVectorValues.build(rows, null, liftedDim), corpusOrdinals, queryOrdinals);
    }

    private static float[] withLiftDimension(float[] vector, int liftedDim) {
        float[] lifted = new float[liftedDim];
        System.arraycopy(vector, 0, lifted, 0, vector.length);
        return lifted;
    }

    private static float[] scale(float[] direction, float scalar) {
        float[] vector = new float[direction.length];
        for (int d = 0; d < direction.length; d++) {
            vector[d] = scalar * direction[d];
        }
        return vector;
    }

    private record SweepDistances(double[] x, double[] logY, int count) {}

    private static SweepDistances collectSweepDistances(
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int calibrationK,
        int[] ranksForK
    ) throws IOException {
        int m = Math.min(ranksForK.length, MANIFOLD_SAMPLE_SIZES.length);
        double[] x = new double[m];
        double[] logY = new double[m];
        int count = 0;
        int sampleStart = 0;
        float[] queryScratch = new float[queries.dimension()];
        ManifoldModel.ManifoldTopK[] topKs = new ManifoldModel.ManifoldTopK[queries.size()];
        for (int qi = 0; qi < queries.size(); qi++) {
            topKs[qi] = new ManifoldModel.ManifoldTopK(similarityFunction, 6 * calibrationK);
        }
        for (int i = 0; i < m; i++) {
            int sampleEnd = MANIFOLD_SAMPLE_SIZES[i];
            if (sampleEnd > corpusOrdinals.length) {
                break;
            }
            double sum = 0;
            for (int qi = 0; qi < queries.size(); qi++) {
                queries.copyQuery(qi, false, queryScratch);
                topKs[qi].add(queryScratch, fvv, corpusOrdinals, sampleStart, sampleEnd);
                sum += topKs[qi].ithDistance(ranksForK[i]);
            }
            x[count] = Math.log(ranksForK[i]) - Math.log(sampleEnd);
            logY[count] = Math.log(sum / queries.size());
            count++;
            sampleStart = sampleEnd;
        }
        return new SweepDistances(x, logY, count);
    }
}
