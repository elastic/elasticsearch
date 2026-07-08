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
        addTopKCorpusSlice(topK, query, fvv, ordinals, 0, corpus.length, false);

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
        addTopKCorpusSlice(topK, query, fvv, ordinals, 0, corpus.length, false);

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
        addTopKCorpusSlice(topK, query, fvv, ordinals, 0, corpus.length, true);

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
        CalibrationSource source = new CalibrationSource(
            VectorSimilarityFunction.EUCLIDEAN,
            8,
            fvv,
            queryOrdinals,
            8,
            false,
            false,
            null,
            corpusOrdinals,
            10
        );
        double[] params = ManifoldModel.estimateManifoldParameters(source);
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

        CalibrationSource source = new CalibrationSource(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            fixture.fvv(),
            fixture.queryOrdinals(),
            dim,
            false,
            false,
            null,
            fixture.corpusOrdinals(),
            calibrationK
        );
        double[] params = ManifoldModel.estimateManifoldParameters(source);
        double logAlpha = params[0];
        double invDim = params[1];

        assertThat(invDim, greaterThan(0.0));

        SweepDistances sweep = collectSweepDistances(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            fixture.fvv(),
            fixture.queryOrdinals(),
            dim,
            false,
            false,
            fixture.fvv(),
            fixture.corpusOrdinals(),
            calibrationK,
            ManifoldModel.ranksFromMultipliers(calibrationK)
        );
        assertThat(sweep.count(), greaterThan(20));

        double[] x = Arrays.copyOf(sweep.x(), sweep.count());
        double[] y = Arrays.copyOf(sweep.logY(), sweep.count());
        Regression.OLSResult independentFit = Regression.fitOls(x, y);
        assertThat(logAlpha, closeTo(independentFit.beta0(), 1e-6));
        assertThat(invDim, closeTo(independentFit.beta1(), 1e-6));
        assertThat(Regression.rSquared(x, y, independentFit), greaterThan(0.95));

        double meanAbsLogResidual = 0;
        int[] ranks = ManifoldModel.ranksFromMultipliers(calibrationK);
        for (int i = 0; i < sweep.count(); i++) {
            double predictedLog = logAlpha + invDim * x[i];
            meanAbsLogResidual += Math.abs(predictedLog - y[i]);
            double predictedDist = ManifoldModel.expectedRankDistance(
                VectorSimilarityFunction.EUCLIDEAN,
                logAlpha,
                invDim,
                ManifoldModel.SAMPLE_SIZES[i],
                ranks[i]
            );
            assertThat(predictedDist, closeTo(Math.exp(predictedLog), 1e-12));
        }
        meanAbsLogResidual /= sweep.count();
        assertThat(meanAbsLogResidual, lessThan(0.16));
    }

    /**
     * Cosine-normalized calibration queries must use a buffer sized via
     * {@link CalibrationUtils#calibrationQueryDimension(int, boolean)}, not the raw embedding dimension alone.
     */
    public void testEstimateManifoldParametersWithCosineCalibrationQueries() throws IOException {
        int dim = 8;
        int calibrationK = 10;
        int corpusSize = 8192;
        int numQueries = 32;
        ColinearFixture fixture = newColinearFixture(dim, corpusSize, numQueries, new Random(19), false);

        CalibrationSource cosineSource = new CalibrationSource(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            fixture.fvv(),
            fixture.queryOrdinals(),
            dim,
            true,
            false,
            null,
            fixture.corpusOrdinals(),
            calibrationK
        );
        double[] params = ManifoldModel.estimateManifoldParameters(cosineSource);
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

        CalibrationSource neyshaburSource = new CalibrationSource(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            fixture.fvv(),
            fixture.queryOrdinals(),
            dim,
            false,
            true,
            null,
            fixture.corpusOrdinals(),
            calibrationK
        );
        double[] params = ManifoldModel.estimateManifoldParameters(neyshaburSource);
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

    private record ColinearFixture(FloatVectorValues fvv, int[] corpusOrdinals, int[] queryOrdinals) {}

    private static ColinearFixture newColinearFixture(int dim, int corpusSize, int numQueries, Random random) {
        return newColinearFixture(dim, corpusSize, numQueries, random, true);
    }

    private static ColinearFixture newColinearFixture(int dim, int corpusSize, int numQueries, Random random, boolean queriesAtOrigin) {
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

    /**
     * Feeds corpus slice {@code [startDoc, endDoc)} to a single query's heap, replicating the per-query scan that
     * {@link ManifoldModel.ManifoldTopK} used to do internally (now inlined in
     * {@link ManifoldModel#estimateManifoldParameters}). Uses the same sign convention: negated dot for dot-like
     * metrics so the heap tracks the largest similarities.
     */
    private static void addTopKCorpusSlice(
        ManifoldModel.ManifoldTopK topK,
        float[] query,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int startDoc,
        int endDoc,
        boolean dotLike
    ) throws IOException {
        for (int d = startDoc; d < endDoc; d++) {
            float[] doc = fvv.vectorValue(corpusOrdinals[d]);
            float dist = dotLike ? -ESVectorUtil.dotProduct(query, doc) : ESVectorUtil.squareDistance(query, doc);
            topK.considerCandidate(dist);
        }
    }

    private record SweepDistances(double[] x, double[] logY, int count) {}

    private static SweepDistances collectSweepDistances(
        VectorSimilarityFunction similarityFunction,
        int dim,
        FloatVectorValues querySource,
        int[] queryOrdinals,
        int baseDim,
        boolean cosine,
        boolean neyshabur,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int calibrationK,
        int[] ranksForK
    ) throws IOException {
        int m = Math.min(ranksForK.length, ManifoldModel.SAMPLE_SIZES.length);
        double[] x = new double[m];
        double[] logY = new double[m];
        int count = 0;
        int sampleStart = 0;
        int dimWork = CalibrationUtils.calibrationQueryDimension(baseDim, neyshabur);
        float[] queryScratch = new float[dimWork];
        ManifoldModel.ManifoldTopK[] topKs = new ManifoldModel.ManifoldTopK[queryOrdinals.length];
        for (int qi = 0; qi < queryOrdinals.length; qi++) {
            topKs[qi] = new ManifoldModel.ManifoldTopK(similarityFunction, 6 * calibrationK);
        }
        for (int i = 0; i < m; i++) {
            int sampleEnd = ManifoldModel.SAMPLE_SIZES[i];
            if (sampleEnd > corpusOrdinals.length) {
                break;
            }
            double sum = 0;
            for (int qi = 0; qi < queryOrdinals.length; qi++) {
                CalibrationUtils.materializeCalibrationQuery(
                    querySource,
                    queryOrdinals[qi],
                    baseDim,
                    dimWork,
                    cosine,
                    neyshabur,
                    null,
                    false,
                    queryScratch,
                    null
                );
                addTopKCorpusSlice(
                    topKs[qi],
                    queryScratch,
                    fvv,
                    corpusOrdinals,
                    sampleStart,
                    sampleEnd,
                    ManifoldModel.isDotLike(similarityFunction)
                );
                sum += topKs[qi].ithDistance(ranksForK[i]);
            }
            x[count] = Math.log(ranksForK[i]) - Math.log(sampleEnd);
            logY[count] = Math.log(sum / queryOrdinals.length);
            count++;
            sampleStart = sampleEnd;
        }
        return new SweepDistances(x, logY, count);
    }
}
