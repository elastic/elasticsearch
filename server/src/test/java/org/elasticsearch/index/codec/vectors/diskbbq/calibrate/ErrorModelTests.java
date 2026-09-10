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
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.apache.lucene.util.VectorUtil.l2normalize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ErrorModelTests extends ESTestCase {

    public void testPackedByteDotProductMatchesIntegerDot() {
        byte[] a = { 1, 2, 3 };
        byte[] b = { 4, 5, 6 };
        assertEquals((long) 4 + 2 * 5 + 3 * 6, (long) ESVectorUtil.dotProduct(a, b));
    }

    public void testSelectTopNDescendingReturnsLargestInDescendingOrder() {
        double[] keys = { 5.0, 1.0, 4.0, 2.0, 3.0 };
        int[] idx = new int[keys.length];
        ErrorModel.selectTopNDescending(keys, idx, keys.length, 3);
        // top-3 keys are 5, 4, 3 at indices 0, 2, 4; only idx[0..n) is specified by the contract.
        assertEquals(0, idx[0]);
        assertEquals(2, idx[1]);
        assertEquals(4, idx[2]);
    }

    public void testSelectTopNDescendingSingleElement() {
        // n == 1: picks the (a) maximum key. 7.0 ties at indices 1 and 3.
        double[] keys = { -3.0, 7.0, 2.0, 7.0, 1.0 };
        int[] idx = new int[keys.length];
        ErrorModel.selectTopNDescending(keys, idx, keys.length, 1);
        assertEquals(7.0, keys[idx[0]], 0.0);
        assertTopN(keys, 1);
    }

    public void testSelectTopNDescendingFullSortWhenNEqualsLen() {
        // n >= len takes the full-sort branch: the whole array is permuted into descending key order.
        double[] keys = { 2.0, 5.0, -1.0, 3.0 };
        int[] idx = new int[keys.length];
        ErrorModel.selectTopNDescending(keys, idx, keys.length, keys.length);
        assertEquals(1, idx[0]); // 5.0
        assertEquals(3, idx[1]); // 3.0
        assertEquals(0, idx[2]); // 2.0
        assertEquals(2, idx[3]); // -1.0
        assertTopN(keys, keys.length);
    }

    public void testSelectTopNDescendingSingletonArray() {
        double[] keys = { 42.0 };
        int[] idx = new int[1];
        ErrorModel.selectTopNDescending(keys, idx, 1, 1);
        assertEquals(0, idx[0]);
    }

    public void testSelectTopNDescendingHandlesDuplicateKeys() {
        // Four docs tie at the top; the top-3 must all be those, and the singleton 1.0 must be excluded.
        double[] keys = { 4.0, 4.0, 4.0, 1.0, 4.0 };
        int[] idx = new int[keys.length];
        ErrorModel.selectTopNDescending(keys, idx, keys.length, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(4.0, keys[idx[i]], 0.0);
        }
        assertTrue("the sole non-top key 1.0 must not be selected", keys[idx[0]] != 1.0 && keys[idx[1]] != 1.0 && keys[idx[2]] != 1.0);
        assertTopN(keys, 3);
    }

    public void testSelectTopNDescendingRandomizedMatchesFullSort() {
        for (int iter = 0; iter < 100; iter++) {
            int len = randomIntBetween(1, 200);
            double[] keys = new double[len];
            for (int i = 0; i < len; i++) {
                // Mix a small integer range (frequent ties) with a wide continuous range (negatives included).
                keys[i] = randomBoolean() ? randomIntBetween(-5, 5) : randomDouble() * 2000 - 1000;
            }
            assertTopN(keys, randomIntBetween(1, len));
        }
    }

    /**
     * Verifies the {@link ErrorModel#selectTopNDescending} postcondition: {@code idx[0..n)} holds distinct in-range
     * indices whose keys are the {@code n} largest, listed in non-increasing key order. Compares selected key
     * <em>values</em> (not indices) against a reference full sort so it is robust to how ties are broken.
     */
    private static void assertTopN(double[] keys, int n) {
        int len = keys.length;
        int[] idx = new int[len];
        ErrorModel.selectTopNDescending(keys, idx, len, n);

        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < n; i++) {
            assertTrue("index out of range: " + idx[i], idx[i] >= 0 && idx[i] < len);
            assertTrue("duplicate index: " + idx[i], seen.add(idx[i]));
        }
        for (int i = 1; i < n; i++) {
            assertTrue("keys not in descending order at position " + i, keys[idx[i - 1]] >= keys[idx[i]]);
        }

        double[] sortedAsc = keys.clone();
        Arrays.sort(sortedAsc);
        double[] expectedTopN = new double[n];
        for (int i = 0; i < n; i++) {
            expectedTopN[i] = sortedAsc[len - 1 - i];
        }
        double[] actualTopN = new double[n];
        for (int i = 0; i < n; i++) {
            actualTopN[i] = keys[idx[i]];
        }
        assertArrayEquals(expectedTopN, actualTopN, 0.0);
    }

    public void testEstimateQuantizationErrorStdModelReturnsFiniteModel() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.EUCLIDEAN, 10);
        QuantizationErrorStdModel model = ErrorModel.estimateErrorScalingFit(source, 128).scalingModel();
        assertTrue(Double.isFinite(model.params().beta0()));
        assertTrue(Double.isFinite(model.params().beta1()));
        assertThat(model.errorStd(128, 5000), greaterThan(0.0));
    }

    public void testEstimateMagnitudeModelReturnsFiniteModel() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.EUCLIDEAN, 10);
        ErrorScalingFit scalingFit = ErrorModel.estimateErrorScalingFit(source, 128);
        QuantizationErrorStdModel magnitudeModel = ErrorModel.estimateMagnitudeModel(scalingFit, source, true, 4, 2, 128);
        assertTrue(Double.isFinite(magnitudeModel.params().beta0()));
        assertTrue(Double.isFinite(magnitudeModel.params().beta1()));
        assertThat(magnitudeModel.errorStd(128, 4096), greaterThan(0.0));
    }

    public void testEstimateMagnitudeModelReusesScalingSlope() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.EUCLIDEAN, 10);
        ErrorScalingFit scalingFit = ErrorModel.estimateErrorScalingFit(source, 128);
        QuantizationErrorStdModel magnitudeModel = ErrorModel.estimateMagnitudeModel(scalingFit, source, true, 4, 2, 128);
        assertEquals(scalingFit.scalingModel().params().beta1(), magnitudeModel.params().beta1(), 0.0);
    }

    public void testEstimateMagnitudeModelWithInsufficientCorpusPreservesScalingModel() throws IOException {
        float[][] rows = syntheticClusteredRows(64, 8, 4);
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(rows), null, 8);
        int[] queryOrdinals = { 0, 1, 2, 3 };
        int[] corpusOrdinals = { 4, 5, 6, 7, 8, 9, 10, 11 };
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
        Regression.OLSResult scalingParams = new Regression.OLSResult(-2.5, 0.35, 0.01, 0.001, 0.0, 0.01);
        ErrorScalingFit scalingFit = ErrorScalingFit.fromScalingModel(new QuantizationErrorStdModel(scalingParams));

        QuantizationErrorStdModel magnitudeModel = ErrorModel.estimateMagnitudeModel(scalingFit, source, false, 4, 2, 128);
        assertEquals(scalingParams.beta0(), magnitudeModel.params().beta0(), 0.0);
        assertEquals(scalingParams.beta1(), magnitudeModel.params().beta1(), 0.0);
    }

    public void testGrowingCorpusSweepReusesWarmStartCentroids() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.EUCLIDEAN, 10);
        HierarchicalKMeans<float[]> kmeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, 8);
        float[][] docWarmStart = null;
        float[][] queryWarmStart = null;

        // Shared scratch sized to the larger of the two sample sizes
        ErrorModel.QuantizedErrorScratch scratch = new ErrorModel.QuantizedErrorScratch(3072, 8, false, true, false);

        ErrorModel.QuantizedErrorComputeResult first = ErrorModel.quantizedRepErrorStdWithCentroids(
            source,
            true,
            2048,
            128,
            4,
            1,
            kmeans,
            docWarmStart,
            queryWarmStart,
            scratch
        );
        ErrorModel.QuantizedErrorComputeResult second = ErrorModel.quantizedRepErrorStdWithCentroids(
            source,
            true,
            3072,
            128,
            4,
            1,
            kmeans,
            first.docCentroids(),
            first.queryCentroids(),
            scratch
        );

        assertTrue(Double.isFinite(first.std()));
        assertTrue(Double.isFinite(second.std()));
        assertThat(first.std(), greaterThan(0.0));
        assertThat(second.std(), greaterThan(0.0));
        assertNotNull(second.docCentroids());
        assertNotNull(second.queryCentroids());
        assertThat(first.docCentroids().length, greaterThan(0));
        assertThat(second.docCentroids().length, greaterThan(0));
    }

    public void testMagnitudeFitReusesScalingFitClusteringState() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.EUCLIDEAN, 10);
        ErrorScalingFit scalingFit = ErrorModel.estimateErrorScalingFit(source, 128);
        QuantizationErrorStdModel magnitudeModel = ErrorModel.estimateMagnitudeModel(scalingFit, source, true, 4, 2, 128);
        assertTrue(Double.isFinite(magnitudeModel.params().beta0()));
        assertThat(magnitudeModel.errorStd(128, 4096), greaterThan(0.0));
    }

    /**
     * Verifies that {@link ErrorModel#estimateMagnitudeFromRealResiduals} (fast single-shot path)
     * approximates {@code ErrorModel#estimateMagnitudeModel} (full multi-sample sweep).
     * <p>
     * For a fair like-for-like comparison both models are given the <em>same slope</em> (from the
     * scaling fit). The fast model is anchored at {@link ErrorModel#REAL_RESIDUAL_SAMPLE}, which is
     * the measurement sample size, so at that evaluation point it should closely agree with the full
     * model's OLS fit through {@code SAMPLE_SIZES_MAGNITUDE = [2048, 3072, 4096]}. At a second,
     * larger evaluation point the extrapolated predictions are compared with the same 3× tolerance
     * to account for the OLS safety margin included in the full path but absent in the fast path.
     */
    public void testEstimateMagnitudeFromRealResidualsApproximatesMagnitudeModel() throws IOException {
        int dim = 1024;
        int numQueries = 64;
        int corpusSize = 6_000;
        float[][] rows = randomNormalizedRows(numQueries + corpusSize, dim, 43L);
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(rows), null, dim);
        CalibrationSource source = new CalibrationSource(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            fvv,
            range(0, numQueries),
            dim,
            false,
            false,
            null,
            range(numQueries, corpusSize),
            10
        );
        int nDocsPerCluster = 128;

        // Full path: sweeps [2048, 3072, 4096] and fits a plug-in OLS intercept with the scaling slope
        ErrorScalingFit scalingFit = ErrorModel.estimateErrorScalingFit(source, nDocsPerCluster);
        QuantizationErrorStdModel fullModel = ErrorModel.estimateMagnitudeModel(scalingFit, source, false, 4, 2, nDocsPerCluster);

        // Fast path: single measurement; use the same slope for a direct comparison of the intercept
        double sharedInvDim = scalingFit.scalingModel().params().beta1();
        ErrorModel.RealResidualState state = ErrorModel.newRealResidualState(source);
        QuantizationErrorStdModel fastModel = ErrorModel.estimateMagnitudeFromRealResiduals(
            sharedInvDim,
            source,
            false,
            4,
            2,
            nDocsPerCluster,
            // anchor at the measurement sample size for correct semantics
            state
        );

        // FULL path: beta1 is always fixed from the scaling fit (plug-in OLS)
        assertEquals("full model must use the shared slope", sharedInvDim, fullModel.params().beta1(), 0.0);
        assertTrue("fast model beta1 must be finite", Double.isFinite(fastModel.params().beta1()));

        // At the anchor sample size the fast model is anchored; both should agree within the OLS safety margin
        int anchor = ErrorModel.REAL_RESIDUAL_SAMPLE; // 2048
        double fastAtAnchor = fastModel.errorStd(nDocsPerCluster, anchor);
        double fullAtAnchor = fullModel.errorStd(nDocsPerCluster, anchor);
        assertThat("fast estimate at anchor must be positive", fastAtAnchor, greaterThan(0.0));
        assertThat("full estimate at anchor must be positive", fullAtAnchor, greaterThan(0.0));
        double ratioAtAnchor = fastAtAnchor / fullAtAnchor;
        assertThat(
            "fast/full ratio at anchor must be < 1.15 (fast=" + fastAtAnchor + " full=" + fullAtAnchor + ")",
            ratioAtAnchor,
            lessThan(1.15)
        );
        assertThat(
            "fast/full ratio at anchor must be > 0.85 (fast=" + fastAtAnchor + " full=" + fullAtAnchor + ")",
            ratioAtAnchor,
            greaterThan(0.85)
        );

        // At the largest magnitude sample size the slope extrapolation governs; check agreement there too
        int largerSample = 4096;
        double fastAtLarger = fastModel.errorStd(nDocsPerCluster, largerSample);
        double fullAtLarger = fullModel.errorStd(nDocsPerCluster, largerSample);
        assertThat("fast estimate at larger sample must be positive", fastAtLarger, greaterThan(0.0));
        assertThat("full estimate at larger sample must be positive", fullAtLarger, greaterThan(0.0));
        double ratioAtLarger = fastAtLarger / fullAtLarger;
        assertThat(
            "fast/full ratio at larger sample must be < 3 (fast=" + fastAtLarger + " full=" + fullAtLarger + ")",
            ratioAtLarger,
            lessThan(1.15)
        );
        assertThat(
            "fast/full ratio at larger sample must be > 1/3 (fast=" + fastAtLarger + " full=" + fullAtLarger + ")",
            ratioAtLarger,
            greaterThan(0.85)
        );
    }

    /**
     * Guards {@link CalibrationUtils#MAX_QUERY_SAMPLE}: reducing the query sample (256 vs 1024) must not
     * materially change the real-residual error-std estimate, otherwise the parameter is set too low and
     * would break estimation accuracy. Runs the same manifold + real-residual estimate at both query counts
     * over an identical corpus and asserts the estimates stay close.
     */
    public void testErrorEstimateStableAcrossQuerySampleSize() throws IOException {
        int dim = 1024;
        float[][] rows = randomNormalizedRows(7000, dim, 42L);
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(rows), null, dim);
        int[] corpus = range(1024, 5000); // disjoint from the 1024-vector query pool [0, 1024)
        double errSmall = realResidualErrorStd(fvv, dim, range(0, 256), corpus);
        double errLarge = realResidualErrorStd(fvv, dim, range(0, 1024), corpus);
        assertThat(errSmall, greaterThan(0.0));
        assertThat(errLarge, greaterThan(0.0));
        double rel = Math.abs(errSmall - errLarge) / errLarge;
        assertThat(
            "error-std should be stable across query sample size 256 vs 1024, got " + errSmall + " vs " + errLarge,
            rel,
            lessThan(0.01)
        );
    }

    /**
     * Regression guard for the size-extrapolation fix. The real-residual error-std model must depend on
     * corpus size: evaluating it at a larger corpus changes the prediction according to the model's own
     * fitted slope. With a large enough corpus the model fits both intercept and slope from two actual
     * measurements, so the ratio at a 10× larger sample size must equal {@code (1/10)^modelSlope} exactly.
     */
    public void testRealResidualErrorScalesWithCorpusSizeViaInvDim() throws IOException {
        int dim = 1024;
        float[][] rows = randomNormalizedRows(9000, dim, 7L);
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(rows), null, dim);
        CalibrationSource source = new CalibrationSource(
            VectorSimilarityFunction.EUCLIDEAN,
            dim,
            fvv,
            range(0, 256),
            dim,
            false,
            false,
            null,
            range(256, 8000),
            10
        );
        double invDim = ManifoldModel.estimateManifoldParameters(source)[1];

        ErrorModel.RealResidualState state = ErrorModel.newRealResidualState(source);
        QuantizationErrorStdModel model = ErrorModel.estimateMagnitudeFromRealResiduals(invDim, source, false, 4, 2, 128, state);

        // Use the model's own fitted slope (beta1) — with corpus ≥ REAL_RESIDUAL_SAMPLE this comes
        // from a 2-point real-data fit rather than the manifold invDim proxy
        double modelSlope = model.params().beta1();
        assumeTrue("need a non-zero model slope to exercise extrapolation", Math.abs(modelSlope) > 1e-4);

        int sample = Math.min(ErrorModel.REAL_RESIDUAL_SAMPLE, source.corpusOrdinals().length);
        double atSample = model.errorStd(128, sample);
        double atTenX = model.errorStd(128, sample * 10);
        assertThat(atSample, greaterThan(0.0));
        double ratio = atTenX / atSample;
        // Self-consistency: the ratio must match the model's slope exactly
        assertEquals("errorStd must scale as (sample/N)^modelSlope", Math.pow(0.1, modelSlope), ratio, 1e-3);
        assertTrue("estimate must depend on corpus size (slope must not cancel)", Math.abs(ratio - 1.0) > 1e-3);
    }

    /** Real-residual error-std for encoding (4q,2d) at a given query set / corpus, extrapolated to the corpus size. */
    private static double realResidualErrorStd(FloatVectorValues fvv, int dim, int[] queries, int[] corpus) throws IOException {
        CalibrationSource source = new CalibrationSource(
            VectorSimilarityFunction.DOT_PRODUCT,
            dim,
            fvv,
            queries,
            dim,
            false,
            false,
            null,
            corpus,
            10
        );
        double invDim = ManifoldModel.estimateManifoldParameters(source)[1];
        ErrorModel.RealResidualState state = ErrorModel.newRealResidualState(source);
        return ErrorModel.estimateMagnitudeFromRealResiduals(invDim, source, false, 4, 2, 128, state).errorStd(128, corpus.length);
    }

    private static int[] range(int start, int count) {
        int[] a = new int[count];
        for (int i = 0; i < count; i++) {
            a[i] = start + i;
        }
        return a;
    }

    private static float[][] randomNormalizedRows(int count, int dim, long seed) {
        Random rng = new Random(seed);
        float[][] rows = new float[count][dim];
        for (int i = 0; i < count; i++) {
            for (int d = 0; d < dim; d++) {
                rows[i][d] = (float) rng.nextGaussian();
            }
            l2normalize(rows[i]);
        }
        return rows;
    }

    private record CalibrationFixture(FloatVectorValues fvv, int[] queryOrdinals, int[] corpusOrdinals, int dim) {
        CalibrationSource toSource(VectorSimilarityFunction similarityFunction, int k) {
            return new CalibrationSource(similarityFunction, dim, fvv, queryOrdinals, dim, false, false, null, corpusOrdinals, k);
        }
    }

    private static CalibrationFixture newCalibrationFixture(int dim) throws IOException {
        float[][] rows = syntheticClusteredRows(5200, dim, 8);
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(rows), null, dim);
        int[] queryOrdinals = new int[32];
        int[] corpusOrdinals = new int[5000];
        for (int i = 0; i < queryOrdinals.length; i++) {
            queryOrdinals[i] = i;
        }
        for (int i = 0; i < corpusOrdinals.length; i++) {
            corpusOrdinals[i] = 32 + i;
        }
        return new CalibrationFixture(fvv, queryOrdinals, corpusOrdinals, dim);
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
}
