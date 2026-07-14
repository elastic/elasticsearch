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
import java.util.Set;

import static org.apache.lucene.util.VectorUtil.l2normalize;
import static org.hamcrest.Matchers.greaterThan;

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

    public void testEstimateMagnitudeFromManifoldResidualsReturnsFiniteModel() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.EUCLIDEAN, 10);
        double alpha = -1.5;
        double invDim = 0.5;
        QuantizationErrorStdModel model = ErrorModel.estimateMagnitudeFromManifoldResiduals(alpha, invDim, source, false, 4, 2, 128, 5000);
        assertTrue(Double.isFinite(model.params().beta0()));
        assertTrue(Double.isFinite(model.params().beta1()));
        assertThat(model.errorStd(128, 5000), greaterThan(0.0));
    }

    public void testEstimateMagnitudeFromManifoldResidualsUsesInvDimAsSlope() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.EUCLIDEAN, 10);
        double invDim = 0.42;
        QuantizationErrorStdModel model = ErrorModel.estimateMagnitudeFromManifoldResiduals(-1.0, invDim, source, false, 4, 2, 128, 5000);
        // slope beta1 is taken directly from invDim, not refit
        assertEquals(invDim, model.params().beta1(), 0.0);
    }

    public void testEstimateMagnitudeFromManifoldResidualsWithDotProduct() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.DOT_PRODUCT, 10);
        double invDim = 0.4;
        QuantizationErrorStdModel model = ErrorModel.estimateMagnitudeFromManifoldResiduals(-1.2, invDim, source, false, 4, 1, 128, 5000);
        assertTrue(Double.isFinite(model.params().beta0()));
        assertEquals(invDim, model.params().beta1(), 0.0);
        assertThat(model.errorStd(128, 5000), greaterThan(0.0));
    }

    public void testEstimateMagnitudeFromManifoldResidualsSmallCorpus() throws IOException {
        // corpus smaller than SAMPLE_SIZES_MAGNITUDE[0]=2048 — nDocs is clamped to corpusOrdinals.length
        float[][] rows = syntheticClusteredRows(600, 8, 4);
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(rows), null, 8);
        int[] queryOrdinals = new int[10];
        int[] corpusOrdinals = new int[500];
        for (int i = 0; i < queryOrdinals.length; i++) {
            queryOrdinals[i] = i;
        }
        for (int i = 0; i < corpusOrdinals.length; i++) {
            corpusOrdinals[i] = 10 + i;
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
        double invDim = 0.3;
        QuantizationErrorStdModel model = ErrorModel.estimateMagnitudeFromManifoldResiduals(-1.0, invDim, source, false, 4, 2, 64, 500);
        assertTrue(Double.isFinite(model.params().beta0()));
        assertEquals(invDim, model.params().beta1(), 0.0);
    }

    public void testEstimateMagnitudeFromManifoldResidualsIsDeterministic() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(8);
        CalibrationSource source = fixture.toSource(VectorSimilarityFunction.EUCLIDEAN, 10);
        double alpha = -1.5;
        double invDim = 0.5;
        QuantizationErrorStdModel first = ErrorModel.estimateMagnitudeFromManifoldResiduals(alpha, invDim, source, false, 4, 2, 128, 5000);
        QuantizationErrorStdModel second = ErrorModel.estimateMagnitudeFromManifoldResiduals(alpha, invDim, source, false, 4, 2, 128, 5000);
        assertEquals(first.params().beta0(), second.params().beta0(), 0.0);
        assertEquals(first.params().beta1(), second.params().beta1(), 0.0);
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
