/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.util.VectorUtil.l2normalize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * End-to-end calibration model integration: fits {@link ManifoldModel} and {@link ErrorModel}
 * on the same corpus, then feeds both into {@link ExpectedRecall} the way merge-time
 * auto-calibration does before encoding selection.
 */
public class CalibrationModelsIntegrationTests extends ESTestCase {

    private static final int CALIBRATION_K = 10;
    private static final int VECTORS_PER_CLUSTER = 128;
    private static final double RERANK_DEPTH = 2.5;

    public void testManifoldAndErrorModelsProduceBoundedExpectedRecall() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(32);
        int numVectors = fixture.corpusOrdinals().length;

        double recall = expectedRecallForEncoding(
            fixture,
            VectorSimilarityFunction.EUCLIDEAN,
            randomIntBetween(1, 4),
            randomIntBetween(1, 4),
            numVectors
        );

        assertTrue(Double.isFinite(recall));
        assertThat(recall, greaterThan(0.0));
        assertThat(recall, lessThan(1.01));
    }

    /**
     * Both EUCLIDEAN and DOT_PRODUCT paths fit manifold and error models on the same
     * corpus and produce finite expected-recall estimates.
     */
    public void testSimilarityFunctionsComposeWithExpectedRecall() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(32);
        int numVectors = fixture.corpusOrdinals().length;

        for (VectorSimilarityFunction similarityFunction : List.of(
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarityFunction.DOT_PRODUCT
        )) {
            double recall = expectedRecallForEncoding(
                fixture,
                similarityFunction,
                randomIntBetween(1, 4),
                randomIntBetween(1, 4),
                numVectors
            );
            assertTrue(Double.isFinite(recall));
            assertThat(recall, greaterThan(0.0));
            assertThat(recall, lessThan(1.01));
        }
    }

    /**
     * End-to-end check that {@link ErrorModel#estimateErrorScalingFit} and
     * {@link ErrorModel#estimateMagnitudeModel} compose: scaling is fit once at 4q/1d,
     * then magnitude is fit for a target encoding while reusing the scaling slope.
     */
    public void testErrorScalingFitAndMagnitudeModelCompose() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(32);
        int numVectors = fixture.corpusOrdinals().length;
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;

        ErrorScalingFit scalingFit = estimateErrorScalingFit(fixture, similarityFunction);
        QuantizationErrorStdModel scalingModel = scalingFit.scalingModel();
        assertTrue(Double.isFinite(scalingModel.params().beta0()));
        assertTrue(Double.isFinite(scalingModel.params().beta1()));

        QuantizationErrorStdModel magnitudeModel = estimateErrorMagnitude(fixture, similarityFunction, scalingFit, 4, 2);
        assertEquals(scalingModel.params().beta1(), magnitudeModel.params().beta1(), 0.0);
        assertThat(magnitudeModel.errorStd(VECTORS_PER_CLUSTER, numVectors), greaterThan(0.0));
    }

    /**
     * A single scaling fit can seed magnitude fits for multiple encodings on the same corpus.
     */
    public void testErrorScalingFitReusedForMultipleMagnitudeModels() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(32);
        int numVectors = fixture.corpusOrdinals().length;
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;

        ErrorScalingFit scalingFit = estimateErrorScalingFit(fixture, similarityFunction);
        double scalingSlope = scalingFit.scalingModel().params().beta1();

        QuantizationErrorStdModel oneBitDoc = estimateErrorMagnitude(fixture, similarityFunction, scalingFit, 4, 1);
        QuantizationErrorStdModel fourBitDoc = estimateErrorMagnitude(fixture, similarityFunction, scalingFit, 4, 4);

        assertEquals(scalingSlope, oneBitDoc.params().beta1(), 0.0);
        assertEquals(scalingSlope, fourBitDoc.params().beta1(), 0.0);
        assertThat(oneBitDoc.errorStd(VECTORS_PER_CLUSTER, numVectors), greaterThan(fourBitDoc.errorStd(VECTORS_PER_CLUSTER, numVectors)));
    }

    /**
     * The error model's std should grow when we evaluate a coarser document bit width,
     * using manifold parameters from the same fixture.
     */
    public void testCoarserDocumentQuantizationIncreasesErrorStd() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(32);
        int numVectors = fixture.corpusOrdinals().length;
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;

        ErrorScalingFit scalingFit = estimateErrorScalingFit(fixture, similarityFunction);
        QuantizationErrorStdModel oneBitDoc = estimateErrorMagnitude(fixture, similarityFunction, scalingFit, 4, 1);
        QuantizationErrorStdModel fourBitDoc = estimateErrorMagnitude(fixture, similarityFunction, scalingFit, 4, 4);

        double coarseStd = oneBitDoc.errorStd(VECTORS_PER_CLUSTER, numVectors);
        double fineStd = fourBitDoc.errorStd(VECTORS_PER_CLUSTER, numVectors);
        assertThat(coarseStd, greaterThan(0.0));
        assertThat(fineStd, greaterThan(0.0));
        assertThat(coarseStd, greaterThan(fineStd));
    }

    private static double expectedRecallForEncoding(
        CalibrationFixture fixture,
        VectorSimilarityFunction similarityFunction,
        int qbits,
        int dbits,
        int numVectors
    ) throws IOException {
        double[] manifold = estimateManifold(fixture, similarityFunction);
        double alpha = manifold[0];
        double invDim = manifold[1];

        ErrorScalingFit scalingFit = estimateErrorScalingFit(fixture, similarityFunction);
        QuantizationErrorStdModel errorModel = estimateErrorMagnitude(fixture, similarityFunction, scalingFit, qbits, dbits);
        double errorStd = errorModel.errorStd(VECTORS_PER_CLUSTER, numVectors);
        int rerank = ExpectedRecall.rerankN(CALIBRATION_K, RERANK_DEPTH);
        return ExpectedRecall.expectedRecallAtK(similarityFunction, numVectors, alpha, invDim, errorStd, CALIBRATION_K, rerank);
    }

    private static double[] estimateManifold(CalibrationFixture fixture, VectorSimilarityFunction similarityFunction) throws IOException {
        return ManifoldModel.estimateManifoldParameters(toSource(fixture, similarityFunction));
    }

    private static ErrorScalingFit estimateErrorScalingFit(CalibrationFixture fixture, VectorSimilarityFunction similarityFunction) {
        return ErrorModel.estimateErrorScalingFit(toSource(fixture, similarityFunction), VECTORS_PER_CLUSTER);
    }

    private static QuantizationErrorStdModel estimateErrorMagnitude(
        CalibrationFixture fixture,
        VectorSimilarityFunction similarityFunction,
        ErrorScalingFit scalingFit,
        int qbits,
        int dbits
    ) {
        return ErrorModel.estimateMagnitudeModel(
            scalingFit,
            toSource(fixture, similarityFunction),
            false,
            qbits,
            dbits,
            VECTORS_PER_CLUSTER
        );
    }

    private static CalibrationSource toSource(CalibrationFixture fixture, VectorSimilarityFunction similarityFunction) {
        return new CalibrationSource(
            similarityFunction,
            fixture.dim(),
            fixture.fvv(),
            fixture.queryOrdinals(),
            fixture.dim(),
            false,
            false,
            null,
            fixture.corpusOrdinals(),
            CALIBRATION_K
        );
    }

    private record CalibrationFixture(FloatVectorValues fvv, int[] queryOrdinals, int[] corpusOrdinals, int dim) {}

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
