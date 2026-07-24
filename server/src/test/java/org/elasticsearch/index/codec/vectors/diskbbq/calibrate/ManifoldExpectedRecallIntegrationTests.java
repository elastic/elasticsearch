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
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.util.VectorUtil.l2normalize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * IT for how {@link ManifoldModel} and {@link ExpectedRecall} are expected to work together.
 * It estimates a {@link QuantizationErrorStdModel} from scalar-quantization measurements, then feeds both
 * into {@link ExpectedRecall}.
 */
public class ManifoldExpectedRecallIntegrationTests extends ESTestCase {

    private static final int CALIBRATION_K = 10;
    private static final int VECTORS_PER_CLUSTER = 128;
    private static final double RERANK_DEPTH = 2.5;
    private static final int[] ERROR_STD_SAMPLE_SIZES = { 512, 1024, 1536, 2048, 2560 };

    public void testManifoldAndErrorStdModelsProduceBoundedExpectedRecall() throws IOException {
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
     * Both similarity functions fit a manifold on the same corpus and compose with
     * {@link ExpectedRecall}.
     */
    public void testSimilarityFunctionsComposeWithExpectedRecall() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(32);
        int numVectors = fixture.corpusOrdinals().length;

        double euclideanRecall = expectedRecallForEncoding(
            fixture,
            VectorSimilarityFunction.EUCLIDEAN,
            randomIntBetween(1, 4),
            randomIntBetween(1, 4),
            numVectors
        );
        assertTrue(Double.isFinite(euclideanRecall));
        assertThat(euclideanRecall, greaterThan(0.0));
        assertThat(euclideanRecall, lessThan(1.01));

        double[] dotManifold = estimateManifold(fixture, VectorSimilarityFunction.DOT_PRODUCT);
        double dotRecall = ExpectedRecall.expectedRecallAtK(
            VectorSimilarityFunction.DOT_PRODUCT,
            numVectors,
            dotManifold[0],
            dotManifold[1],
            conservativePlaceholderErrorStd(numVectors),
            CALIBRATION_K,
            ExpectedRecall.rerankN(CALIBRATION_K, RERANK_DEPTH)
        );
        assertTrue(Double.isFinite(dotRecall));
        assertThat(dotRecall, greaterThan(0.0));
        assertThat(dotRecall, lessThan(1.01));
    }

    /**
     * Coarser document quantization should yield a larger predicted error std when the
     * scaling model is fit from measurements at each bit width.
     */
    public void testCoarserDocumentQuantizationIncreasesErrorStd() throws IOException {
        CalibrationFixture fixture = newCalibrationFixture(32);
        int numVectors = fixture.corpusOrdinals().length;
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;

        QuantizationErrorStdModel oneBitDoc = estimateErrorStdModel(fixture, similarityFunction, 4, 1);
        QuantizationErrorStdModel fourBitDoc = estimateErrorStdModel(fixture, similarityFunction, 4, 4);

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
        QuantizationErrorStdModel errorModel = estimateErrorStdModel(fixture, similarityFunction, qbits, dbits);
        double errorStd = errorModel.errorStd(VECTORS_PER_CLUSTER, numVectors);
        int rerank = ExpectedRecall.rerankN(CALIBRATION_K, RERANK_DEPTH);
        return ExpectedRecall.expectedRecallAtK(similarityFunction, numVectors, manifold[0], manifold[1], errorStd, CALIBRATION_K, rerank);
    }

    private static double[] estimateManifold(CalibrationFixture fixture, VectorSimilarityFunction similarityFunction) throws IOException {
        CalibrationSource source = new CalibrationSource(
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
        return ManifoldModel.estimateManifoldParameters(source);
    }

    private static QuantizationErrorStdModel estimateErrorStdModel(
        CalibrationFixture fixture,
        VectorSimilarityFunction similarityFunction,
        int qbits,
        int dbits
    ) throws IOException {
        Regression.OLSAccumulator accumulator = new Regression.OLSAccumulator();
        double logNDocsPerCluster = Math.log(VECTORS_PER_CLUSTER);
        float[] centroid = new float[fixture.dim()];
        for (int sampleSize : ERROR_STD_SAMPLE_SIZES) {
            if (sampleSize > fixture.corpusOrdinals().length) {
                break;
            }
            double measured = measureQuantizationErrorStd(fixture, similarityFunction, sampleSize, qbits, dbits, centroid);
            assertThat(measured, greaterThan(0.0));
            double x = logNDocsPerCluster - Math.log(sampleSize);
            double y = Math.log(measured);
            accumulator.update(new double[] { x }, new double[] { y });
        }
        return new QuantizationErrorStdModel(accumulator.fit());
    }

    private static double conservativePlaceholderErrorStd(int sampleSize) {
        return new QuantizationErrorStdModel(new Regression.OLSResult(-3.0, 0.2, 0.01, 0.001, 0.0, 0.01)).errorStd(
            VECTORS_PER_CLUSTER,
            sampleSize
        );
    }

    private static double measureQuantizationErrorStd(
        CalibrationFixture fixture,
        VectorSimilarityFunction similarityFunction,
        int corpusLength,
        int qbits,
        int dbits,
        float[] centroid
    ) throws IOException {
        int dim = fixture.dim();
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        float[] residualScratch = new float[dim];
        int[] quantizeScratch = new int[dim];

        WelfordVariance moments = new WelfordVariance();
        for (int qi = 0; qi < fixture.queryOrdinals().length; qi++) {
            float[] query = fixture.fvv().vectorValue(fixture.queryOrdinals()[qi]).clone();
            OptimizedScalarQuantizer.QuantizationResult queryResult = quantizer.scalarQuantize(
                query,
                residualScratch,
                quantizeScratch,
                (byte) qbits,
                centroid
            );
            byte[] queryQuantized = new byte[dim];
            packQuantizedAsBytes(quantizeScratch, queryQuantized, dim);

            double dScale = 1.0 / ((1 << dbits) - 1);
            double qScale = 1.0 / ((1 << qbits) - 1);
            double aq = queryResult.lowerInterval();
            double lq = qScale * (queryResult.upperInterval() - queryResult.lowerInterval());
            int queryL1 = queryResult.quantizedComponentSum();

            for (int i = 0; i < corpusLength; i++) {
                float[] doc = fixture.fvv().vectorValue(fixture.corpusOrdinals()[i]);
                OptimizedScalarQuantizer.QuantizationResult docResult = quantizer.scalarQuantize(
                    doc,
                    residualScratch,
                    quantizeScratch,
                    (byte) dbits,
                    centroid
                );
                byte[] docQuantized = new byte[dim];
                packQuantizedAsBytes(quantizeScratch, docQuantized, dim);

                double ad = docResult.lowerInterval();
                double ld = dScale * (docResult.upperInterval() - docResult.lowerInterval());
                long intDot = (long) ESVectorUtil.dotProduct(docQuantized, queryQuantized);
                double dotEst = ad * aq * dim + aq * ld * docResult.quantizedComponentSum() + ad * lq * queryL1 + ld * lq * intDot;

                double corpusDotCentroid = ESVectorUtil.dotProduct(centroid, doc);
                double queryDotCentroid = ESVectorUtil.dotProduct(query, centroid);
                double centroidDotCentroid = ESVectorUtil.dotProduct(centroid, centroid);
                dotEst += corpusDotCentroid + queryDotCentroid - centroidDotCentroid;

                double simExact;
                double simQuantized;
                if (similarityFunction == VectorSimilarityFunction.EUCLIDEAN) {
                    double docDotDoc = ESVectorUtil.dotProduct(doc, doc);
                    simQuantized = 2.0 * dotEst - docDotDoc;
                    simExact = 2.0 * ESVectorUtil.dotProduct(query, doc) - docDotDoc;
                } else {
                    simQuantized = dotEst;
                    simExact = ESVectorUtil.dotProduct(query, doc);
                }
                moments.add(simExact - simQuantized);
            }
        }
        return Math.sqrt(3.0 * moments.sampleVariance());
    }

    private static void packQuantizedAsBytes(int[] quantized, byte[] destination, int dim) {
        for (int d = 0; d < dim; d++) {
            destination[d] = (byte) quantized[d];
        }
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
