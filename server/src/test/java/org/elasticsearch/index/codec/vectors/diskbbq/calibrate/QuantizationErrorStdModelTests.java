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
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class QuantizationErrorStdModelTests extends ESTestCase {

    public void testErrorStdIncreasesWithClusterSize() {
        Regression.OLSResult params = new Regression.OLSResult(-5.0, 0.2, 0.01, 0.001, 0.0, 0.01);
        QuantizationErrorStdModel model = new QuantizationErrorStdModel(params);
        int sampleSize = 4096;
        double smallCluster = model.errorStd(64, sampleSize);
        double largeCluster = model.errorStd(256, sampleSize);
        assertThat(smallCluster, greaterThan(0.0));
        assertThat(largeCluster, greaterThan(smallCluster));
    }

    public void testErrorStdDecreasesWithSampleSize() {
        Regression.OLSResult params = new Regression.OLSResult(-4.0, 0.15, 0.01, 0.001, 0.0, 0.01);
        QuantizationErrorStdModel model = new QuantizationErrorStdModel(params);
        int nDocsPerCluster = 128;
        double smallSample = model.errorStd(nDocsPerCluster, 4096);
        double largeSample = model.errorStd(nDocsPerCluster, 16384);
        assertThat(smallSample, greaterThan(0.0));
        assertThat(largeSample, lessThan(smallSample));
    }

    /**
     * End-to-end usage: measure scalar-quantization similarity error at several corpus sample sizes,
     * regress log(error std) on log(nDocsPerCluster) - log(sampleSize), then predict deployment error std.
     */
    public void testErrorStdFromScalarQuantizationMeasurements() throws IOException {
        int dim = 32;
        int vectorsPerCluster = 64;
        int fullCorpusSize = 512;
        int[] sampleSizes = { 128, 192, 256, 320, 384 };
        byte qbits = 4;
        byte dbits = 1;

        Random rnd = new Random(42);
        FloatVectorValues corpus = randomUnitVectors(rnd, fullCorpusSize, dim);
        float[] query = randomUnitVector(rnd, dim);
        float[] centroid = new float[dim];

        int[] corpusOrdinals = new int[fullCorpusSize];
        for (int i = 0; i < corpusOrdinals.length; i++) {
            corpusOrdinals[i] = i;
        }

        Regression.OLSAccumulator accumulator = new Regression.OLSAccumulator();
        double logNDocsPerCluster = Math.log(vectorsPerCluster);
        for (int sampleSize : sampleSizes) {
            double measured = measureQuantizationErrorStd(corpus, corpusOrdinals, sampleSize, query, qbits, dbits, centroid);
            assertThat(measured, greaterThan(0.0));
            double x = logNDocsPerCluster - Math.log(sampleSize);
            double y = Math.log(Math.max(measured, 1e-38));
            accumulator.update(new double[] { x }, new double[] { y });
        }

        QuantizationErrorStdModel model = new QuantizationErrorStdModel(accumulator.fit());
        double predicted = model.errorStd(vectorsPerCluster, fullCorpusSize);

        double xPredict = Math.log(vectorsPerCluster) - Math.log(fullCorpusSize);
        Regression.Prediction prediction = Regression.predictOls(model.params(), xPredict);
        assertThat(predicted, closeTo(Math.exp(prediction.mean() + 3.0 * prediction.std()), 1e-10));
        assertTrue(Double.isFinite(predicted));
        assertThat(predicted, greaterThan(0.0));
    }

    /**
     * Standard deviation of similarity error after OSQ, using the {@code sqrt(3 * sampleVariance)} statistic.
     */
    private static double measureQuantizationErrorStd(
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int corpusLength,
        float[] query,
        byte qbits,
        byte dbits,
        float[] centroid
    ) throws IOException {
        int dim = fvv.dimension();
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(VectorSimilarityFunction.EUCLIDEAN);
        float[] residualScratch = new float[dim];
        int[] quantizeScratch = new int[dim];
        float[] queryVector = query.clone();

        OptimizedScalarQuantizer.QuantizationResult queryResult = quantizer.scalarQuantize(
            queryVector,
            residualScratch,
            quantizeScratch,
            qbits,
            centroid
        );
        byte[] queryQuantized = new byte[dim];
        packQuantizedAsBytes(quantizeScratch, queryQuantized, dim);

        double dScale = 1.0 / ((1 << dbits) - 1);
        double qScale = 1.0 / ((1 << qbits) - 1);
        double aq = queryResult.lowerInterval();
        double lq = qScale * (queryResult.upperInterval() - queryResult.lowerInterval());
        int queryL1 = queryResult.quantizedComponentSum();

        WelfordVariance moments = new WelfordVariance();
        for (int i = 0; i < corpusLength; i++) {
            float[] doc = fvv.vectorValue(corpusOrdinals[i]);
            OptimizedScalarQuantizer.QuantizationResult docResult = quantizer.scalarQuantize(
                doc,
                residualScratch,
                quantizeScratch,
                dbits,
                centroid
            );
            byte[] docQuantized = new byte[dim];
            packQuantizedAsBytes(quantizeScratch, docQuantized, dim);

            double ad = docResult.lowerInterval();
            double ld = dScale * (docResult.upperInterval() - docResult.lowerInterval());
            long intDot = (long) ESVectorUtil.dotProduct(docQuantized, queryQuantized);
            double dotEst = ad * aq * dim + aq * ld * docResult.quantizedComponentSum() + ad * lq * queryL1 + ld * lq * intDot;

            double corpusDotCentroid = ESVectorUtil.dotProduct(centroid, doc);
            double queryDotCentroid = ESVectorUtil.dotProduct(queryVector, centroid);
            double centroidDotCentroid = ESVectorUtil.dotProduct(centroid, centroid);
            dotEst += corpusDotCentroid + queryDotCentroid - centroidDotCentroid;

            double docDotDoc = ESVectorUtil.dotProduct(doc, doc);
            double simQuantized = 2.0 * dotEst - docDotDoc;
            double simExact = 2.0 * ESVectorUtil.dotProduct(queryVector, doc) - docDotDoc;
            moments.add(simExact - simQuantized);
        }
        return Math.sqrt(3.0 * moments.sampleVariance());
    }

    private static void packQuantizedAsBytes(int[] quantized, byte[] destination, int dim) {
        for (int d = 0; d < dim; d++) {
            destination[d] = (byte) quantized[d];
        }
    }

    private static FloatVectorValues randomUnitVectors(Random rnd, int count, int dim) throws IOException {
        List<float[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            vectors.add(randomUnitVector(rnd, dim));
        }
        return KMeansFloatVectorValues.build(vectors, null, dim);
    }

    private static float[] randomUnitVector(Random rnd, int dim) {
        float[] vector = new float[dim];
        for (int d = 0; d < dim; d++) {
            vector[d] = rnd.nextFloat();
        }
        ESVectorUtil.l2Normalize(vector, dim);
        return vector;
    }
}
