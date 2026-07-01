/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExpectedRecallTests extends ESTestCase {

    private static final double ALPHA = -2.0;
    private static final double INV_DIM = 0.5;
    private static final int CORPUS_SIZE = 10_000;

    public void testErfAtKnownValues() {
        assertThat(ExpectedRecall.erf(0.0), closeTo(0.0, 1e-6));
        assertThat(ExpectedRecall.erf(1.0), closeTo(0.8427007929497149, 1e-7));
        assertThat(ExpectedRecall.erf(2.0), closeTo(0.9953222650189523, 1e-7));
        assertThat(ExpectedRecall.erf(-1.0), closeTo(-ExpectedRecall.erf(1.0), 1e-6));
    }

    public void testNormalCdfAtMeanAndTails() {
        assertThat(ExpectedRecall.normalCdf(0.0, 0.0, 1.0), closeTo(0.5, 1e-6));
        assertThat(ExpectedRecall.normalCdf(1.0, 0.0, 1.0), closeTo(0.8413447460685429, 1e-7));
        assertThat(ExpectedRecall.normalCdf(-1.0, 0.0, 1.0), closeTo(0.15865525393145707, 1e-7));
        assertThat(ExpectedRecall.normalCdf(10.0, 0.0, 1.0), closeTo(1.0, 1e-7));
        assertThat(ExpectedRecall.normalCdf(-10.0, 0.0, 1.0), closeTo(0.0, 1e-7));
    }

    public void testNormalPdfPeaksAtMeanAndIntegratesToOne() {
        double mean = 2.5;
        double stddev = 0.75;
        double peak = ExpectedRecall.normalPdf(mean, mean, stddev);
        assertThat(ExpectedRecall.normalPdf(mean + stddev, mean, stddev), lessThan(peak));
        assertThat(ExpectedRecall.normalPdf(mean - stddev, mean, stddev), lessThan(peak));

        double integral = ExpectedRecall.quadrature(
            x -> ExpectedRecall.normalPdf(x, mean, stddev),
            mean - 10.0 * stddev,
            mean + 10.0 * stddev,
            12
        );
        assertThat(integral, closeTo(1.0, 1e-3));
    }

    public void testQuadratureIntegratesPolynomials() {
        assertThat(ExpectedRecall.quadrature(x -> 1.0, 0.0, 1.0, 4), closeTo(1.0, 1e-10));
        assertThat(ExpectedRecall.quadrature(x -> x, 0.0, 1.0, 4), closeTo(0.5, 1e-10));
        assertThat(ExpectedRecall.quadrature(x -> x * x, 0.0, 1.0, 8), closeTo(1.0 / 3.0, 1e-8));
        assertThat(ExpectedRecall.quadrature(Math::sin, 0.0, Math.PI, 8), closeTo(2.0, 1e-6));
    }

    public void testProbabilityRankLessThanNIncreasesWithThreshold() {
        double[] rankDistances = { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8 };
        double errorStd = 0.05;
        double x = 0.45;
        double p3 = ExpectedRecall.probabilityRankLessThanN(rankDistances, errorStd, 3, x);
        double p6 = ExpectedRecall.probabilityRankLessThanN(rankDistances, errorStd, 6, x);
        assertThat(p3, greaterThanOrEqualTo(0.0));
        assertThat(p6, greaterThanOrEqualTo(p3));
        assertThat(p6, lessThanOrEqualTo(1.0));
    }

    public void testExpectedRecallOfKIsBoundedAndMonotoneInRerankDepth() {
        double[] rankDistances = rankDistances(VectorSimilarityFunction.EUCLIDEAN, CORPUS_SIZE, ALPHA, INV_DIM, 200);
        double errorStd = 0.02;
        double shallow = ExpectedRecall.expectedRecallOfK(rankDistances, errorStd, 5, 1);
        double deep = ExpectedRecall.expectedRecallOfK(rankDistances, errorStd, 50, 1);
        assertThat(shallow, greaterThanOrEqualTo(0.0));
        assertThat(shallow, lessThanOrEqualTo(1.0));
        assertThat(deep, greaterThanOrEqualTo(shallow));
    }

    public void testExpectedRecallAtKMatchesAverageOfPerRankTerms() {
        int k = 5;
        int rerank = 20;
        double errorStd = 0.03;
        double[] rankDistances = rankDistances(VectorSimilarityFunction.EUCLIDEAN, CORPUS_SIZE, ALPHA, INV_DIM, 20 * rerank);

        double manual = 0;
        for (int rank = 1; rank <= k; rank++) {
            manual += ExpectedRecall.expectedRecallOfK(rankDistances, errorStd, rerank, rank);
        }
        manual /= k;

        double fromApi = ExpectedRecall.expectedRecallAtK(
            VectorSimilarityFunction.EUCLIDEAN,
            CORPUS_SIZE,
            ALPHA,
            INV_DIM,
            errorStd,
            k,
            rerank
        );
        assertThat(fromApi, closeTo(manual, 1e-12));
    }

    public void testExpectedRecallDecreasesAsErrorStdIncreases() {
        int n = 10_000;
        double alpha = -2.0;
        double invDim = 0.5;
        int k = 10;
        int rerank = 25;
        double lowError = ExpectedRecall.expectedRecallAtK(VectorSimilarityFunction.EUCLIDEAN, n, alpha, invDim, 0.001, k, rerank);
        double highError = ExpectedRecall.expectedRecallAtK(VectorSimilarityFunction.EUCLIDEAN, n, alpha, invDim, 0.5, k, rerank);
        assertThat(lowError, greaterThan(highError));
        assertThat(lowError, lessThan(1.01));
        assertThat(highError, greaterThan(0.0));
    }

    public void testExpectedRecallAtKIsBounded() {
        double recall = ExpectedRecall.expectedRecallAtK(VectorSimilarityFunction.EUCLIDEAN, 50_000, -1.5, 0.3, 0.05, 10, 25);
        assertThat(recall, greaterThan(0.0));
        assertTrue(Double.isFinite(recall));
        assertTrue(recall <= 1.0);
    }

    public void testExpectedRecallAtKIncreasesWithRerankDepth() {
        double shallow = ExpectedRecall.expectedRecallAtK(VectorSimilarityFunction.EUCLIDEAN, CORPUS_SIZE, ALPHA, INV_DIM, 0.05, 10, 10);
        double deep = ExpectedRecall.expectedRecallAtK(VectorSimilarityFunction.EUCLIDEAN, CORPUS_SIZE, ALPHA, INV_DIM, 0.05, 10, 40);
        assertThat(deep, greaterThan(shallow));
    }

    public void testExpectedRecallAtKSupportsDotProductManifold() {
        double recall = ExpectedRecall.expectedRecallAtK(VectorSimilarityFunction.DOT_PRODUCT, CORPUS_SIZE, ALPHA, INV_DIM, 0.05, 10, 25);
        assertThat(recall, greaterThan(0.0));
        assertTrue(Double.isFinite(recall));
        assertTrue(recall <= 1.0);
    }

    public void testExpectedRecallAtKWithTinyErrorStdIsNearPerfect() {
        double recall = ExpectedRecall.expectedRecallAtK(VectorSimilarityFunction.EUCLIDEAN, CORPUS_SIZE, ALPHA, INV_DIM, 1e-6, 1, 50);
        assertThat(recall, greaterThan(0.99));
        assertTrue(recall <= 1.0);
    }

    private static double[] rankDistances(
        VectorSimilarityFunction similarityFunction,
        int corpusSize,
        double alpha,
        double invDim,
        int maxRank
    ) {
        double[] rankDistances = new double[maxRank];
        for (int i = 0; i < maxRank; i++) {
            rankDistances[i] = ManifoldModel.expectedRankDistance(similarityFunction, alpha, invDim, corpusSize, i + 1);
        }
        return rankDistances;
    }
}
