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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ExpectedRecallTests extends ESTestCase {

    public void testRerankNRoundsHalfUp() {
        assertEquals(25, ExpectedRecall.rerankN(10, 5, 2));
        assertEquals(15, ExpectedRecall.rerankN(10, 15, 10));
        assertEquals(18, ExpectedRecall.rerankN(10, 7, 4));
        assertEquals(20, ExpectedRecall.rerankN(10, 2, 1));
    }

    public void testRerankNFromDepth() {
        assertEquals(13, ExpectedRecall.rerankN(10, 1.25));
        assertEquals(15, ExpectedRecall.rerankN(10, 1.5));
        assertEquals(18, ExpectedRecall.rerankN(10, 1.75));
        assertEquals(20, ExpectedRecall.rerankN(10, 2.0));
        assertEquals(23, ExpectedRecall.rerankN(10, 2.25));
        assertEquals(25, ExpectedRecall.rerankN(10, 2.5));
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
}
