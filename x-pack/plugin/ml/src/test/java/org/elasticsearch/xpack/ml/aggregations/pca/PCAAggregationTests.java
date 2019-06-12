/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.search.aggregations.matrix.AbstractMatrixStatsTestCase;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsResults;

import java.io.IOException;

public class PCAAggregationTests extends AbstractMatrixStatsTestCase {

    private boolean useCovariance;

    @Override
    protected void initializeTestParams() {
        this.useCovariance = randomBoolean();
    }

    public void testAggregationAccuracy() throws IOException {
        testCase(new MatchAllDocsQuery(),
            pca -> {
                PCAStatsResults truth = getResults();
                InternalPCAStats pcaAgg = (InternalPCAStats) pca;
                assertNearlyEqual(truth, pcaAgg.getResults());
            });
    }

    @Override
    public PCAStatsResults computeResults() {
        results = new PCAStatsResults(baseTruthStats, useCovariance);
        return (PCAStatsResults) results;
    }

    @Override
    public PCAAggregationBuilder getAggregatorBuilder(String name) {
        PCAAggregationBuilder aggBuilder = new PCAAggregationBuilder(name);
        aggBuilder.setUseCovariance(useCovariance);
        aggBuilder.fields(fieldNames);
        return aggBuilder;
    }

    @Override
    protected void assertNearlyEqual(MatrixStatsResults a, MatrixStatsResults b) {
        super.assertNearlyEqual(a, b);

        assertTrue(a instanceof PCAStatsResults);
        assertTrue(b instanceof PCAStatsResults);

        PCAStatsResults resultA = (PCAStatsResults) a;
        PCAStatsResults resultB = (PCAStatsResults) b;

        for (String field : fieldNames) {
            // eigen values & vectors : note we're only checking that the values are not null because
            // subtle differences in the covariance matrix (out to e-7) can change
            // eigenvalue & eigenvector results by +/- 10 to 20, its not significant enough to change interpretation
            assertFalse(Double.isNaN(resultA.getEigenValue(field).getReal()));
            assertFalse(Double.isNaN(resultB.getEigenValue(field).getReal()));
            assertFalse(Double.isNaN(resultA.getEigenValue(field).getImaginary()));
            assertFalse(Double.isNaN(resultB.getEigenValue(field).getImaginary()));
            // eigen vectors
            double[] eigenVectorA = resultA.getEigenVector(field);
            double[] eigenVectorB = resultB.getEigenVector(field);
            for (int i = 0; i < eigenVectorA.length; ++i) {
                assertFalse(Double.isNaN(eigenVectorA[i]));
                assertFalse(Double.isNaN(eigenVectorB[i]));
            }
        }
    }
}
