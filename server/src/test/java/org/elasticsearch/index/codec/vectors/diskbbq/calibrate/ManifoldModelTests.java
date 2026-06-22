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
import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.util.VectorUtil.l2normalize;
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
}
