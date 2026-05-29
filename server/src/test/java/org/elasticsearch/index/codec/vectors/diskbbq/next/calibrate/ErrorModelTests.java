/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.next.AutoCalibrationVectorFixtures;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

public class ErrorModelTests extends ESTestCase {

    public void testSimExactEuclideanUsesNegativeSquaredDistanceConvention() {
        float[] query = { 1f, 0f };
        float[] doc = { 0f, 1f };
        double sim = ErrorModel.simExact(VectorSimilarityFunction.EUCLIDEAN, 2, query, doc);
        assertTrue(sim < 0.0);
    }

    public void testSimExactDotProductIsInnerProduct() {
        float[] query = { 1f, 2f };
        float[] doc = { 3f, 4f };
        double sim = ErrorModel.simExact(VectorSimilarityFunction.DOT_PRODUCT, 2, query, doc);
        assertEquals(11.0, sim, 1e-5);
    }

    public void testDotIntComputesInnerProduct() {
        int[] a = { 1, 2, 3, 99 };
        int[] b = { 4, 5, 6, 99 };
        assertEquals((long) 4 + 2 * 5 + 3 * 6, ErrorModel.dotInt(3, a, 0, b, 0));
    }

    public void testEstimateQuantizationErrorStdModelReturnsFiniteModel() throws IOException {
        float[][] rows = AutoCalibrationVectorFixtures.syntheticClusteredRows(5200, 8, 8);
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(rows), null, 8);
        int[] queryOrdinals = new int[32];
        int[] corpusOrdinals = new int[5000];
        for (int i = 0; i < queryOrdinals.length; i++) {
            queryOrdinals[i] = i;
        }
        for (int i = 0; i < corpusOrdinals.length; i++) {
            corpusOrdinals[i] = 32 + i;
        }
        CalibrationQueries queries = new CalibrationQueries(fvv, queryOrdinals, 8, false, false, null, 8);
        QuantizationErrorStdModel model = ErrorModel.estimateQuantizationErrorStdModel(
            VectorSimilarityFunction.EUCLIDEAN,
            8,
            queries,
            fvv,
            corpusOrdinals,
            false,
            10,
            128
        );
        assertTrue(Double.isFinite(model.params().beta0()));
        assertTrue(Double.isFinite(model.params().beta1()));
        assertThat(model.errorStd(128, 5000), greaterThan(0.0));
    }

}
