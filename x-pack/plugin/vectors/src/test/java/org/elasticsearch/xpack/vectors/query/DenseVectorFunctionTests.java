/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilarity;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProduct;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1Norm;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2Norm;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoderTests.mockEncodeDenseVector;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DenseVectorFunctionTests extends ESTestCase {

    public void testDenseVectorFunctions() {
        testDenseVectorFunctions(Version.V_7_4_0);
        testDenseVectorFunctions(Version.CURRENT);
    }

    private void testDenseVectorFunctions(Version indexVersion) {
        String field = "vector";
        float[] docVector = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f};
        BytesRef encodedDocVector = mockEncodeDenseVector(docVector, indexVersion);
        VectorScriptDocValues.DenseVectorScriptDocValues dvs = mock(VectorScriptDocValues.DenseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript._getIndexVersion()).thenReturn(indexVersion);
        when(scoreScript.getDoc()).thenReturn(Collections.singletonMap(field, dvs));

        List<Number> queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);

        // test dotProduct
        DotProduct dotProduct = new DotProduct(scoreScript, queryVector, field);
        double result = dotProduct.dotProduct();
        assertEquals("dotProduct result is not equal to the expected value!", 65425.624, result, 0.001);

        // test cosineSimilarity
        CosineSimilarity cosineSimilarity = new CosineSimilarity(scoreScript, queryVector, field);
        double result2 = cosineSimilarity.cosineSimilarity();
        assertEquals("cosineSimilarity result is not equal to the expected value!", 0.790, result2, 0.001);

        // test l1Norm
        L1Norm l1norm = new L1Norm(scoreScript, queryVector, field);
        double result3 = l1norm.l1norm();
        assertEquals("l1norm result is not equal to the expected value!", 485.184, result3, 0.001);

        // test l2norm
        L2Norm l2norm = new L2Norm(scoreScript, queryVector, field);
        double result4 = l2norm.l2norm();
        assertEquals("l2norm result is not equal to the expected value!", 301.361, result4, 0.001);

        // test dotProduct fails when queryVector has wrong number of dims
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);
        DotProduct dotProduct2 = new DotProduct(scoreScript, invalidQueryVector, field);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, dotProduct2::dotProduct);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));

        // test cosineSimilarity fails when queryVector has wrong number of dims
        CosineSimilarity cosineSimilarity2 = new CosineSimilarity(scoreScript, invalidQueryVector, field);
        e = expectThrows(IllegalArgumentException.class, cosineSimilarity2::cosineSimilarity);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));

        // test l1norm fails when queryVector has wrong number of dims
        L1Norm l1norm2 = new L1Norm(scoreScript, invalidQueryVector, field);
        e = expectThrows(IllegalArgumentException.class, l1norm2::l1norm);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));

        // test l2norm fails when queryVector has wrong number of dims
        L2Norm l2norm2 = new L2Norm(scoreScript, invalidQueryVector, field);
        e = expectThrows(IllegalArgumentException.class, l2norm2::l2norm);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
    }

    public void testDeprecatedDenseVectorFunctions() {
        testDeprecatedDenseVectorFunctions(Version.V_7_4_0);
        testDeprecatedDenseVectorFunctions(Version.CURRENT);
    }

    private void testDeprecatedDenseVectorFunctions(Version indexVersion) {
        float[] docVector = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f};
        BytesRef encodedDocVector = mockEncodeDenseVector(docVector, indexVersion);
        VectorScriptDocValues.DenseVectorScriptDocValues dvs = mock(VectorScriptDocValues.DenseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript._getIndexVersion()).thenReturn(indexVersion);

        List<Number> queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);

        // test dotProduct
        DotProduct dotProduct = new DotProduct(scoreScript, queryVector, dvs);
        double result = dotProduct.dotProduct();
        assertEquals("dotProduct result is not equal to the expected value!", 65425.624, result, 0.001);
        assertWarnings(ScoreScriptUtils.DEPRECATION_MESSAGE);

        // test cosineSimilarity
        CosineSimilarity cosineSimilarity = new CosineSimilarity(scoreScript, queryVector, dvs);
        double result2 = cosineSimilarity.cosineSimilarity();
        assertEquals("cosineSimilarity result is not equal to the expected value!", 0.790, result2, 0.001);
        assertWarnings(ScoreScriptUtils.DEPRECATION_MESSAGE);

        // test l1Norm
        L1Norm l1norm = new L1Norm(scoreScript, queryVector, dvs);
        double result3 = l1norm.l1norm();
        assertEquals("l1norm result is not equal to the expected value!", 485.184, result3, 0.001);
        assertWarnings(ScoreScriptUtils.DEPRECATION_MESSAGE);

        // test l2norm
        L2Norm l2norm = new L2Norm(scoreScript, queryVector, dvs);
        double result4 = l2norm.l2norm();
        assertEquals("l2norm result is not equal to the expected value!", 301.361, result4, 0.001);
        assertWarnings(ScoreScriptUtils.DEPRECATION_MESSAGE);

        // test dotProduct fails when queryVector has wrong number of dims
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);
        DotProduct dotProduct2 = new DotProduct(scoreScript, invalidQueryVector, dvs);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, dotProduct2::dotProduct);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
        assertWarnings(ScoreScriptUtils.DEPRECATION_MESSAGE);

        // test cosineSimilarity fails when queryVector has wrong number of dims
        CosineSimilarity cosineSimilarity2 = new CosineSimilarity(scoreScript, invalidQueryVector, dvs);
        e = expectThrows(IllegalArgumentException.class, cosineSimilarity2::cosineSimilarity);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
        assertWarnings(ScoreScriptUtils.DEPRECATION_MESSAGE);

        // test l1norm fails when queryVector has wrong number of dims
        L1Norm l1norm2 = new L1Norm(scoreScript, invalidQueryVector, dvs);
        e = expectThrows(IllegalArgumentException.class, l1norm2::l1norm);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
        assertWarnings(ScoreScriptUtils.DEPRECATION_MESSAGE);

        // test l2norm fails when queryVector has wrong number of dims
        L2Norm l2norm2 = new L2Norm(scoreScript, invalidQueryVector, dvs);
        e = expectThrows(IllegalArgumentException.class, l2norm2::l2norm);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
        assertWarnings(ScoreScriptUtils.DEPRECATION_MESSAGE);
    }
}
