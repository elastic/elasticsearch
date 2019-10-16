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
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilarity;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilaritySparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProduct;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProductSparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1Norm;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1NormSparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2Norm;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2NormSparse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoderTests.mockEncodeDenseVector;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScoreScriptUtilsTests extends ESTestCase {

    public void testDenseVectorFunctions() {
        testDenseVectorFunctions(Version.V_7_4_0);
        testDenseVectorFunctions(Version.CURRENT);
    }

    private void testDenseVectorFunctions(Version indexVersion) {
        float[] docVector = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f};
        BytesRef encodedDocVector = mockEncodeDenseVector(docVector, indexVersion);
        VectorScriptDocValues.DenseVectorScriptDocValues dvs = mock(VectorScriptDocValues.DenseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript._getIndexVersion()).thenReturn(indexVersion);

        List<Number> queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);

        // test dotProduct
        DotProduct dotProduct = new DotProduct(scoreScript, queryVector);
        double result = dotProduct.dotProduct(dvs);
        assertEquals("dotProduct result is not equal to the expected value!", 65425.624, result, 0.001);

        // test cosineSimilarity
        CosineSimilarity cosineSimilarity = new CosineSimilarity(scoreScript, queryVector);
        double result2 = cosineSimilarity.cosineSimilarity(dvs);
        assertEquals("cosineSimilarity result is not equal to the expected value!", 0.790, result2, 0.001);

        // test l1Norm
        L1Norm l1norm = new L1Norm(scoreScript, queryVector);
        double result3 = l1norm.l1norm(dvs);
        assertEquals("l1norm result is not equal to the expected value!", 485.184, result3, 0.001);

        // test l2norm
        L2Norm l2norm = new L2Norm(scoreScript, queryVector);
        double result4 = l2norm.l2norm(dvs);
        assertEquals("l2norm result is not equal to the expected value!", 301.361, result4, 0.001);

        // test dotProduct fails when queryVector has wrong number of dims
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);
        DotProduct dotProduct2 = new DotProduct(scoreScript, invalidQueryVector);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> dotProduct2.dotProduct(dvs));
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));

        // test cosineSimilarity fails when queryVector has wrong number of dims
        CosineSimilarity cosineSimilarity2 = new CosineSimilarity(scoreScript, invalidQueryVector);
        e = expectThrows(IllegalArgumentException.class, () -> cosineSimilarity2.cosineSimilarity(dvs));
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));

        // test l1norm fails when queryVector has wrong number of dims
        L1Norm l1norm2 = new L1Norm(scoreScript, invalidQueryVector);
        e = expectThrows(IllegalArgumentException.class, () -> l1norm2.l1norm(dvs));
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));

        // test l2norm fails when queryVector has wrong number of dims
        L2Norm l2norm2 = new L2Norm(scoreScript, invalidQueryVector);
        e = expectThrows(IllegalArgumentException.class, () -> l2norm2.l2norm(dvs));
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
    }

    public void testSparseVectorFunctions() {
        testSparseVectorFunctions(Version.V_7_4_0);
        testSparseVectorFunctions(Version.CURRENT);
    }

    private void testSparseVectorFunctions(Version indexVersion) {
        int[] docVectorDims = {2, 10, 50, 113, 4545};
        float[] docVectorValues = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f};
        BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(
            indexVersion, docVectorDims, docVectorValues, docVectorDims.length);
        VectorScriptDocValues.SparseVectorScriptDocValues dvs = mock(VectorScriptDocValues.SparseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);
        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript._getIndexVersion()).thenReturn(indexVersion);

        Map<String, Number> queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("4545", -156.0);
        }};

        // test dotProduct
        DotProductSparse docProductSparse = new DotProductSparse(scoreScript, queryVector);
        double result = docProductSparse.dotProductSparse(dvs);
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.624, result, 0.001);

        // test cosineSimilarity
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(scoreScript, queryVector);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse(dvs);
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.790, result2, 0.001);

        // test l1norm
        L1NormSparse l1Norm = new L1NormSparse(scoreScript, queryVector);
        double result3 = l1Norm.l1normSparse(dvs);
        assertEquals("l1normSparse result is not equal to the expected value!", 485.184, result3, 0.001);

        // test l2norm
        L2NormSparse l2Norm = new L2NormSparse(scoreScript, queryVector);
        double result4 = l2Norm.l2normSparse(dvs);
        assertEquals("l2normSparse result is not equal to the expected value!", 301.361, result4, 0.001);
    }

    public void testSparseVectorMissingDimensions1() {
        // Document vector's biggest dimension > query vector's biggest dimension
        int[] docVectorDims = {2, 10, 50, 113, 4545, 4546};
        float[] docVectorValues = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f, 11.5f};
        BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(
            Version.CURRENT, docVectorDims, docVectorValues, docVectorDims.length);
        VectorScriptDocValues.SparseVectorScriptDocValues dvs = mock(VectorScriptDocValues.SparseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);
        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript._getIndexVersion()).thenReturn(Version.CURRENT);
        Map<String, Number> queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("114", -20.5);
            put("4545", -156.0);
        }};

        // test dotProduct
        DotProductSparse docProductSparse = new DotProductSparse(scoreScript, queryVector);
        double result = docProductSparse.dotProductSparse(dvs);
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.624, result, 0.001);

        // test cosineSimilarity
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(scoreScript, queryVector);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse(dvs);
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.786, result2, 0.001);

        // test l1norm
        L1NormSparse l1Norm = new L1NormSparse(scoreScript, queryVector);
        double result3 = l1Norm.l1normSparse(dvs);
        assertEquals("l1normSparse result is not equal to the expected value!", 517.184, result3, 0.001);

        // test l2norm
        L2NormSparse l2Norm = new L2NormSparse(scoreScript, queryVector);
        double result4 = l2Norm.l2normSparse(dvs);
        assertEquals("l2normSparse result is not equal to the expected value!", 302.277, result4, 0.001);
    }

    public void testSparseVectorMissingDimensions2() {
        // Document vector's biggest dimension < query vector's biggest dimension
        int[] docVectorDims = {2, 10, 50, 113, 4545, 4546};
        float[] docVectorValues = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f, 11.5f};
        BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(
            Version.CURRENT, docVectorDims, docVectorValues, docVectorDims.length);
        VectorScriptDocValues.SparseVectorScriptDocValues dvs = mock(VectorScriptDocValues.SparseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);
        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript._getIndexVersion()).thenReturn(Version.CURRENT);
        Map<String, Number> queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("4545", -156.0);
            put("4548", -20.5);
        }};

        // test dotProduct
        DotProductSparse docProductSparse = new DotProductSparse(scoreScript, queryVector);
        double result = docProductSparse.dotProductSparse(dvs);
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.624, result, 0.001);

        // test cosineSimilarity
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(scoreScript, queryVector);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse(dvs);
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.786, result2, 0.001);

        // test l1norm
        L1NormSparse l1Norm = new L1NormSparse(scoreScript, queryVector);
        double result3 = l1Norm.l1normSparse(dvs);
        assertEquals("l1normSparse result is not equal to the expected value!", 517.184, result3, 0.001);

        // test l2norm
        L2NormSparse l2Norm = new L2NormSparse(scoreScript, queryVector);
        double result4 = l2Norm.l2normSparse(dvs);
        assertEquals("l2normSparse result is not equal to the expected value!", 302.277, result4, 0.001);
    }
}
