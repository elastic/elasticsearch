/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilarity;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProductSparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilaritySparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1NormSparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2NormSparse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoderTests.mockEncodeDenseVector;
import static org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.dotProduct;
import static org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.l1norm;
import static org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.l2norm;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ScoreScriptUtilsTests extends ESTestCase {
    public void testDenseVectorFunctions() {
        float[] docVector = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f};
        BytesRef encodedDocVector =  mockEncodeDenseVector(docVector);
        VectorScriptDocValues.DenseVectorScriptDocValues dvs = mock(VectorScriptDocValues.DenseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);
        List<Number> queryVector = Arrays.asList(0.5, 111.3, -13.0, 14.8, -156.0);

        // test dotProduct
        double result = dotProduct(queryVector, dvs);
        assertEquals("dotProduct result is not equal to the expected value!", 65425.626, result, 0.001);

        // test cosineSimilarity
        CosineSimilarity cosineSimilarity = new CosineSimilarity(queryVector);
        double result2 = cosineSimilarity.cosineSimilarity(dvs);
        assertEquals("cosineSimilarity result is not equal to the expected value!", 0.790, result2, 0.001);

        // test l1Norm
        double result3 = l1norm(queryVector, dvs);
        assertEquals("l1norm result is not equal to the expected value!", 485.184, result3, 0.001);

        // test l2norm
        double result4 = l2norm(queryVector, dvs);
        assertEquals("l2norm result is not equal to the expected value!", 301.361, result4, 0.001);

        // test dotProduct fails when queryVector has wrong number of dims
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> dotProduct(invalidQueryVector, dvs));
        assertThat(e.getMessage(), containsString("dimensions of the query vector [2] is different from the documents' vectors [5]"));

        // test cosineSimilarity fails when queryVector has wrong number of dims
        CosineSimilarity cosineSimilarity2 = new CosineSimilarity(invalidQueryVector);
        e = expectThrows(IllegalArgumentException.class, () -> cosineSimilarity2.cosineSimilarity(dvs));
        assertThat(e.getMessage(), containsString("dimensions of the query vector [2] is different from the documents' vectors [5]"));

        // test l1norm fails when queryVector has wrong number of dims
        e = expectThrows(IllegalArgumentException.class, () -> l1norm(invalidQueryVector, dvs));
        assertThat(e.getMessage(), containsString("dimensions of the query vector [2] is different from the documents' vectors [5]"));

        // test l2norm fails when queryVector has wrong number of dims
        e = expectThrows(IllegalArgumentException.class, () -> l2norm(invalidQueryVector, dvs));
        assertThat(e.getMessage(), containsString("dimensions of the query vector [2] is different from the documents' vectors [5]"));
    }

    public void testSparseVectorFunctions() {
        int[] docVectorDims = {2, 10, 50, 113, 4545};
        float[] docVectorValues = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f};
        BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(docVectorDims, docVectorValues, docVectorDims.length);
        VectorScriptDocValues.SparseVectorScriptDocValues dvs = mock(VectorScriptDocValues.SparseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);
        Map<String, Number> queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("4545", -156.0);
        }};

        // test dotProduct
        DotProductSparse docProductSparse = new DotProductSparse(queryVector);
        double result = docProductSparse.dotProductSparse(dvs);
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.626, result, 0.001);

        // test cosineSimilarity
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(queryVector);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse(dvs);
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.790, result2, 0.001);

        // test l1norm
        L1NormSparse l1Norm = new L1NormSparse(queryVector);
        double result3 = l1Norm.l1normSparse(dvs);
        assertEquals("l1normSparse result is not equal to the expected value!", 485.184, result3, 0.001);

        // test l2norm
        L2NormSparse l2Norm = new L2NormSparse(queryVector);
        double result4 = l2Norm.l2normSparse(dvs);
        assertEquals("l2normSparse result is not equal to the expected value!", 301.361, result4, 0.001);
    }

    public void testSparseVectorMissingDimensions1() {
        // Document vector's biggest dimension > query vector's biggest dimension
        int[] docVectorDims = {2, 10, 50, 113, 4545, 4546};
        float[] docVectorValues = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f, 11.5f};
        BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(docVectorDims, docVectorValues, docVectorDims.length);
        VectorScriptDocValues.SparseVectorScriptDocValues dvs = mock(VectorScriptDocValues.SparseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);
        Map<String, Number> queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("114", -20.5);
            put("4545", -156.0);
        }};

        // test dotProduct
        DotProductSparse docProductSparse = new DotProductSparse(queryVector);
        double result = docProductSparse.dotProductSparse(dvs);
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.626, result, 0.001);

        // test cosineSimilarity
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(queryVector);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse(dvs);
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.786, result2, 0.001);

        // test l1norm
        L1NormSparse l1Norm = new L1NormSparse(queryVector);
        double result3 = l1Norm.l1normSparse(dvs);
        assertEquals("l1normSparse result is not equal to the expected value!", 517.184, result3, 0.001);

        // test l2norm
        L2NormSparse l2Norm = new L2NormSparse(queryVector);
        double result4 = l2Norm.l2normSparse(dvs);
        assertEquals("l2normSparse result is not equal to the expected value!", 302.277, result4, 0.001);
    }

    public void testSparseVectorMissingDimensions2() {
        // Document vector's biggest dimension < query vector's biggest dimension
        int[] docVectorDims = {2, 10, 50, 113, 4545, 4546};
        float[] docVectorValues = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f, 11.5f};
        BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(docVectorDims, docVectorValues, docVectorDims.length);
        VectorScriptDocValues.SparseVectorScriptDocValues dvs = mock(VectorScriptDocValues.SparseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);
        Map<String, Number> queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("4545", -156.0);
            put("4548", -20.5);
        }};

        // test dotProduct
        DotProductSparse docProductSparse = new DotProductSparse(queryVector);
        double result = docProductSparse.dotProductSparse(dvs);
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.626, result, 0.001);

        // test cosineSimilarity
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(queryVector);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse(dvs);
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.786, result2, 0.001);

        // test l1norm
        L1NormSparse l1Norm = new L1NormSparse(queryVector);
        double result3 = l1Norm.l1normSparse(dvs);
        assertEquals("l1normSparse result is not equal to the expected value!", 517.184, result3, 0.001);

        // test l2norm
        L2NormSparse l2Norm = new L2NormSparse(queryVector);
        double result4 = l2Norm.l2normSparse(dvs);
        assertEquals("l2normSparse result is not equal to the expected value!", 302.277, result4, 0.001);
    }
}
