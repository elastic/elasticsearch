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
import org.elasticsearch.xpack.vectors.mapper.SparseVectorFieldMapper;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilaritySparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProductSparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1NormSparse;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2NormSparse;
import org.elasticsearch.xpack.vectors.query.VectorScriptDocValues.SparseVectorScriptDocValues;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparseVectorFunctionTests extends ESTestCase {
    private String field;
    private int[] docVectorDims;
    private float[] docVectorValues;
    private Map<String, Number> queryVector;

    @Before
    public void setUpVectors() {
        field = "vector";
        docVectorDims = new int[] {2, 10, 50, 113, 4545};
        docVectorValues = new float[] {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f};
        queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("4545", -156.0);
        }};
    }

    public void testSparseVectorFunctions() {
        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(indexVersion,
                docVectorDims, docVectorValues, docVectorDims.length);
            SparseVectorScriptDocValues docValues = mock(SparseVectorScriptDocValues.class);
            when(docValues.getEncodedValue()).thenReturn(encodedDocVector);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript._getIndexVersion()).thenReturn(indexVersion);
            when(scoreScript.getDoc()).thenReturn(Collections.singletonMap(field, docValues));

            testDotProduct(docValues, scoreScript);
            testCosineSimilarity(docValues, scoreScript);
            testL1Norm(docValues, scoreScript);
            testL2Norm(docValues, scoreScript);
        }
    }

    private void testDotProduct(SparseVectorScriptDocValues docValues, ScoreScript scoreScript) {
        DotProductSparse function = new DotProductSparse(scoreScript, queryVector, field);
        double result = function.dotProductSparse();
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.624, result, 0.001);

        DotProductSparse deprecatedFunction = new DotProductSparse(scoreScript, queryVector, docValues);
        double deprecatedResult = deprecatedFunction.dotProductSparse();
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.624, deprecatedResult, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE, ScoreScriptUtils.DEPRECATION_MESSAGE);
    }

    private void testCosineSimilarity(SparseVectorScriptDocValues docValues, ScoreScript scoreScript) {
        CosineSimilaritySparse function = new CosineSimilaritySparse(scoreScript, queryVector, field);
        double result = function.cosineSimilaritySparse();
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.790, result, 0.001);

        CosineSimilaritySparse deprecatedFunction = new CosineSimilaritySparse(scoreScript, queryVector, docValues);
        double deprecatedResult = deprecatedFunction.cosineSimilaritySparse();
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.790, deprecatedResult, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE, ScoreScriptUtils.DEPRECATION_MESSAGE);
    }

    private void testL1Norm(SparseVectorScriptDocValues docValues, ScoreScript scoreScript) {
        L1NormSparse function = new L1NormSparse(scoreScript, queryVector, field);
        double result = function.l1normSparse();
        assertEquals("l1norm result is not equal to the expected value!", 485.184, result, 0.001);

        L1NormSparse deprecatedFunction = new L1NormSparse(scoreScript, queryVector, docValues);
        double deprecatedResult = deprecatedFunction.l1normSparse();
        assertEquals("l1norm result is not equal to the expected value!", 485.184, deprecatedResult, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE, ScoreScriptUtils.DEPRECATION_MESSAGE);
    }

    private void testL2Norm(SparseVectorScriptDocValues docValues, ScoreScript scoreScript) {
        L2NormSparse function = new L2NormSparse(scoreScript, queryVector, field);
        double result = function.l2normSparse();
        assertEquals("L2NormSparse result is not equal to the expected value!", 301.361, result, 0.001);

        L2NormSparse deprecatedFunction = new L2NormSparse(scoreScript, queryVector, docValues);
        double deprecatedResult = deprecatedFunction.l2normSparse();
        assertEquals("L2NormSparse result is not equal to the expected value!", 301.361, deprecatedResult, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE, ScoreScriptUtils.DEPRECATION_MESSAGE);
    }

    public void testSparseVectorMissingDimensions1() {
        String field = "vector";

        // Document vector's biggest dimension > query vector's biggest dimension
        int[] docVectorDims = {2, 10, 50, 113, 4545, 4546};
        float[] docVectorValues = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f, 11.5f};
        BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(
            Version.CURRENT, docVectorDims, docVectorValues, docVectorDims.length);
        VectorScriptDocValues.SparseVectorScriptDocValues dvs = mock(VectorScriptDocValues.SparseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript._getIndexVersion()).thenReturn(Version.CURRENT);
        when(scoreScript.getDoc()).thenReturn(Collections.singletonMap(field, dvs));

        Map<String, Number> queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("114", -20.5);
            put("4545", -156.0);
        }};

        // test dotProductSparse
        DotProductSparse docProductSparse = new DotProductSparse(scoreScript, queryVector, field);
        double result = docProductSparse.dotProductSparse();
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.624, result, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE);

        // test cosineSimilaritySparse
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(scoreScript, queryVector, field);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse();
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.786, result2, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE);

        // test l1norm
        L1NormSparse l1Norm = new L1NormSparse(scoreScript, queryVector, field);
        double result3 = l1Norm.l1normSparse();
        assertEquals("l1normSparse result is not equal to the expected value!", 517.184, result3, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE);

        // test L2NormSparse
        L2NormSparse L2NormSparse = new L2NormSparse(scoreScript, queryVector, field);
        double result4 = L2NormSparse.l2normSparse();
        assertEquals("L2NormSparse result is not equal to the expected value!", 302.277, result4, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE);
    }

    public void testSparseVectorMissingDimensions2() {
        String field = "vector";

        // Document vector's biggest dimension < query vector's biggest dimension
        int[] docVectorDims = {2, 10, 50, 113, 4545, 4546};
        float[] docVectorValues = {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f, 11.5f};
        BytesRef encodedDocVector = VectorEncoderDecoder.encodeSparseVector(
            Version.CURRENT, docVectorDims, docVectorValues, docVectorDims.length);
        VectorScriptDocValues.SparseVectorScriptDocValues dvs = mock(VectorScriptDocValues.SparseVectorScriptDocValues.class);
        when(dvs.getEncodedValue()).thenReturn(encodedDocVector);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript._getIndexVersion()).thenReturn(Version.CURRENT);
        when(scoreScript.getDoc()).thenReturn(Collections.singletonMap(field, dvs));

        Map<String, Number> queryVector = new HashMap<String, Number>() {{
            put("2", 0.5);
            put("10", 111.3);
            put("50", -13.0);
            put("113", 14.8);
            put("4545", -156.0);
            put("4548", -20.5);
        }};

        // test dotProductSparse
        DotProductSparse docProductSparse = new DotProductSparse(scoreScript, queryVector, field);
        double result = docProductSparse.dotProductSparse();
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.624, result, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE);

        // test cosineSimilaritySparse
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(scoreScript, queryVector, field);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse();
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.786, result2, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE);

        // test l1norm
        L1NormSparse l1Norm = new L1NormSparse(scoreScript, queryVector, field);
        double result3 = l1Norm.l1normSparse();
        assertEquals("l1normSparse result is not equal to the expected value!", 517.184, result3, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE);

        // test L2NormSparse
        L2NormSparse L2NormSparse = new L2NormSparse(scoreScript, queryVector, field);
        double result4 = L2NormSparse.l2normSparse();
        assertEquals("L2NormSparse result is not equal to the expected value!", 302.277, result4, 0.001);
        assertWarnings(SparseVectorFieldMapper.DEPRECATION_MESSAGE);
    }
}
