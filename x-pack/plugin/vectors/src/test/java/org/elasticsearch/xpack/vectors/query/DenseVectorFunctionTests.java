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
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProduct;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1Norm;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2Norm;
import org.junit.Before;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DenseVectorFunctionTests extends ESTestCase {
    private String field;
    private float[] docVector;
    private List<Number> queryVector;
    private List<Number> invalidQueryVector;

    @Before
    public void setUpVectors() {
        field = "vector";
        docVector = new float[] {230.0f, 300.33f, -34.8988f, 15.555f, -200.0f};
        queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);
        invalidQueryVector = Arrays.asList(0.5, 111.3);
    }

    public void testVectorFunctions() {
        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BytesRef encodedDocVector = mockEncodeDenseVector(docVector, indexVersion);
            DenseVectorScriptDocValues docValues = mock(DenseVectorScriptDocValues.class);
            when(docValues.getEncodedValue()).thenReturn(encodedDocVector);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript._getIndexVersion()).thenReturn(indexVersion);
            when(scoreScript.getDoc()).thenReturn(Collections.singletonMap(field, docValues));
            
            testDotProduct(scoreScript);
            testCosineSimilarity(scoreScript);
            testL1Norm(scoreScript);
            testL2Norm(scoreScript);
        }
    }
    
    private void testDotProduct(ScoreScript scoreScript) {
        DotProduct function = new DotProduct(scoreScript, queryVector, field);
        double result = function.dotProduct();
        assertEquals("dotProduct result is not equal to the expected value!", 65425.624, result, 0.001);

        DotProduct invalidFunction = new DotProduct(scoreScript, invalidQueryVector, field);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, invalidFunction::dotProduct);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
    }
    
    private void testCosineSimilarity(ScoreScript scoreScript) {
        CosineSimilarity function = new CosineSimilarity(scoreScript, queryVector, field);
        double result = function.cosineSimilarity();
        assertEquals("cosineSimilarity result is not equal to the expected value!", 0.790, result, 0.001);

        CosineSimilarity invalidFunction = new CosineSimilarity(scoreScript, invalidQueryVector, field);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, invalidFunction::cosineSimilarity);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
    }

    private void testL1Norm(ScoreScript scoreScript) {
        L1Norm function = new L1Norm(scoreScript, queryVector, field);
        double result = function.l1norm();
        assertEquals("l1norm result is not equal to the expected value!", 485.184, result, 0.001);

        L1Norm invalidFunction = new L1Norm(scoreScript, invalidQueryVector, field);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, invalidFunction::l1norm);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
    }

    private void testL2Norm(ScoreScript scoreScript) {
        L2Norm function = new L2Norm(scoreScript, queryVector, field);
        double result = function.l2norm();
        assertEquals("l2norm result is not equal to the expected value!", 301.361, result, 0.001);

        L2Norm invalidFunction = new L2Norm(scoreScript, invalidQueryVector, field);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, invalidFunction::l2norm);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
    }

    private static BytesRef mockEncodeDenseVector(float[] values, Version indexVersion) {
        byte[] bytes = indexVersion.onOrAfter(Version.V_7_5_0)
            ? new byte[VectorEncoderDecoder.INT_BYTES * values.length + VectorEncoderDecoder.INT_BYTES]
            : new byte[VectorEncoderDecoder.INT_BYTES * values.length];
        double dotProduct = 0f;

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        for (float value : values) {
            byteBuffer.putFloat(value);
            dotProduct += value * value;
        }

        if (indexVersion.onOrAfter(Version.V_7_5_0)) {
            // encode vector magnitude at the end
            float vectorMagnitude = (float) Math.sqrt(dotProduct);
            byteBuffer.putFloat(vectorMagnitude);
        }
        return new BytesRef(bytes);
    }
}
