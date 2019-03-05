/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.VectorEncoderDecoder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.index.query.ScoreScriptUtils.CosineSimilarity;
import org.elasticsearch.index.query.ScoreScriptUtils.DotProductSparse;
import org.elasticsearch.index.query.ScoreScriptUtils.CosineSimilaritySparse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.VectorEncoderDecoderTests.mockEncodeDenseVector;
import static org.elasticsearch.index.query.ScoreScriptUtils.dotProduct;
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
        assertEquals("dotProduct result is not equal to the expected value!", 65425.62, result, 0.1);

        // test cosineSimilarity
        CosineSimilarity cosineSimilarity = new CosineSimilarity(queryVector);
        double result2 = cosineSimilarity.cosineSimilarity(dvs);
        assertEquals("cosineSimilarity result is not equal to the expected value!", 0.78, result2, 0.1);
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
        assertEquals("dotProductSparse result is not equal to the expected value!", 65425.62, result, 0.1);

        // test cosineSimilarity
        CosineSimilaritySparse cosineSimilaritySparse = new CosineSimilaritySparse(queryVector);
        double result2 = cosineSimilaritySparse.cosineSimilaritySparse(dvs);
        assertEquals("cosineSimilaritySparse result is not equal to the expected value!", 0.78, result2, 0.1);
    }
}
