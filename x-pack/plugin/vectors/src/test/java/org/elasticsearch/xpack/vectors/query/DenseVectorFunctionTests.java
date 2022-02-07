/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.elasticsearch.Version;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilarity;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProduct;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1Norm;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2Norm;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DenseVectorFunctionTests extends ESTestCase {

    public void testVectorFunctions() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[] docVector = new float[] { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f };
        List<Number> queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);

        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BinaryDocValues docValues = BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, indexVersion);
            DenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(docValues, "test", dims, indexVersion);
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field("vector")).thenAnswer(mock -> field);

            // Test cosine similarity explicitly, as it must perform special logic on top of the doc values
            CosineSimilarity function = new CosineSimilarity(scoreScript, queryVector, fieldName);
            assertEquals("cosineSimilarity result is not equal to the expected value!", 0.790, function.cosineSimilarity(), 0.001);

            // Check each function rejects query vectors with the wrong dimension
            assertDimensionMismatch(() -> new DotProduct(scoreScript, invalidQueryVector, fieldName));
            assertDimensionMismatch(() -> new CosineSimilarity(scoreScript, invalidQueryVector, fieldName));
            assertDimensionMismatch(() -> new L1Norm(scoreScript, invalidQueryVector, fieldName));
            assertDimensionMismatch(() -> new L2Norm(scoreScript, invalidQueryVector, fieldName));

            // Check scripting infrastructure integration
            DotProduct dotProduct = new DotProduct(scoreScript, queryVector, fieldName);
            assertEquals(65425.6249, dotProduct.dotProduct(), 0.001);
            assertEquals(485.1837, new L1Norm(scoreScript, queryVector, fieldName).l1norm(), 0.001);
            assertEquals(301.3614, new L2Norm(scoreScript, queryVector, fieldName).l2norm(), 0.001);
            when(scoreScript._getDocId()).thenReturn(1);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, dotProduct::dotProduct);
            assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
        }

    }

    private void assertDimensionMismatch(Supplier<ScoreScriptUtils.DenseVectorFunction> supplier) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, supplier::get);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
    }
}
