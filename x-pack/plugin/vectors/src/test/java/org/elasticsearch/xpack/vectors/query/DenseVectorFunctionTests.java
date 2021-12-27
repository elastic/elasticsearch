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
import org.elasticsearch.xpack.vectors.query.BinaryDenseVectorScriptDocValues.BinaryDenseVectorSupplier;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.CosineSimilarity;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.DotProduct;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L1Norm;
import org.elasticsearch.xpack.vectors.query.ScoreScriptUtils.L2Norm;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DenseVectorFunctionTests extends ESTestCase {

    public void testVectorFunctions() {
        String field = "vector";
        int dims = 5;
        float[] docVector = new float[] { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f };
        List<Number> queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);

        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BinaryDocValues docValues = BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, indexVersion);
            DenseVectorScriptDocValues scriptDocValues = new BinaryDenseVectorScriptDocValues(
                new BinaryDenseVectorSupplier(docValues),
                indexVersion,
                dims
            );

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.getDoc()).thenReturn(Collections.singletonMap(field, scriptDocValues));

            // Test cosine similarity explicitly, as it must perform special logic on top of the doc values
            CosineSimilarity function = new CosineSimilarity(scoreScript, queryVector, field);
            assertEquals("cosineSimilarity result is not equal to the expected value!", 0.790, function.cosineSimilarity(), 0.001);

            // Check each function rejects query vectors with the wrong dimension
            assertDimensionMismatch(() -> new DotProduct(scoreScript, invalidQueryVector, field));
            assertDimensionMismatch(() -> new CosineSimilarity(scoreScript, invalidQueryVector, field));
            assertDimensionMismatch(() -> new L1Norm(scoreScript, invalidQueryVector, field));
            assertDimensionMismatch(() -> new L2Norm(scoreScript, invalidQueryVector, field));
        }
    }

    private void assertDimensionMismatch(Supplier<ScoreScriptUtils.DenseVectorFunction> supplier) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, supplier::get);
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
    }
}
