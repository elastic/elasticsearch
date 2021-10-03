/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.hamcrest.Matchers.hasSize;

public class TextEmbeddingResultsTests extends AbstractWireSerializingTestCase<TextEmbeddingResults> {
    @Override
    protected Writeable.Reader<TextEmbeddingResults> instanceReader() {
        return TextEmbeddingResults::new;
    }

    @Override
    protected TextEmbeddingResults createTestInstance() {
        int columns = randomIntBetween(1, 10);
        double[] arr = new double[columns];
        for (int i=0; i<columns; i++) {
            arr[i] = randomDouble();
        }

        return new TextEmbeddingResults(DEFAULT_RESULTS_FIELD, arr);
    }

    public void testAsMap() {
        TextEmbeddingResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        assertThat(asMap.keySet(), hasSize(1));
        assertArrayEquals(testInstance.getInference(), (double[]) asMap.get(DEFAULT_RESULTS_FIELD), 1e-10);
    }
}
