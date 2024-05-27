/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;

import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TextEmbeddingByteResultsTests extends InferenceResultsTestCase<TextEmbeddingByteResults> {

    public static TextEmbeddingByteResults createRandomResults() {
        int columns = randomIntBetween(1, 10);
        var arr = new byte[columns];
        for (int i = 0; i < columns; i++) {
            arr[i] = randomByte();
        }

        return new TextEmbeddingByteResults(DEFAULT_RESULTS_FIELD, arr, randomBoolean());
    }

    @Override
    protected Writeable.Reader<TextEmbeddingByteResults> instanceReader() {
        return TextEmbeddingByteResults::new;
    }

    @Override
    protected TextEmbeddingByteResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected TextEmbeddingByteResults mutateInstance(TextEmbeddingByteResults instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testAsMap() {
        TextEmbeddingByteResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        int size = testInstance.isTruncated ? 2 : 1;
        assertThat(asMap.keySet(), hasSize(size));
        assertArrayEquals(testInstance.getInference(), (byte[]) asMap.get(DEFAULT_RESULTS_FIELD));
        if (testInstance.isTruncated) {
            assertThat(asMap.get("is_truncated"), is(true));
        }
    }

    @Override
    void assertFieldValues(TextEmbeddingByteResults createdInstance, IngestDocument document, String parentField, String resultsField) {
        assertArrayEquals(document.getFieldValue(parentField + resultsField, byte[].class), createdInstance.getInference());
    }
}
