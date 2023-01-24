/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;

import static org.hamcrest.Matchers.equalTo;

public class TextSimilarityInferenceResultsTests extends InferenceResultsTestCase<TextSimilarityInferenceResults> {

    public static TextSimilarityInferenceResults createRandomResults() {
        return new TextSimilarityInferenceResults(randomAlphaOfLength(10), randomDoubleBetween(0.0, 1.0, false), randomBoolean());
    }

    @Override
    protected TextSimilarityInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<TextSimilarityInferenceResults> instanceReader() {
        return TextSimilarityInferenceResults::new;
    }

    @Override
    void assertFieldValues(TextSimilarityInferenceResults createdInstance, IngestDocument document, String resultsField) {
        String path = resultsField + "." + createdInstance.getResultsField();
        assertThat(document.getFieldValue(path, Double.class), equalTo(createdInstance.predictedValue()));
    }
}
