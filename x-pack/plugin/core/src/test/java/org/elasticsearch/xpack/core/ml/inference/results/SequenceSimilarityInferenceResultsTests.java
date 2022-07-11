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

public class SequenceSimilarityInferenceResultsTests extends InferenceResultsTestCase<SequenceSimilarityInferenceResults> {

    public static SequenceSimilarityInferenceResults createRandomResults() {
        return new SequenceSimilarityInferenceResults(randomAlphaOfLength(10), randomDoubleBetween(0.0, 1.0, false), randomBoolean());
    }

    @Override
    protected SequenceSimilarityInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<SequenceSimilarityInferenceResults> instanceReader() {
        return SequenceSimilarityInferenceResults::new;
    }

    @Override
    void assertFieldValues(SequenceSimilarityInferenceResults createdInstance, IngestDocument document, String resultsField) {
        String path = resultsField + "." + createdInstance.getResultsField();
        assertThat(document.getFieldValue(path, Double.class), equalTo(createdInstance.predictedValue()));
    }
}
