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

public class WarningInferenceResultsTests extends InferenceResultsTestCase<WarningInferenceResults> {

    public static WarningInferenceResults createRandomResults() {
        return new WarningInferenceResults(randomAlphaOfLength(10));
    }

    @Override
    protected WarningInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected WarningInferenceResults mutateInstance(WarningInferenceResults instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<WarningInferenceResults> instanceReader() {
        return WarningInferenceResults::new;
    }

    @Override
    void assertFieldValues(WarningInferenceResults createdInstance, IngestDocument document, String resultsField) {
        assertThat(document.getFieldValue(resultsField + ".warning", String.class), equalTo(createdInstance.getWarning()));
    }
}
