/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ErrorInferenceResultsTests extends InferenceResultsTestCase<ErrorInferenceResults> {

    @Override
    protected Writeable.Reader<ErrorInferenceResults> instanceReader() {
        return ErrorInferenceResults::new;
    }

    @Override
    protected ErrorInferenceResults createTestInstance() {
        return new ErrorInferenceResults(new ElasticsearchStatusException(randomAlphaOfLength(8), randomFrom(RestStatus.values())));
    }

    @Override
    protected ErrorInferenceResults mutateInstance(ErrorInferenceResults instance) throws IOException {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    void assertFieldValues(ErrorInferenceResults createdInstance, IngestDocument document, String parentField, String resultsField) {
        assertThat(document.getFieldValue(parentField + "error", String.class), equalTo(createdInstance.getException().getMessage()));
    }
}
