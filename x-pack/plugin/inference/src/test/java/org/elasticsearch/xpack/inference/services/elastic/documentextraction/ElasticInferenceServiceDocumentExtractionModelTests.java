/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceDocumentExtractionModelTests extends ESTestCase {

    public void testUriCreation() {
        var model = createModel("http://eis-gateway.com", "my-model-id");

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/document-extraction"));
    }

    public void testUriCreation_WithTrailingSlash() {
        var model = createModel("http://eis-gateway.com/", "my-model-id");

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/document-extraction"));
    }

    public static ElasticInferenceServiceDocumentExtractionModel createModel(String url, String modelId) {
        return new ElasticInferenceServiceDocumentExtractionModel(
            "id",
            TaskType.DOCUMENT_EXTRACTION,
            new ElasticInferenceServiceDocumentExtractionServiceSettings(modelId),
            ElasticInferenceServiceComponents.of(url)
        );
    }

}
