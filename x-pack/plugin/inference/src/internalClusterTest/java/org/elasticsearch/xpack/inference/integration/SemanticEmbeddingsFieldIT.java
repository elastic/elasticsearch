/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

public class SemanticEmbeddingsFieldIT extends AbstractEmbeddingsFieldIT {
    private static final String INFERENCE_ID = "embedding-test-endpoint";

    @Before
    public void setUpInferenceEndpoint() throws IOException {
        createInferenceEndpoint(TaskType.EMBEDDING, INFERENCE_ID);
    }

    @Override
    Map<String, String> getFields() {
        return Map.of("embedding_field", INFERENCE_ID);
    }

    @Override
    XContentBuilder generateMapping(Map<String, String> fieldNameToInferenceIdMap) throws IOException {
        return IntegrationTestUtils.generateSemanticMapping(fieldNameToInferenceIdMap);
    }
}
