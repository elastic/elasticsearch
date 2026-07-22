/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;

/**
 * Integration tests verifying that the {@code _inference_fields} metadata field is correctly
 * included or excluded from {@code _source} for the {@code semantic} field type.
 * This test covers text and image modalities.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class SemanticFieldInferenceFieldsIT extends AbstractInferenceFieldsIT {
    private static final Map<String, Object> EMBEDDING_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    // A small JPEG encoded as a base64 data URI for use as a multimodal input.
    private static final Map<String, String> IMAGE_INPUT = Map.of(
        "type",
        "image",
        "value",
        "data:image/jpeg;base64,Y2F0IG9uIGEgd2luZG93c2lsbA=="
    );

    public void testExcludeInferenceFieldsFromSourceWithMixedInputs() throws Exception {
        final String semanticFieldName = randomIdentifier();
        final String embeddingInferenceId = randomIdentifier();
        IntegrationTestUtils.createInferenceEndpoint(client(), TaskType.EMBEDDING, embeddingInferenceId, EMBEDDING_SERVICE_SETTINGS);
        final var fieldMap = Map.of(semanticFieldName, embeddingInferenceId);

        testExcludeInferenceFieldsFromSource(
            () -> IntegrationTestUtils.generateSemanticFieldMapping(fieldMap),
            fieldMap.keySet(),
            IndexVersion.current(),
            IndexVersion.current(),
            20
        );

        IntegrationTestUtils.deleteInferenceEndpoint(client(), TaskType.EMBEDDING, embeddingInferenceId);
    }

    @Override
    protected void indexDocuments(String indexName, String field, int count) {
        for (int i = 0; i < count; i++) {
            Map<String, Object> source = Map.of(field, randomBoolean() ? randomAlphaOfLength(10) : IMAGE_INPUT);
            DocWriteResponse response = client().prepareIndex(indexName).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(response.getResult(), is(DocWriteResponse.Result.CREATED));
        }
        client().admin().indices().prepareRefresh(indexName).get();
    }
}
