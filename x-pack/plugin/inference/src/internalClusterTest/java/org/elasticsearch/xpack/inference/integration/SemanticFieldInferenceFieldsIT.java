/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.is;

/**
 * Integration tests verifying that the {@code _inference_fields} metadata field is correctly
 * included or excluded from {@code _source} for the {@code semantic} field type.
 * This test covers text and image modalities.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class SemanticFieldInferenceFieldsIT extends AbstractInferenceFieldsIT {
    private final String indexName = randomIdentifier();
    private final Map<String, TaskType> inferenceIds = new HashMap<>();

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

    @After
    public void cleanUp() {
        IntegrationTestUtils.deleteIndex(client(), indexName);
        for (var entry : inferenceIds.entrySet()) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), entry.getValue(), entry.getKey());
        }
    }

    public void testExcludeInferenceFieldsFromSourceWithTextInputs() throws Exception {
        final String embeddingInferenceId = randomIdentifier();
        createInferenceEndpoint(TaskType.EMBEDDING, embeddingInferenceId, EMBEDDING_SERVICE_SETTINGS);
        final String semanticField = randomIdentifier();

        for (int i = 0; i < 10; i++) {
            final Settings indexSettings = generateIndexSettings(IndexVersion.current());
            XContentBuilder mappings = IntegrationTestUtils.generateSemanticFieldMapping(Map.of(semanticField, embeddingInferenceId));
            assertAcked(prepareCreate(indexName).setSettings(indexSettings).setMapping(mappings));

            final int docCount = randomIntBetween(10, 50);
            indexTextDocuments(semanticField, docCount);

            assertSearchResponse(indexName, matchAllQuery(), indexSettings, docCount, request -> {
                request.source().fetchSource(generateRandomFetchSourceContext()).fetchField(semanticField);
            }, response -> {
                for (SearchHit hit : response.getHits()) {
                    Map<String, DocumentField> documentFields = hit.getDocumentFields();
                    assertThat(documentFields.size(), is(1));
                    assertThat(documentFields.containsKey(semanticField), is(true));
                }
            });

            IntegrationTestUtils.deleteIndex(client(), indexName);
        }
    }

    public void testExcludeInferenceFieldsFromSourceWithImageInputs() throws Exception {
        final String embeddingInferenceId = randomIdentifier();
        createInferenceEndpoint(TaskType.EMBEDDING, embeddingInferenceId, EMBEDDING_SERVICE_SETTINGS);
        final String semanticField = randomIdentifier();

        for (int i = 0; i < 10; i++) {
            final Settings indexSettings = generateIndexSettings(IndexVersion.current());
            XContentBuilder mappings = IntegrationTestUtils.generateSemanticFieldMapping(Map.of(semanticField, embeddingInferenceId));
            assertAcked(prepareCreate(indexName).setSettings(indexSettings).setMapping(mappings));

            final int docCount = randomIntBetween(10, 50);
            indexImageDocuments(semanticField, docCount);

            assertSearchResponse(indexName, matchAllQuery(), indexSettings, docCount, request -> {
                request.source().fetchSource(generateRandomFetchSourceContext()).fetchField(semanticField);
            }, response -> {
                for (SearchHit hit : response.getHits()) {
                    Map<String, DocumentField> documentFields = hit.getDocumentFields();
                    assertThat(documentFields.size(), is(1));
                    assertThat(documentFields.containsKey(semanticField), is(true));
                }
            });

            IntegrationTestUtils.deleteIndex(client(), indexName);
        }
    }

    public void testExcludeInferenceFieldsFromSourceWithMixedInputs() throws Exception {
        final String embeddingInferenceId = randomIdentifier();
        createInferenceEndpoint(TaskType.EMBEDDING, embeddingInferenceId, EMBEDDING_SERVICE_SETTINGS);
        final String semanticField = randomIdentifier();

        for (int i = 0; i < 10; i++) {
            final Settings indexSettings = generateIndexSettings(IndexVersion.current());
            XContentBuilder mappings = IntegrationTestUtils.generateSemanticFieldMapping(Map.of(semanticField, embeddingInferenceId));
            assertAcked(prepareCreate(indexName).setSettings(indexSettings).setMapping(mappings));

            final int textDocCount = randomIntBetween(5, 25);
            final int imageDocCount = randomIntBetween(5, 25);
            indexTextDocuments(semanticField, textDocCount);
            indexImageDocuments(semanticField, imageDocCount);
            final int totalDocCount = textDocCount + imageDocCount;

            assertSearchResponse(indexName, matchAllQuery(), indexSettings, totalDocCount, request -> {
                request.source().fetchSource(generateRandomFetchSourceContext()).fetchField(semanticField);
            }, response -> {
                for (SearchHit hit : response.getHits()) {
                    Map<String, DocumentField> documentFields = hit.getDocumentFields();
                    assertThat(documentFields.size(), is(1));
                    assertThat(documentFields.containsKey(semanticField), is(true));
                }
            });

            IntegrationTestUtils.deleteIndex(client(), indexName);
        }
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
    }

    private void indexTextDocuments(String field, int count) {
        for (int i = 0; i < count; i++) {
            Map<String, Object> source = Map.of(field, randomAlphaOfLength(10));
            DocWriteResponse response = client().prepareIndex(indexName).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(response.getResult(), is(DocWriteResponse.Result.CREATED));
        }
        client().admin().indices().prepareRefresh(indexName).get();
    }

    private void indexImageDocuments(String field, int count) {
        for (int i = 0; i < count; i++) {
            // Unlike text, image inputs are object values {type, value}
            Map<String, Object> source = Map.of(field, IMAGE_INPUT);
            DocWriteResponse response = client().prepareIndex(indexName).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(response.getResult(), is(DocWriteResponse.Result.CREATED));
        }
        client().admin().indices().prepareRefresh(indexName).get();
    }
}
