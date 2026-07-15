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
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class SemanticTextInferenceFieldsIT extends AbstractInferenceFieldsIT {
    private final String indexName = randomIdentifier();
    private final Map<String, TaskType> inferenceIds = new HashMap<>();

    private static final Map<String, Object> SPARSE_EMBEDDING_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");
    private static final Map<String, Object> TEXT_EMBEDDING_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    @After
    public void cleanUp() {
        IntegrationTestUtils.deleteIndex(client(), indexName);
        for (var entry : inferenceIds.entrySet()) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), entry.getValue(), entry.getKey());
        }
    }

    public void testExcludeInferenceFieldsFromSource() throws Exception {
        excludeInferenceFieldsFromSourceTestCase(IndexVersion.current(), IndexVersion.current(), 10);
    }

    public void testExcludeInferenceFieldsFromSourceOldIndexVersions() throws Exception {
        excludeInferenceFieldsFromSourceTestCase(
            IndexVersions.SEMANTIC_TEXT_FIELD_TYPE,
            IndexVersionUtils.getPreviousVersion(IndexVersion.current()),
            40
        );
    }

    private void excludeInferenceFieldsFromSourceTestCase(IndexVersion minIndexVersion, IndexVersion maxIndexVersion, int iterations)
        throws Exception {
        final String sparseEmbeddingInferenceId = randomIdentifier();
        final String textEmbeddingInferenceId = randomIdentifier();
        createInferenceEndpoint(TaskType.SPARSE_EMBEDDING, sparseEmbeddingInferenceId, SPARSE_EMBEDDING_SERVICE_SETTINGS);
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, textEmbeddingInferenceId, TEXT_EMBEDDING_SERVICE_SETTINGS);

        final String sparseEmbeddingField = randomIdentifier();
        final String textEmbeddingField = randomIdentifier();

        for (int i = 0; i < iterations; i++) {
            final IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(minIndexVersion, maxIndexVersion);
            final Settings indexSettings = generateIndexSettings(indexVersion);
            XContentBuilder mappings = IntegrationTestUtils.generateSemanticTextMapping(
                Map.of(sparseEmbeddingField, sparseEmbeddingInferenceId, textEmbeddingField, textEmbeddingInferenceId)
            );
            assertAcked(prepareCreate(indexName).setSettings(indexSettings).setMapping(mappings));

            final int docCount = randomIntBetween(10, 50);
            indexDocuments(sparseEmbeddingField, docCount);
            indexDocuments(textEmbeddingField, docCount);

            assertSearchResponse(
                indexName,
                new SemanticQueryBuilder(sparseEmbeddingField, randomAlphaOfLength(10)),
                indexSettings,
                docCount,
                request -> {
                    request.source().fetchSource(generateRandomFetchSourceContext()).fetchField(sparseEmbeddingField);
                },
                response -> {
                    for (SearchHit hit : response.getHits()) {
                        Map<String, DocumentField> documentFields = hit.getDocumentFields();
                        assertThat(documentFields.size(), is(1));
                        assertThat(documentFields.containsKey(sparseEmbeddingField), is(true));
                    }
                }
            );

            assertSearchResponse(
                indexName,
                new SemanticQueryBuilder(textEmbeddingField, randomAlphaOfLength(10)),
                indexSettings,
                docCount,
                request -> {
                    request.source().fetchSource(generateRandomFetchSourceContext()).fetchField(textEmbeddingField);
                },
                response -> {
                    for (SearchHit hit : response.getHits()) {
                        Map<String, DocumentField> documentFields = hit.getDocumentFields();
                        assertThat(documentFields.size(), is(1));
                        assertThat(documentFields.containsKey(textEmbeddingField), is(true));
                    }
                }
            );

            IntegrationTestUtils.deleteIndex(client(), indexName);
        }
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
    }

    private void indexDocuments(String field, int count) {
        for (int i = 0; i < count; i++) {
            Map<String, Object> source = Map.of(field, randomAlphaOfLength(10));
            DocWriteResponse response = client().prepareIndex(indexName).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(response.getResult(), is(DocWriteResponse.Result.CREATED));
        }

        client().admin().indices().prepareRefresh(indexName).get();
    }
}
