/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.EmbeddingsField;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 1, supportsDedicatedMasters = false)
abstract class AbstractEmbeddingsFieldIT extends ESIntegTestCase {
    private static final Map<String, Object> DENSE_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        4,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );
    private static final Map<String, Object> SPARSE_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");

    String indexName = null;
    final Map<String, TaskType> inferenceIds = new HashMap<>();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @After
    private void cleanUp() {
        if (indexName != null) {
            IntegrationTestUtils.deleteIndex(client(), indexName);
        }

        for (var entry : inferenceIds.entrySet()) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), entry.getValue(), entry.getKey());
        }
        inferenceIds.clear();
    }

    abstract Map<String, String> getFields();

    abstract XContentBuilder generateMapping(Map<String, String> fieldNameToInferenceIdMap) throws IOException;

    public void testFetchEmbeddingsFields() throws Exception {
        indexName = randomIndexName();
        final Map<String, String> fields = getFields();

        assertAcked(
            prepareCreate(indexName).setMapping(generateMapping(fields))
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, cluster().numDataNodes()).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );

        BulkRequestBuilder bulk = client().prepareBulk(indexName);
        Map<String, Object> source = new HashMap<>();
        for (String fieldName : fields.keySet()) {
            source.put(fieldName, randomAlphaOfLengthBetween(5, 10));
        }
        bulk.add(client().prepareIndex(indexName).setSource(source));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.get(TEST_REQUEST_TIMEOUT);
        ensureGreen(indexName);

        for (var entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            String inferenceId = entry.getValue();
            VectorType expectedVectorType = getExpectedVectorType(inferenceId);

            assertEmbeddingsFields(
                indexName,
                List.of(new EmbeddingsField(fieldName, null)),
                List.of(Map.of(fieldName, expectedVectorType))
            );
            assertEmbeddingsFields(
                indexName,
                List.of(new EmbeddingsField(fieldName, expectedVectorType)),
                List.of(Map.of(fieldName, expectedVectorType))
            );
            assertEmbeddingsFields(
                indexName,
                List.of(new EmbeddingsField(fieldName, randomValueOtherThan(expectedVectorType, () -> randomFrom(VectorType.values())))),
                List.of(Map.of())
            );
        }

        if (fields.size() > 1) {
            List<EmbeddingsField> embeddingsFields = new ArrayList<>();
            Map<String, VectorType> expectedFields = new HashMap<>();
            for (var entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                VectorType expectedVectorType = getExpectedVectorType(entry.getValue());

                embeddingsFields.add(new EmbeddingsField(fieldName, null));
                expectedFields.put(fieldName, expectedVectorType);
            }

            assertEmbeddingsFields(indexName, embeddingsFields, List.of(expectedFields));
        }
    }

    public void testFetchEmbeddingsFieldsNoDocuments() throws Exception {
        indexName = randomIndexName();
        final Map<String, String> fields = getFields();

        assertAcked(
            prepareCreate(indexName).setMapping(generateMapping(fields))
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, cluster().numDataNodes()).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );
        ensureGreen(indexName);

        for (var entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            String inferenceId = entry.getValue();
            VectorType expectedVectorType = getExpectedVectorType(inferenceId);

            assertEmbeddingsFields(indexName, List.of(new EmbeddingsField(fieldName, null)), List.of());
            assertEmbeddingsFields(indexName, List.of(new EmbeddingsField(fieldName, expectedVectorType)), List.of());
            assertEmbeddingsFields(
                indexName,
                List.of(new EmbeddingsField(fieldName, randomValueOtherThan(expectedVectorType, () -> randomFrom(VectorType.values())))),
                List.of()
            );
        }

        if (fields.size() > 1) {
            List<EmbeddingsField> embeddingsFields = new ArrayList<>();
            fields.keySet().forEach(fieldName -> embeddingsFields.add(new EmbeddingsField(fieldName, null)));

            assertEmbeddingsFields(indexName, embeddingsFields, List.of());
        }
    }

    void createInferenceEndpoint(TaskType taskType, String inferenceId) throws IOException {
        Map<String, Object> serviceSettings = switch (taskType) {
            case TEXT_EMBEDDING, EMBEDDING -> DENSE_SERVICE_SETTINGS;
            case SPARSE_EMBEDDING -> SPARSE_SERVICE_SETTINGS;
            default -> throw new AssertionError("Unhandled task type [" + taskType + "]");
        };

        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
    }

    VectorType getExpectedVectorType(String inferenceId) {
        TaskType taskType = inferenceIds.get(inferenceId);
        assertNotNull("Inference ID [" + inferenceId + "] not registered", taskType);
        VectorType expectedVectorType = VectorType.fromTaskType(taskType);
        assertNotNull("Cannot determine expected vector type for task type [" + taskType + "]", expectedVectorType);

        return expectedVectorType;
    }

    void assertEmbeddingsFields(String index, List<EmbeddingsField> requestedFields, List<Map<String, VectorType>> expectedFieldsPerHit)
        throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder();
        for (EmbeddingsField field : requestedFields) {
            source.fetchEmbeddingsField(field);
        }

        // Use the coordinating-only node so that fetched embeddings fields are serialized
        // over the wire (data node → coordinating node), exercising transport serialization.
        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { index }, source)),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo((long) expectedFieldsPerHit.size()));
                for (int i = 0; i < expectedFieldsPerHit.size(); i++) {
                    SearchHit hit = response.getHits().getAt(i);
                    Map<String, VectorType> expected = expectedFieldsPerHit.get(i);
                    assertThat(hit.getFields().size(), equalTo(expected.size()));
                    for (var entry : expected.entrySet()) {
                        String fieldName = entry.getKey();
                        VectorType vectorType = entry.getValue();
                        DocumentField documentField = hit.field(fieldName);
                        assertNotNull(documentField);
                        assertEmbeddingsFieldVectorType(documentField, vectorType);
                    }
                }
            }
        );
    }

    /**
     * Validates that each value in {@code field} matches the expected {@link VectorType}:
     * {@link VectorType#DENSE_VECTOR} → {@code float[]} per chunk;
     * {@link VectorType#SPARSE_VECTOR} → {@code Map<String, Float>} per chunk.
     */
    static void assertEmbeddingsFieldVectorType(DocumentField field, VectorType expectedType) {
        assertThat(field.getValues(), not(empty()));
        for (Object value : field.getValues()) {
            switch (expectedType) {
                case DENSE_VECTOR -> assertThat(value, instanceOf(float[].class));
                case SPARSE_VECTOR -> {
                    assertThat(value, instanceOf(Map.class));
                    Map<?, ?> weights = (Map<?, ?>) value;
                    assertThat(weights.keySet(), everyItem(instanceOf(String.class)));
                    assertThat(weights.values(), everyItem(instanceOf(Float.class)));
                }
            }
        }
    }
}
