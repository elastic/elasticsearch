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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.inference.SimilarityMeasure;
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
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 1, supportsDedicatedMasters = false)
abstract class AbstractEmbeddingsFieldIT extends ESIntegTestCase {
    static final int VECTOR_DIMENSIONS = 128;  // Use a dimension count that is compatible with BIT element type

    private static final Map<String, Object> SPARSE_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");

    String indexName = null;
    final Map<String, TaskType> inferenceIds = new HashMap<>();
    final List<InferenceFieldConfig> inferenceFields = new ArrayList<>();

    /**
     * An inference field to fetch embeddings from, and metadata about the inference endpoint that generates its embeddings. The element
     * type is null when the endpoint produces sparse embeddings.
     */
    record InferenceFieldConfig(
        String fieldName,
        String inferenceId,
        TaskType taskType,
        @Nullable DenseVectorFieldMapper.ElementType elementType
    ) {}

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int maximumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @Before
    private void createInferenceEndpoints() throws IOException {
        for (TaskType taskType : supportedTaskTypes()) {
            switch (taskType) {
                case SPARSE_EMBEDDING -> addInferenceField(taskType, null, SPARSE_SERVICE_SETTINGS);
                case TEXT_EMBEDDING, EMBEDDING -> {
                    for (DenseVectorFieldMapper.ElementType elementType : DenseVectorFieldMapper.ElementType.values()) {
                        addInferenceField(taskType, elementType, generateDenseServiceSettings(elementType));
                    }
                }
                default -> throw new AssertionError("Unhandled task type [" + taskType + "]");
            }
        }
    }

    @After
    private void cleanUp() {
        if (indexName != null) {
            IntegrationTestUtils.deleteIndex(client(), indexName);
        }

        for (var entry : inferenceIds.entrySet()) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), entry.getValue(), entry.getKey());
        }
    }

    abstract Set<TaskType> supportedTaskTypes();

    abstract XContentBuilder generateMapping(Map<String, String> fieldNameToInferenceIdMap) throws IOException;

    public void testFetchEmbeddingsFields() throws Exception {
        createIndex();

        BulkRequestBuilder bulk = client().prepareBulk(indexName);
        Map<String, Object> source = new HashMap<>();
        for (InferenceFieldConfig inferenceField : inferenceFields) {
            source.put(inferenceField.fieldName(), randomAlphaOfLengthBetween(5, 10));
        }
        bulk.add(client().prepareIndex(indexName).setSource(source));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.get(TEST_REQUEST_TIMEOUT);
        ensureGreen(indexName);

        for (InferenceFieldConfig inferenceField : inferenceFields) {
            String fieldName = inferenceField.fieldName();
            VectorType expectedVectorType = getExpectedVectorType(inferenceField);
            String message = describe(inferenceField);

            assertEmbeddingsFields(
                message,
                indexName,
                List.of(new EmbeddingsField(fieldName, null)),
                List.of(Map.of(fieldName, expectedVectorType))
            );
            assertEmbeddingsFields(
                message,
                indexName,
                List.of(new EmbeddingsField(fieldName, expectedVectorType)),
                List.of(Map.of(fieldName, expectedVectorType))
            );
            assertEmbeddingsFields(
                message,
                indexName,
                List.of(new EmbeddingsField(fieldName, randomValueOtherThan(expectedVectorType, () -> randomFrom(VectorType.values())))),
                List.of(Map.of())
            );
        }

        if (inferenceFields.size() > 1) {
            List<EmbeddingsField> embeddingsFields = new ArrayList<>();
            Map<String, VectorType> expectedFields = new HashMap<>();
            for (InferenceFieldConfig inferenceField : inferenceFields) {
                embeddingsFields.add(new EmbeddingsField(inferenceField.fieldName(), null));
                expectedFields.put(inferenceField.fieldName(), getExpectedVectorType(inferenceField));
            }

            assertEmbeddingsFields("Fetching all inference fields", indexName, embeddingsFields, List.of(expectedFields));
        }
    }

    public void testFetchEmbeddingsFieldsNoDocuments() throws Exception {
        createIndex();
        ensureGreen(indexName);

        for (InferenceFieldConfig inferenceField : inferenceFields) {
            String fieldName = inferenceField.fieldName();
            VectorType expectedVectorType = getExpectedVectorType(inferenceField);
            String message = describe(inferenceField);

            assertEmbeddingsFields(message, indexName, List.of(new EmbeddingsField(fieldName, null)), List.of());
            assertEmbeddingsFields(message, indexName, List.of(new EmbeddingsField(fieldName, expectedVectorType)), List.of());
            assertEmbeddingsFields(
                message,
                indexName,
                List.of(new EmbeddingsField(fieldName, randomValueOtherThan(expectedVectorType, () -> randomFrom(VectorType.values())))),
                List.of()
            );
        }

        if (inferenceFields.size() > 1) {
            List<EmbeddingsField> embeddingsFields = new ArrayList<>();
            inferenceFields.forEach(f -> embeddingsFields.add(new EmbeddingsField(f.fieldName(), null)));

            assertEmbeddingsFields("Fetching all inference fields", indexName, embeddingsFields, List.of());
        }
    }

    private void createIndex() throws IOException {
        indexName = randomIndexName();
        final Map<String, String> fieldNameToInferenceIdMap = inferenceFields.stream()
            .collect(Collectors.toMap(InferenceFieldConfig::fieldName, InferenceFieldConfig::inferenceId));
        assertAcked(prepareCreate(indexName).setMapping(generateMapping(fieldNameToInferenceIdMap)));
    }

    private void addInferenceField(
        TaskType taskType,
        @Nullable DenseVectorFieldMapper.ElementType elementType,
        Map<String, Object> serviceSettings
    ) throws IOException {
        String fieldName = generateFieldName(taskType, elementType);
        String inferenceId = fieldName + "-test-endpoint";

        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
        inferenceFields.add(new InferenceFieldConfig(fieldName, inferenceId, taskType, elementType));
    }

    VectorType getExpectedVectorType(InferenceFieldConfig inferenceField) {
        VectorType expectedVectorType = VectorType.fromTaskType(inferenceField.taskType());
        assertNotNull("Cannot determine expected vector type for task type [" + inferenceField.taskType() + "]", expectedVectorType);

        return expectedVectorType;
    }

    void assertEmbeddingsFields(
        String message,
        String index,
        List<EmbeddingsField> requestedFields,
        List<Map<String, VectorType>> expectedFieldsPerHit
    ) throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder();
        for (EmbeddingsField field : requestedFields) {
            source.fetchEmbeddingsField(field);
        }

        // Use the coordinating-only node so that fetched embeddings fields are serialized
        // over the wire (data node → coordinating node), exercising transport serialization.
        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { index }, source)),
            response -> {
                assertThat(message, response.getHits().getTotalHits().value(), equalTo((long) expectedFieldsPerHit.size()));
                for (int i = 0; i < expectedFieldsPerHit.size(); i++) {
                    SearchHit hit = response.getHits().getAt(i);
                    Map<String, VectorType> expected = expectedFieldsPerHit.get(i);
                    assertThat(message, hit.getFields().size(), equalTo(expected.size()));
                    for (var entry : expected.entrySet()) {
                        String fieldName = entry.getKey();
                        VectorType vectorType = entry.getValue();
                        DocumentField documentField = hit.field(fieldName);
                        assertNotNull(message, documentField);
                        assertEmbeddingsFieldVectorType(message, documentField, vectorType);
                    }
                }
            }
        );
    }

    /**
     * Validates that each value in {@code field} matches the expected {@link VectorType}:
     * {@link VectorType#DENSE_VECTOR} → {@code float[]} per chunk;
     * {@link VectorType#SPARSE_VECTOR} → {@code Map<String, Float>} per chunk.
     * <p>
     * Dense embeddings are always fetched as {@code float[]}, whatever element type the inference endpoint produces, because byte and bit
     * embeddings can be represented exactly as float vectors.
     * </p>
     */
    static void assertEmbeddingsFieldVectorType(String message, DocumentField field, VectorType expectedType) {
        assertThat(message, field.getValues(), not(empty()));
        for (Object value : field.getValues()) {
            switch (expectedType) {
                case DENSE_VECTOR -> assertThat(message, value, instanceOf(float[].class));
                case SPARSE_VECTOR -> {
                    assertThat(message, value, instanceOf(Map.class));
                    Map<?, ?> weights = (Map<?, ?>) value;
                    assertThat(message, weights.keySet(), everyItem(instanceOf(String.class)));
                    assertThat(message, weights.values(), everyItem(instanceOf(Float.class)));
                }
            }
        }
    }

    private static Map<String, Object> generateDenseServiceSettings(DenseVectorFieldMapper.ElementType elementType) {
        List<SimilarityMeasure> supportedSimilarities = new ArrayList<>(
            DenseVectorFieldMapperTestUtils.getSupportedSimilarities(elementType)
        );
        // Dot product requires unit vectors, which the mock inference services do not produce
        supportedSimilarities.remove(SimilarityMeasure.DOT_PRODUCT);

        return Map.of(
            "model",
            "my_model",
            "dimensions",
            VECTOR_DIMENSIONS,
            "similarity",
            randomFrom(supportedSimilarities),
            "element_type",
            elementType,
            "api_key",
            "my_api_key"
        );
    }

    private static String generateFieldName(TaskType taskType, @Nullable DenseVectorFieldMapper.ElementType elementType) {
        return elementType == null ? Strings.format("%s_field", taskType) : Strings.format("%s_%s_field", taskType, elementType);
    }

    private static String describe(InferenceFieldConfig inferenceField) {
        return Strings.format(
            "Fetching embeddings from field [%s] with task type [%s] and element type [%s]",
            inferenceField.fieldName(),
            inferenceField.taskType(),
            inferenceField.elementType()
        );
    }
}
