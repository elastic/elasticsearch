/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.inference.integration.IntegrationTestUtils.createInferenceEndpoint;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class GetInferenceFieldsIT extends ESIntegTestCase {
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

    private static final String SPARSE_EMBEDDING_INFERENCE_ID = "sparse-embedding-id";
    private static final String TEXT_EMBEDDING_INFERENCE_ID = "text-embedding-id";

    private static final String INDEX_1 = "index-1";
    private static final String INDEX_2 = "index-2";
    private static final Set<String> ALL_INDICES = Set.of(INDEX_1, INDEX_2);

    private static final String INFERENCE_FIELD_1 = "inference-field-1";
    private static final String INFERENCE_FIELD_2 = "inference-field-2";
    private static final String INFERENCE_FIELD_3 = "inference-field-3";
    private static final String TEXT_FIELD_1 = "text-field-1";
    private static final String TEXT_FIELD_2 = "text-field-2";
    private static final Set<String> ALL_FIELDS = Set.of(
        INFERENCE_FIELD_1,
        INFERENCE_FIELD_2,
        INFERENCE_FIELD_3,
        TEXT_FIELD_1,
        TEXT_FIELD_2
    );

    private static final Set<InferenceFieldAndId> INDEX_1_EXPECTED_INFERENCE_FIELDS = Set.of(
        new InferenceFieldAndId(INFERENCE_FIELD_1, SPARSE_EMBEDDING_INFERENCE_ID),
        new InferenceFieldAndId(INFERENCE_FIELD_2, TEXT_EMBEDDING_INFERENCE_ID),
        new InferenceFieldAndId(INFERENCE_FIELD_3, SPARSE_EMBEDDING_INFERENCE_ID)
    );
    private static final Set<InferenceFieldAndId> INDEX_2_EXPECTED_INFERENCE_FIELDS = Set.of(
        new InferenceFieldAndId(INFERENCE_FIELD_1, TEXT_EMBEDDING_INFERENCE_ID),
        new InferenceFieldAndId(INFERENCE_FIELD_2, SPARSE_EMBEDDING_INFERENCE_ID),
        new InferenceFieldAndId(INFERENCE_FIELD_3, SPARSE_EMBEDDING_INFERENCE_ID)
    );

    private static final Map<String, Class<? extends InferenceResults>> ALL_EXPECTED_INFERENCE_RESULTS = Map.of(
        SPARSE_EMBEDDING_INFERENCE_ID,
        TextExpansionResults.class,
        TEXT_EMBEDDING_INFERENCE_ID,
        MlDenseEmbeddingResults.class
    );

    private boolean clusterConfigured = false;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, FakeMlPlugin.class);
    }

    @Before
    public void setUpCluster() throws Exception {
        if (clusterConfigured == false) {
            createInferenceEndpoints();
            createIndices();
            clusterConfigured = true;
        }
    }

    public void testNullQuery() {
        explicitInferenceFieldsTestCase(null);
    }

    public void testNonNullQuery() {
        explicitInferenceFieldsTestCase("foo");
    }

    public void testBlankQuery() {
        explicitInferenceFieldsTestCase("   ");
    }

    public void testNoInferenceFields() {
        // TODO: Implement
    }

    public void testResolveWildcards() {
        // TODO: Implement
    }

    public void testUseDefaultFields() {
        // TODO: Implement
    }

    public void testMissingIndexName() {
        // TODO: Implement
    }

    public void testMissingFieldName() {
        // TODO: Implement
    }

    public void testInvalidRequest() {
        // TODO: Implement
    }

    private void explicitInferenceFieldsTestCase(String query) {
        assertRequest(
            new GetInferenceFieldsAction.Request(ALL_INDICES, ALL_FIELDS, false, false, query),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS, INDEX_2, INDEX_2_EXPECTED_INFERENCE_FIELDS),
            query == null || query.isBlank() ? Map.of() : ALL_EXPECTED_INFERENCE_RESULTS
        );

        Map<String, Class<? extends InferenceResults>> expectedInferenceResultsSparseOnly = filterExpectedInferenceResults(
            ALL_EXPECTED_INFERENCE_RESULTS,
            Set.of(SPARSE_EMBEDDING_INFERENCE_ID)
        );
        assertRequest(
            new GetInferenceFieldsAction.Request(ALL_INDICES, Set.of(INFERENCE_FIELD_3), false, false, query),
            Map.of(
                INDEX_1,
                filterExpectedInferenceFieldSet(INDEX_1_EXPECTED_INFERENCE_FIELDS, Set.of(INFERENCE_FIELD_3)),
                INDEX_2,
                filterExpectedInferenceFieldSet(INDEX_2_EXPECTED_INFERENCE_FIELDS, Set.of(INFERENCE_FIELD_3))
            ),
            query == null || query.isBlank() ? Map.of() : expectedInferenceResultsSparseOnly
        );

        assertRequest(
            new GetInferenceFieldsAction.Request(Set.of(INDEX_1), Set.of(INFERENCE_FIELD_3), false, false, query),
            Map.of(INDEX_1, filterExpectedInferenceFieldSet(INDEX_1_EXPECTED_INFERENCE_FIELDS, Set.of(INFERENCE_FIELD_3))),
            query == null || query.isBlank() ? Map.of() : expectedInferenceResultsSparseOnly
        );
    }

    private void createInferenceEndpoints() throws IOException {
        createInferenceEndpoint(client(), TaskType.SPARSE_EMBEDDING, SPARSE_EMBEDDING_INFERENCE_ID, SPARSE_EMBEDDING_SERVICE_SETTINGS);
        createInferenceEndpoint(client(), TaskType.TEXT_EMBEDDING, TEXT_EMBEDDING_INFERENCE_ID, TEXT_EMBEDDING_SERVICE_SETTINGS);
    }

    private void createIndices() throws IOException {
        createIndex(INDEX_1);
        createIndex(INDEX_2);
    }

    private void createIndex(String indexName) throws IOException {
        final String inferenceField1InferenceId = switch (indexName) {
            case INDEX_1 -> SPARSE_EMBEDDING_INFERENCE_ID;
            case INDEX_2 -> TEXT_EMBEDDING_INFERENCE_ID;
            default -> throw new AssertionError("Unhandled index name [" + indexName + "]");
        };
        final String inferenceField2InferenceId = switch (indexName) {
            case INDEX_1 -> TEXT_EMBEDDING_INFERENCE_ID;
            case INDEX_2 -> SPARSE_EMBEDDING_INFERENCE_ID;
            default -> throw new AssertionError("Unhandled index name [" + indexName + "]");
        };

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        addSemanticTextField(INFERENCE_FIELD_1, inferenceField1InferenceId, mapping);
        addSemanticTextField(INFERENCE_FIELD_2, inferenceField2InferenceId, mapping);
        addSemanticTextField(INFERENCE_FIELD_3, SPARSE_EMBEDDING_INFERENCE_ID, mapping);
        addTextField(TEXT_FIELD_1, mapping);
        addTextField(TEXT_FIELD_2, mapping);
        mapping.endObject().endObject();

        assertAcked(prepareCreate(indexName).setMapping(mapping));
    }

    private void addSemanticTextField(String fieldName, String inferenceId, XContentBuilder mapping) throws IOException {
        mapping.startObject(fieldName);
        mapping.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mapping.field("inference_id", inferenceId);
        mapping.endObject();
    }

    private void addTextField(String fieldName, XContentBuilder mapping) throws IOException {
        mapping.startObject(fieldName);
        mapping.field("type", TextFieldMapper.CONTENT_TYPE);
        mapping.endObject();
    }

    private static GetInferenceFieldsAction.Response executeRequest(GetInferenceFieldsAction.Request request) {
        return client().execute(GetInferenceFieldsAction.INSTANCE, request).actionGet(TEST_REQUEST_TIMEOUT);
    }

    private static void assertRequest(
        GetInferenceFieldsAction.Request request,
        Map<String, Set<InferenceFieldAndId>> expectedInferenceFields,
        Map<String, Class<? extends InferenceResults>> expectedInferenceResults
    ) {
        var response = executeRequest(request);
        assertInferenceFieldsMap(response.getInferenceFieldsMap(), expectedInferenceFields);
        assertInferenceResultsMap(response.getInferenceResultsMap(), expectedInferenceResults);
    }

    private static void assertInferenceFieldsMap(
        Map<String, List<InferenceFieldMetadata>> inferenceFieldsMap,
        Map<String, Set<InferenceFieldAndId>> expectedInferenceFields
    ) {
        assertThat(inferenceFieldsMap.size(), equalTo(expectedInferenceFields.size()));
        for (var entry : inferenceFieldsMap.entrySet()) {
            String indexName = entry.getKey();
            List<InferenceFieldMetadata> indexInferenceFields = entry.getValue();

            Set<InferenceFieldAndId> expectedIndexInferenceFields = expectedInferenceFields.get(indexName);
            assertThat(expectedIndexInferenceFields, notNullValue());

            Set<InferenceFieldAndId> remainingExpectedIndexInferenceFields = new HashSet<>(expectedIndexInferenceFields);
            for (InferenceFieldMetadata indexInferenceField : indexInferenceFields) {
                InferenceFieldAndId inferenceFieldAndId = new InferenceFieldAndId(
                    indexInferenceField.getName(),
                    indexInferenceField.getSearchInferenceId()
                );
                assertThat(remainingExpectedIndexInferenceFields.remove(inferenceFieldAndId), is(true));
            }
            assertThat(remainingExpectedIndexInferenceFields, empty());
        }
    }

    private static void assertInferenceResultsMap(
        Map<String, InferenceResults> inferenceResultsMap,
        Map<String, Class<? extends InferenceResults>> expectedInferenceResults
    ) {
        assertThat(inferenceResultsMap.size(), equalTo(expectedInferenceResults.size()));
        for (var entry : inferenceResultsMap.entrySet()) {
            String inferenceId = entry.getKey();
            InferenceResults inferenceResults = entry.getValue();

            Class<? extends InferenceResults> expectedInferenceResultsClass = expectedInferenceResults.get(inferenceId);
            assertThat(expectedInferenceResultsClass, notNullValue());
            assertThat(inferenceResults, instanceOf(expectedInferenceResultsClass));
        }
    }

    private static Set<InferenceFieldAndId> filterExpectedInferenceFieldSet(
        Set<InferenceFieldAndId> inferenceFieldSet,
        Set<String> fieldNames
    ) {
        return inferenceFieldSet.stream().filter(i -> fieldNames.contains(i.field())).collect(Collectors.toSet());
    }

    private static Map<String, Class<? extends InferenceResults>> filterExpectedInferenceResults(
        Map<String, Class<? extends InferenceResults>> expectedInferenceResults,
        Set<String> inferenceIds
    ) {
        return expectedInferenceResults.entrySet()
            .stream()
            .filter(e -> inferenceIds.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private record InferenceFieldAndId(String field, String inferenceId) {}
}
