/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.inference.integration.IntegrationTestUtils.createInferenceEndpoint;
import static org.hamcrest.Matchers.containsString;
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
    private static final String INDEX_ALIAS = "index-alias";

    private static final String INFERENCE_FIELD_1 = "inference-field-1";
    private static final String INFERENCE_FIELD_2 = "inference-field-2";
    private static final String INFERENCE_FIELD_3 = "inference-field-3";
    private static final String INFERENCE_FIELD_4 = "inference-field-4";
    private static final String TEXT_FIELD_1 = "text-field-1";
    private static final String TEXT_FIELD_2 = "text-field-2";
    private static final Map<String, Float> ALL_FIELDS = Collections.unmodifiableMap(
        generateDefaultWeightFieldMap(
            Set.of(INFERENCE_FIELD_1, INFERENCE_FIELD_2, INFERENCE_FIELD_3, INFERENCE_FIELD_4, TEXT_FIELD_1, TEXT_FIELD_2)
        )
    );

    private static final Set<InferenceFieldWithTestMetadata> INDEX_1_EXPECTED_INFERENCE_FIELDS = Set.of(
        new InferenceFieldWithTestMetadata(INFERENCE_FIELD_1, SPARSE_EMBEDDING_INFERENCE_ID, 1.0f),
        new InferenceFieldWithTestMetadata(INFERENCE_FIELD_2, TEXT_EMBEDDING_INFERENCE_ID, 1.0f),
        new InferenceFieldWithTestMetadata(INFERENCE_FIELD_3, SPARSE_EMBEDDING_INFERENCE_ID, 1.0f),
        new InferenceFieldWithTestMetadata(INFERENCE_FIELD_4, TEXT_EMBEDDING_INFERENCE_ID, 1.0f)
    );
    private static final Set<InferenceFieldWithTestMetadata> INDEX_2_EXPECTED_INFERENCE_FIELDS = Set.of(
        new InferenceFieldWithTestMetadata(INFERENCE_FIELD_1, TEXT_EMBEDDING_INFERENCE_ID, 1.0f),
        new InferenceFieldWithTestMetadata(INFERENCE_FIELD_2, SPARSE_EMBEDDING_INFERENCE_ID, 1.0f),
        new InferenceFieldWithTestMetadata(INFERENCE_FIELD_3, SPARSE_EMBEDDING_INFERENCE_ID, 1.0f),
        new InferenceFieldWithTestMetadata(INFERENCE_FIELD_4, TEXT_EMBEDDING_INFERENCE_ID, 1.0f)
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
            createTestIndices();
            clusterConfigured = true;
        }
    }

    public void testNullQuery() {
        explicitIndicesAndFieldsTestCase(null);
    }

    public void testNonNullQuery() {
        explicitIndicesAndFieldsTestCase("foo");
    }

    public void testBlankQuery() {
        explicitIndicesAndFieldsTestCase("   ");
    }

    public void testFieldWeight() {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(
                ALL_INDICES,
                Map.of(INFERENCE_FIELD_1, 2.0f, "inference-*", 1.5f, TEXT_FIELD_1, 1.75f),
                false,
                false,
                "foo"
            ),
            Map.of(
                INDEX_1,
                Set.of(new InferenceFieldWithTestMetadata(INFERENCE_FIELD_1, SPARSE_EMBEDDING_INFERENCE_ID, 2.0f)),
                INDEX_2,
                Set.of(new InferenceFieldWithTestMetadata(INFERENCE_FIELD_1, TEXT_EMBEDDING_INFERENCE_ID, 2.0f))
            ),
            ALL_EXPECTED_INFERENCE_RESULTS
        );
    }

    public void testNoInferenceFields() {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(
                ALL_INDICES,
                generateDefaultWeightFieldMap(Set.of(TEXT_FIELD_1, TEXT_FIELD_2)),
                false,
                false,
                "foo"
            ),
            Map.of(INDEX_1, Set.of(), INDEX_2, Set.of()),
            Map.of()
        );
    }

    public void testResolveFieldWildcards() {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(ALL_INDICES, generateDefaultWeightFieldMap(Set.of("*")), true, false, "foo"),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS, INDEX_2, INDEX_2_EXPECTED_INFERENCE_FIELDS),
            ALL_EXPECTED_INFERENCE_RESULTS
        );

        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(
                ALL_INDICES,
                Map.of("*-field-1", 2.0f, "*-1", 1.75f, "inference-*-3", 2.0f),
                true,
                false,
                "foo"
            ),
            Map.of(
                INDEX_1,
                Set.of(
                    new InferenceFieldWithTestMetadata(INFERENCE_FIELD_1, SPARSE_EMBEDDING_INFERENCE_ID, 3.5f),
                    new InferenceFieldWithTestMetadata(INFERENCE_FIELD_3, SPARSE_EMBEDDING_INFERENCE_ID, 2.0f)
                ),
                INDEX_2,
                Set.of(
                    new InferenceFieldWithTestMetadata(INFERENCE_FIELD_1, TEXT_EMBEDDING_INFERENCE_ID, 3.5f),
                    new InferenceFieldWithTestMetadata(INFERENCE_FIELD_3, SPARSE_EMBEDDING_INFERENCE_ID, 2.0f)
                )
            ),
            ALL_EXPECTED_INFERENCE_RESULTS
        );
    }

    public void testUseDefaultFields() {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(Set.of(INDEX_1), Map.of(), true, true, "foo"),
            Map.of(INDEX_1, Set.of(new InferenceFieldWithTestMetadata(INFERENCE_FIELD_1, SPARSE_EMBEDDING_INFERENCE_ID, 5.0f))),
            filterExpectedInferenceResults(ALL_EXPECTED_INFERENCE_RESULTS, Set.of(SPARSE_EMBEDDING_INFERENCE_ID))
        );

        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(Set.of(INDEX_2), Map.of(), true, true, "foo"),
            Map.of(INDEX_2, INDEX_2_EXPECTED_INFERENCE_FIELDS),
            ALL_EXPECTED_INFERENCE_RESULTS
        );
    }

    public void testMissingIndexName() {
        Set<String> indicesWithIndex1 = Set.of(INDEX_1, "missing-index");
        assertFailedRequest(
            new GetInferenceFieldsAction.Request(indicesWithIndex1, ALL_FIELDS, false, false, "foo"),
            IndexNotFoundException.class,
            e -> assertThat(e.getMessage(), containsString("no such index [missing-index]"))
        );
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(indicesWithIndex1, ALL_FIELDS, false, false, "foo", IndicesOptions.LENIENT_EXPAND_OPEN),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS),
            ALL_EXPECTED_INFERENCE_RESULTS
        );

        Set<String> indicesWithoutIndex1 = Set.of("missing-index");
        assertFailedRequest(
            new GetInferenceFieldsAction.Request(indicesWithoutIndex1, ALL_FIELDS, false, false, "foo"),
            IndexNotFoundException.class,
            e -> assertThat(e.getMessage(), containsString("no such index [missing-index]"))
        );
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(indicesWithoutIndex1, ALL_FIELDS, false, false, "foo", IndicesOptions.LENIENT_EXPAND_OPEN),
            Map.of(),
            Map.of()
        );
    }

    public void testMissingFieldName() {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(ALL_INDICES, generateDefaultWeightFieldMap(Set.of("missing-field")), false, false, "foo"),
            Map.of(INDEX_1, Set.of(), INDEX_2, Set.of()),
            Map.of()
        );

        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(ALL_INDICES, generateDefaultWeightFieldMap(Set.of("missing-*")), true, false, "foo"),
            Map.of(INDEX_1, Set.of(), INDEX_2, Set.of()),
            Map.of()
        );
    }

    public void testNoIndices() {
        // By default, an empty index set will be interpreted as _all
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(Set.of(), ALL_FIELDS, false, false, "foo"),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS, INDEX_2, INDEX_2_EXPECTED_INFERENCE_FIELDS),
            ALL_EXPECTED_INFERENCE_RESULTS
        );

        // We can provide an IndicesOptions that changes this behavior to interpret an empty index set as no indices
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(Set.of(), ALL_FIELDS, false, false, "foo", IndicesOptions.STRICT_NO_EXPAND_FORBID_CLOSED),
            Map.of(),
            Map.of()
        );
    }

    public void testAllIndices() {
        // By default, _all expands to all indices
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(Set.of("_all"), ALL_FIELDS, false, false, "foo"),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS, INDEX_2, INDEX_2_EXPECTED_INFERENCE_FIELDS),
            ALL_EXPECTED_INFERENCE_RESULTS
        );

        // We can provide an IndicesOptions that changes this behavior to interpret it as no indices
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(
                Set.of("_all"),
                ALL_FIELDS,
                false,
                false,
                "foo",
                IndicesOptions.STRICT_NO_EXPAND_FORBID_CLOSED
            ),
            Map.of(),
            Map.of()
        );
    }

    public void testIndexAlias() {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(Set.of(INDEX_ALIAS), ALL_FIELDS, false, false, "foo"),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS, INDEX_2, INDEX_2_EXPECTED_INFERENCE_FIELDS),
            ALL_EXPECTED_INFERENCE_RESULTS
        );
    }

    public void testResolveIndexWildcards() {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(Set.of("index-*"), ALL_FIELDS, false, false, "foo"),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS, INDEX_2, INDEX_2_EXPECTED_INFERENCE_FIELDS),
            ALL_EXPECTED_INFERENCE_RESULTS
        );

        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(Set.of("*-1"), ALL_FIELDS, false, false, "foo"),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS),
            ALL_EXPECTED_INFERENCE_RESULTS
        );

        assertFailedRequest(
            new GetInferenceFieldsAction.Request(
                Set.of("index-*"),
                ALL_FIELDS,
                false,
                false,
                "foo",
                IndicesOptions.STRICT_NO_EXPAND_FORBID_CLOSED
            ),
            IndexNotFoundException.class,
            e -> assertThat(e.getMessage(), containsString("no such index [index-*]"))
        );
    }

    public void testNoFields() {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(ALL_INDICES, Map.of(), false, false, "foo"),
            Map.of(INDEX_1, Set.of(), INDEX_2, Set.of()),
            Map.of()
        );
    }

    public void testInvalidRequest() {
        final BiConsumer<ActionRequestValidationException, List<String>> validator = (e, l) -> l.forEach(
            s -> assertThat(e.getMessage(), containsString(s))
        );

        assertFailedRequest(
            new GetInferenceFieldsAction.Request(null, Map.of(), false, false, null),
            ActionRequestValidationException.class,
            e -> validator.accept(e, List.of("indices must not be null"))
        );
        assertFailedRequest(
            new GetInferenceFieldsAction.Request(Set.of(), null, false, false, null),
            ActionRequestValidationException.class,
            e -> validator.accept(e, List.of("fields must not be null"))
        );
        assertFailedRequest(
            new GetInferenceFieldsAction.Request(null, null, false, false, null),
            ActionRequestValidationException.class,
            e -> validator.accept(e, List.of("indices must not be null", "fields must not be null"))
        );

        Map<String, Float> fields = new HashMap<>();
        fields.put(INFERENCE_FIELD_1, null);
        assertFailedRequest(
            new GetInferenceFieldsAction.Request(Set.of(), fields, false, false, null),
            ActionRequestValidationException.class,
            e -> validator.accept(e, List.of("weight for field [" + INFERENCE_FIELD_1 + "] must not be null"))
        );
    }

    private void explicitIndicesAndFieldsTestCase(String query) {
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(ALL_INDICES, ALL_FIELDS, false, false, query),
            Map.of(INDEX_1, INDEX_1_EXPECTED_INFERENCE_FIELDS, INDEX_2, INDEX_2_EXPECTED_INFERENCE_FIELDS),
            query == null || query.isBlank() ? Map.of() : ALL_EXPECTED_INFERENCE_RESULTS
        );

        Map<String, Class<? extends InferenceResults>> expectedInferenceResultsSparseOnly = filterExpectedInferenceResults(
            ALL_EXPECTED_INFERENCE_RESULTS,
            Set.of(SPARSE_EMBEDDING_INFERENCE_ID)
        );
        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(
                ALL_INDICES,
                generateDefaultWeightFieldMap(Set.of(INFERENCE_FIELD_3)),
                false,
                false,
                query
            ),
            Map.of(
                INDEX_1,
                filterExpectedInferenceFieldSet(INDEX_1_EXPECTED_INFERENCE_FIELDS, Set.of(INFERENCE_FIELD_3)),
                INDEX_2,
                filterExpectedInferenceFieldSet(INDEX_2_EXPECTED_INFERENCE_FIELDS, Set.of(INFERENCE_FIELD_3))
            ),
            query == null || query.isBlank() ? Map.of() : expectedInferenceResultsSparseOnly
        );

        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(
                Set.of(INDEX_1),
                generateDefaultWeightFieldMap(Set.of(INFERENCE_FIELD_3)),
                false,
                false,
                query
            ),
            Map.of(INDEX_1, filterExpectedInferenceFieldSet(INDEX_1_EXPECTED_INFERENCE_FIELDS, Set.of(INFERENCE_FIELD_3))),
            query == null || query.isBlank() ? Map.of() : expectedInferenceResultsSparseOnly
        );

        assertSuccessfulRequest(
            new GetInferenceFieldsAction.Request(ALL_INDICES, generateDefaultWeightFieldMap(Set.of("*")), false, false, query),
            Map.of(INDEX_1, Set.of(), INDEX_2, Set.of()),
            Map.of()
        );
    }

    private void createInferenceEndpoints() throws IOException {
        createInferenceEndpoint(client(), TaskType.SPARSE_EMBEDDING, SPARSE_EMBEDDING_INFERENCE_ID, SPARSE_EMBEDDING_SERVICE_SETTINGS);
        createInferenceEndpoint(client(), TaskType.TEXT_EMBEDDING, TEXT_EMBEDDING_INFERENCE_ID, TEXT_EMBEDDING_SERVICE_SETTINGS);
    }

    private void createTestIndices() throws IOException {
        createTestIndex(INDEX_1, List.of("*-field-1^5"));
        createTestIndex(INDEX_2, null);
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias(new String[] { INDEX_1, INDEX_2 }, INDEX_ALIAS)
        );
    }

    private void createTestIndex(String indexName, @Nullable List<String> defaultFields) throws IOException {
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
        addSemanticTextField(INFERENCE_FIELD_4, TEXT_EMBEDDING_INFERENCE_ID, mapping);
        addTextField(TEXT_FIELD_1, mapping);
        addTextField(TEXT_FIELD_2, mapping);
        mapping.endObject().endObject();

        var createIndexRequest = prepareCreate(indexName).setMapping(mapping);
        if (defaultFields != null) {
            Settings settings = Settings.builder().putList(DEFAULT_FIELD_SETTING.getKey(), defaultFields).build();
            createIndexRequest.setSettings(settings);
        }
        assertAcked(createIndexRequest);
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

    private static void assertSuccessfulRequest(
        GetInferenceFieldsAction.Request request,
        Map<String, Set<InferenceFieldWithTestMetadata>> expectedInferenceFields,
        Map<String, Class<? extends InferenceResults>> expectedInferenceResults
    ) {
        var response = executeRequest(request);
        assertInferenceFieldsMap(response.getInferenceFieldsMap(), expectedInferenceFields);
        assertInferenceResultsMap(response.getInferenceResultsMap(), expectedInferenceResults);
    }

    private static <T extends Exception> void assertFailedRequest(
        GetInferenceFieldsAction.Request request,
        Class<T> expectedException,
        Consumer<T> exceptionValidator
    ) {
        T exception = assertThrows(expectedException, () -> executeRequest(request));
        exceptionValidator.accept(exception);
    }

    static void assertInferenceFieldsMap(
        Map<String, List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata>> inferenceFieldsMap,
        Map<String, Set<InferenceFieldWithTestMetadata>> expectedInferenceFields
    ) {
        assertThat(inferenceFieldsMap.size(), equalTo(expectedInferenceFields.size()));
        for (var entry : inferenceFieldsMap.entrySet()) {
            String indexName = entry.getKey();
            List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata> indexInferenceFields = entry.getValue();

            Set<InferenceFieldWithTestMetadata> expectedIndexInferenceFields = expectedInferenceFields.get(indexName);
            assertThat(expectedIndexInferenceFields, notNullValue());

            Set<InferenceFieldWithTestMetadata> remainingExpectedIndexInferenceFields = new HashSet<>(expectedIndexInferenceFields);
            for (var indexInferenceField : indexInferenceFields) {
                InferenceFieldWithTestMetadata inferenceFieldWithTestMetadata = new InferenceFieldWithTestMetadata(
                    indexInferenceField.inferenceFieldMetadata().getName(),
                    indexInferenceField.inferenceFieldMetadata().getSearchInferenceId(),
                    indexInferenceField.weight()
                );
                assertThat(remainingExpectedIndexInferenceFields.remove(inferenceFieldWithTestMetadata), is(true));
            }
            assertThat(remainingExpectedIndexInferenceFields, empty());
        }
    }

    static void assertInferenceResultsMap(
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

    static Map<String, Float> generateDefaultWeightFieldMap(Set<String> fieldList) {
        Map<String, Float> fieldMap = new HashMap<>();
        fieldList.forEach(field -> fieldMap.put(field, 1.0f));
        return fieldMap;
    }

    private static Set<InferenceFieldWithTestMetadata> filterExpectedInferenceFieldSet(
        Set<InferenceFieldWithTestMetadata> inferenceFieldSet,
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

    record InferenceFieldWithTestMetadata(String field, String inferenceId, float weight) {}
}
