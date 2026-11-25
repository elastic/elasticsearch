/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.vectors.EmbeddingQueryVectorBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ManyInferenceQueryClausesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test_index";

    private static final Map<String, Object> SPARSE_EMBEDDING_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");
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

    private record FieldInfo(String fieldName, String inferenceId, String fieldType, TaskType inferenceTaskType) {}

    private final Map<String, TaskType> inferenceIds = new HashMap<>();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @After
    public void cleanUp() {
        IntegrationTestUtils.deleteIndex(client(), INDEX_NAME);
        for (var entry : inferenceIds.entrySet()) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), entry.getValue(), entry.getKey());
        }
    }

    public void testManySemanticQueryClauses() throws Exception {
        manyQueryClausesTestCase(
            randomIntBetween(18, 24),
            (fi, q) -> new SemanticQueryBuilder(fi.fieldName(), q),
            Set.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING, TaskType.EMBEDDING)
        );
    }

    public void testManyMatchQueryClauses() throws Exception {
        manyQueryClausesTestCase(
            randomIntBetween(18, 24),
            (fi, query) -> new MatchQueryBuilder(fi.fieldName(), query),
            Set.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING, TaskType.EMBEDDING)
        );
    }

    public void testManySparseVectorQueryClauses() throws Exception {
        manyQueryClausesTestCase(
            randomIntBetween(18, 24),
            (fi, query) -> new SparseVectorQueryBuilder(fi.fieldName(), null, query),
            Set.of(TaskType.SPARSE_EMBEDDING)
        );
    }

    public void testManyKnnQueryClauses() throws Exception {
        int clauseCount = randomIntBetween(18, 24);
        manyQueryClausesTestCase(clauseCount, (fi, query) -> new KnnVectorQueryBuilder(fi.fieldName(), switch (fi.inferenceTaskType()) {
            case EMBEDDING -> new EmbeddingQueryVectorBuilder(fi.inferenceId(), new InferenceStringGroup(query), null);
            case TEXT_EMBEDDING -> new TextEmbeddingQueryVectorBuilder(null, query);
            default -> throw new IllegalArgumentException("Unhandled task type [" + fi.inferenceTaskType() + "]");
        }, clauseCount, clauseCount * 10, null, null), Set.of(TaskType.EMBEDDING, TaskType.TEXT_EMBEDDING));
    }

    private void manyQueryClausesTestCase(
        int clauseCount,
        BiFunction<FieldInfo, String, QueryBuilder> clauseGenerator,
        Set<TaskType> taskTypesToTest
    ) throws Exception {
        List<FieldInfo> fields = new ArrayList<>(clauseCount);
        for (int i = 0; i < clauseCount; i++) {
            String fieldName = randomAlphaOfLength(10);
            String inferenceId = randomIdentifier();
            TaskType taskType = randomFrom(taskTypesToTest);
            String fieldType = taskType.equals(TaskType.EMBEDDING) && randomBoolean()
                ? SemanticFieldMapper.CONTENT_TYPE
                : SemanticTextFieldMapper.CONTENT_TYPE;
            Map<String, Object> serviceSettings = getServiceSettings(taskType);
            createInferenceEndpoint(taskType, inferenceId, serviceSettings);
            fields.add(new FieldInfo(fieldName, inferenceId, fieldType, taskType));
        }

        XContentBuilder mapping = generateMapping(fields);
        assertAcked(prepareCreate(INDEX_NAME).setMapping(mapping));

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (FieldInfo fieldInfo : fields) {
            Map<String, Object> source = Map.of(fieldInfo.fieldName(), randomAlphaOfLength(10));
            DocWriteResponse docWriteResponse = client().prepareIndex(INDEX_NAME).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(docWriteResponse.getResult(), is(DocWriteResponse.Result.CREATED));
            boolQuery.should(clauseGenerator.apply(fieldInfo, randomAlphaOfLength(10)));
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQuery).size(clauseCount);
        SearchRequest searchRequest = new SearchRequest(new String[] { INDEX_NAME }, searchSourceBuilder);
        assertResponse(client().search(searchRequest), response -> {
            assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) clauseCount));
        });
    }

    private static XContentBuilder generateMapping(List<FieldInfo> fields) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (FieldInfo fieldInfo : fields) {
            mapping.startObject(fieldInfo.fieldName());
            mapping.field("type", fieldInfo.fieldType());
            mapping.field("inference_id", fieldInfo.inferenceId());
            mapping.endObject();
        }
        mapping.endObject().endObject();
        return mapping;
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
    }

    private static Map<String, Object> getServiceSettings(TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> SPARSE_EMBEDDING_SERVICE_SETTINGS;
            case TEXT_EMBEDDING, EMBEDDING -> EMBEDDING_SERVICE_SETTINGS;
            default -> throw new IllegalArgumentException("Unhandled task type [" + taskType + "]");
        };
    }
}
