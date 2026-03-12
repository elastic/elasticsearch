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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ManyInferenceQueryClausesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test_index";

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
        manyQueryClausesTestCase(randomIntBetween(18, 24), SemanticQueryBuilder::new, TaskType.SPARSE_EMBEDDING);
    }

    public void testManyMatchQueryClauses() throws Exception {
        manyQueryClausesTestCase(randomIntBetween(18, 24), MatchQueryBuilder::new, TaskType.SPARSE_EMBEDDING);
    }

    public void testManySparseVectorQueryClauses() throws Exception {
        manyQueryClausesTestCase(randomIntBetween(18, 24), (f, q) -> new SparseVectorQueryBuilder(f, null, q), TaskType.SPARSE_EMBEDDING);
    }

    public void testManyKnnQueryClauses() throws Exception {
        int clauseCount = randomIntBetween(18, 24);
        manyQueryClausesTestCase(
            clauseCount,
            (f, q) -> new KnnVectorQueryBuilder(f, new TextEmbeddingQueryVectorBuilder(null, q), clauseCount, clauseCount * 10, null, null),
            TaskType.TEXT_EMBEDDING
        );
    }

    private void manyQueryClausesTestCase(
        int clauseCount,
        BiFunction<String, String, QueryBuilder> clauseGenerator,
        TaskType semanticTextFieldTaskType
    ) throws Exception {
        Map<String, Object> inferenceEndpointServiceSettings = getServiceSettings(semanticTextFieldTaskType);
        Map<String, String> semanticTextFields = new HashMap<>(clauseCount);
        for (int i = 0; i < clauseCount; i++) {
            String fieldName = randomAlphaOfLength(10);
            String inferenceId = randomIdentifier();

            createInferenceEndpoint(semanticTextFieldTaskType, inferenceId, inferenceEndpointServiceSettings);
            semanticTextFields.put(fieldName, inferenceId);
        }

        XContentBuilder mapping = IntegrationTestUtils.generateSemanticTextMapping(semanticTextFields);
        assertAcked(prepareCreate(INDEX_NAME).setMapping(mapping));

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String semanticTextField : semanticTextFields.keySet()) {
            Map<String, Object> source = Map.of(semanticTextField, randomAlphaOfLength(10));
            DocWriteResponse docWriteResponse = client().prepareIndex(INDEX_NAME).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(docWriteResponse.getResult(), is(DocWriteResponse.Result.CREATED));

            boolQuery.should(clauseGenerator.apply(semanticTextField, randomAlphaOfLength(10)));
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQuery).size(clauseCount);
        SearchRequest searchRequest = new SearchRequest(new String[] { INDEX_NAME }, searchSourceBuilder);
        assertResponse(client().search(searchRequest), response -> {
            assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) clauseCount));
        });
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
        inferenceIds.put(inferenceId, taskType);
    }

    private static Map<String, Object> getServiceSettings(TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> SPARSE_EMBEDDING_SERVICE_SETTINGS;
            case TEXT_EMBEDDING -> TEXT_EMBEDDING_SERVICE_SETTINGS;
            default -> throw new IllegalArgumentException("Unhandled task type [" + taskType + "]");
        };
    }
}
