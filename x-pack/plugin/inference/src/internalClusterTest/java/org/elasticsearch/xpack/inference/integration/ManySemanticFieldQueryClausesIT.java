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
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InferenceStringTests;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.vectors.EmbeddingQueryVectorBuilder;
import org.junit.After;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ManySemanticFieldQueryClausesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test_index";

    private static final Map<String, Object> EMBEDDING_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        4,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    private static final Map<String, String> IMAGE = Map.of(
        "type",
        "image",
        "value",
        "data:image/jpeg;base64,Y2F0IG9uIGEgd2luZG93c2lsbA=="
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
        manyQueryClausesTestCase(
            randomIntBetween(18, 24),
            (fieldName, inferenceId) -> new SemanticQueryBuilder(fieldName, randomAlphaOfLength(10))
        );
    }

    public void testManyMatchQueryClauses() throws Exception {
        manyQueryClausesTestCase(
            randomIntBetween(18, 24),
            (fieldName, inferenceId) -> new MatchQueryBuilder(fieldName, randomAlphaOfLength(10))
        );
    }

    public void testManyKnnQueryClauses() throws Exception {
        int clauseCount = randomIntBetween(18, 24);
        manyQueryClausesTestCase(
            clauseCount,
            (fieldName, inferenceId) -> new KnnVectorQueryBuilder(
                fieldName,
                new EmbeddingQueryVectorBuilder(inferenceId, randomInferenceStringGroup(), null),
                clauseCount,
                clauseCount * 10,
                null,
                null
            )
        );
    }

    @FunctionalInterface
    interface ClauseGenerator {
        QueryBuilder generate(String fieldName, String inferenceId);
    }

    private void manyQueryClausesTestCase(int clauseCount, ClauseGenerator clauseGenerator) throws Exception {
        Map<String, String> semanticFields = new HashMap<>(clauseCount);
        for (int i = 0; i < clauseCount; i++) {
            String fieldName = randomAlphaOfLength(10);
            String inferenceId = randomIdentifier();
            createInferenceEndpoint(inferenceId);
            semanticFields.put(fieldName, inferenceId);
        }

        XContentBuilder mapping = IntegrationTestUtils.generateSemanticMapping(semanticFields);
        assertAcked(prepareCreate(INDEX_NAME).setMapping(mapping));

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (var entry : semanticFields.entrySet()) {
            String fieldName = entry.getKey();
            String inferenceId = entry.getValue();

            Object fieldValue = randomBoolean() ? randomAlphaOfLength(10) : IMAGE;
            Map<String, Object> source = Map.of(fieldName, fieldValue);
            DocWriteResponse docWriteResponse = client().prepareIndex(INDEX_NAME).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(docWriteResponse.getResult(), is(DocWriteResponse.Result.CREATED));

            boolQuery.should(clauseGenerator.generate(fieldName, inferenceId));
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQuery).size(clauseCount);
        SearchRequest searchRequest = new SearchRequest(new String[] { INDEX_NAME }, searchSourceBuilder);
        assertResponse(client().search(searchRequest), response -> {
            assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) clauseCount));
        });
    }

    private void createInferenceEndpoint(String inferenceId) throws Exception {
        IntegrationTestUtils.createInferenceEndpoint(client(), TaskType.EMBEDDING, inferenceId, EMBEDDING_SERVICE_SETTINGS);
        inferenceIds.put(inferenceId, TaskType.EMBEDDING);
    }

    private static InferenceStringGroup randomInferenceStringGroup() {
        if (randomBoolean()) {
            return new InferenceStringGroup(randomAlphaOfLength(10));
        } else {
            return new InferenceStringGroup(InferenceStringTests.createRandomUsingDataTypes(EnumSet.of(DataType.IMAGE)));
        }
    }
}
