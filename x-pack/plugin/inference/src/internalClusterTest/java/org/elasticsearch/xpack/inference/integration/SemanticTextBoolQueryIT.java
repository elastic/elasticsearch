/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1".
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SemanticTextBoolQueryIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test_index";
    private static final String INFERENCE_ID = "test_endpoint";
    private static final String INFERENCE_FIELD = "inference_field";

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

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateInferencePlugin.class,
            TestInferenceServicePlugin.class,
            ReindexPlugin.class,
            FakeMlPlugin.class
        );
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    public void testBoolQueryWithExistsOnSemanticTextField() throws Exception {
        IntegrationTestUtils.createInferenceEndpoint(
            client(),
            org.elasticsearch.inference.TaskType.TEXT_EMBEDDING,
            INFERENCE_ID,
            EMBEDDING_SERVICE_SETTINGS
        );

        assertAcked(
            prepareCreate(INDEX_NAME).setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject(INFERENCE_FIELD)
                    .field("type", "semantic_text")
                    .field("inference_id", INFERENCE_ID)
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        client().prepareIndex(INDEX_NAME)
            .setId("1")
            .setSource(Map.of(INFERENCE_FIELD, "value"))
            .setRefreshPolicy("true")
            .get();

        // Control case: semantic_text match returns the document.
        assertSearchHitCount(
            new SemanticQueryBuilder(INFERENCE_FIELD, "value"),
            1
        );

        // Control case: exists alone returns the document.
        assertSearchHitCount(
            QueryBuilders.existsQuery(INFERENCE_FIELD),
            1
        );

        // Regression case: exists in a must clause must not break semantic_text search.
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery()
            .must(new SemanticQueryBuilder(INFERENCE_FIELD, "value"))
            .must(QueryBuilders.existsQuery(INFERENCE_FIELD));

        assertSearchHitCount(mustQuery, 1);

        // Regression case: exists in filter must not break semantic_text search.
        BoolQueryBuilder filterQuery = QueryBuilders.boolQuery()
            .must(new SemanticQueryBuilder(INFERENCE_FIELD, "value"))
            .filter(QueryBuilders.existsQuery(INFERENCE_FIELD));

        assertSearchHitCount(filterQuery, 1);
    }

    private void assertSearchHitCount(QueryBuilder query, long expected) throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().query(query).trackTotalHits(true);

        assertResponse(
            client().search(new SearchRequest(INDEX_NAME).source(source)),
            response -> assertHitCount(response, expected)
        );
    }
}
