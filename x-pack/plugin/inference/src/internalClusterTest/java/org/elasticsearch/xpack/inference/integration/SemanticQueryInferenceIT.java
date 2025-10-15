/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.inference.integration.InferenceUtils.createInferenceEndpoint;
import static org.hamcrest.CoreMatchers.equalTo;

public class SemanticQueryInferenceIT extends ESIntegTestCase {
    private static final Map<String, Object> SPARSE_EMBEDDING_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateInferencePlugin.class,
            TestInferenceServicePlugin.class,
            ReindexPlugin.class,
            SemanticTextInferenceFieldsIT.FakeMlPlugin.class
        );
    }

    public void testManyInferenceRequests() throws Exception {
        final String semanticTextFieldName = randomAlphaOfLength(10);
        int indexCount = randomIntBetween(16, 20);
        List<String> indices = new ArrayList<>(indexCount);
        for (int i = 0; i < indexCount; i++) {
            String indexName = randomIdentifier();
            String inferenceId = randomIdentifier();
            XContentBuilder mapping = generateMapping(semanticTextFieldName, inferenceId);

            createInferenceEndpoint(client(), TaskType.SPARSE_EMBEDDING, inferenceId, SPARSE_EMBEDDING_SERVICE_SETTINGS);
            assertAcked(prepareCreate(indexName).setMapping(mapping));

            indices.add(indexName);
        }

        SemanticQueryBuilder query = new SemanticQueryBuilder(semanticTextFieldName, randomAlphaOfLength(10));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        SearchRequest searchRequest = new SearchRequest(indices.toArray(new String[0]), searchSourceBuilder);
        assertResponse(client().search(searchRequest), response -> {
            assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
            assertThat(response.getHits().getTotalHits().value(), equalTo(0L));
        });
    }

    private static XContentBuilder generateMapping(String semanticTextFieldName, String inferenceId) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(semanticTextFieldName)
            .field("type", SemanticTextFieldMapper.CONTENT_TYPE)
            .field("inference_id", inferenceId)
            .endObject()
            .endObject()
            .endObject();
    }
}
