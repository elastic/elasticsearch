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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 1, supportsDedicatedMasters = false)
public class SemanticTextFieldsApiMultiNodeIT extends ESIntegTestCase {

    private static final String INFERENCE_ID = "dense-test-endpoint";
    private static final String CONTENT_FIELD = "content";

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

    private String indexName;

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
        if (indexName != null) {
            IntegrationTestUtils.deleteIndex(client(), indexName);
        }
        IntegrationTestUtils.deleteInferenceEndpoint(client(), TaskType.TEXT_EMBEDDING, INFERENCE_ID);
    }

    public void testFetchInferenceFieldsViaFieldsApi() throws Exception {
        IntegrationTestUtils.createInferenceEndpoint(client(), TaskType.TEXT_EMBEDDING, INFERENCE_ID, DENSE_SERVICE_SETTINGS);

        indexName = randomIdentifier();
        int numDataNodes = internalCluster().numDataNodes();
        Settings indexSettings = Settings.builder().put("index.number_of_shards", numDataNodes).put("index.number_of_replicas", 0).build();

        XContentBuilder mapping = IntegrationTestUtils.generateSemanticTextMapping(Map.of(CONTENT_FIELD, INFERENCE_ID));
        assertAcked(prepareCreate(indexName).setSettings(indexSettings).setMapping(mapping));

        BulkRequestBuilder bulk = client().prepareBulk(indexName);
        bulk.add(client().prepareIndex(indexName).setSource(Map.of(CONTENT_FIELD, "semantic search with dense embeddings")));
        bulk.add(client().prepareIndex(indexName).setSource(Map.of(CONTENT_FIELD, "vector similarity for document retrieval")));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.get(TEST_REQUEST_TIMEOUT);
        ensureGreen(indexName);

        SearchRequest request = new SearchRequest(new String[] { indexName }).source(
            new SearchSourceBuilder().query(matchAllQuery()).fetchField(InferenceMetadataFieldsMapper.NAME)
        );

        // Issue the search from the coordinating-only node: it holds no shards, so every hit
        // must be serialized from a data node across the transport layer.
        assertResponse(internalCluster().coordOnlyNodeClient().search(request), response -> {
            assertThat("Expected no shard failures, but got: " + response.getFailedShards(), response.getFailedShards(), equalTo(0));
            assertThat("All shards should have succeeded", response.getSuccessfulShards(), equalTo(response.getTotalShards()));
            assertThat("Expected non-empty hits", response.getHits().getHits().length, greaterThan(0));

            for (var hit : response.getHits().getHits()) {
                var inferenceField = hit.field(InferenceMetadataFieldsMapper.NAME);
                assertThat("Each hit should have an _inference_fields DocumentField", inferenceField, notNullValue());

                List<Object> values = inferenceField.getValues();
                assertThat("_inference_fields should have exactly one value (a map)", values.size(), equalTo(1));
                assertThat("_inference_fields value should be a Map", values.get(0), instanceOf(Map.class));

                @SuppressWarnings("unchecked")
                Map<String, Object> inferenceMap = (Map<String, Object>) values.get(0);
                assertThat(
                    "The _inference_fields map should contain an entry for [" + CONTENT_FIELD + "]",
                    inferenceMap,
                    hasKey(CONTENT_FIELD)
                );

                assertThat(inferenceMap.get(CONTENT_FIELD), instanceOf(SemanticTextField.class));
                SemanticTextField semanticTextField = (SemanticTextField) inferenceMap.get(CONTENT_FIELD);
                assertThat(semanticTextField.fieldName(), equalTo(CONTENT_FIELD));

                SemanticTextField.InferenceResult inference = semanticTextField.inference();
                assertThat(inference.inferenceId(), equalTo(INFERENCE_ID));

                MinimalServiceSettings modelSettings = inference.modelSettings();
                assertThat(modelSettings, notNullValue());
                assertThat(modelSettings.taskType(), equalTo(TaskType.TEXT_EMBEDDING));
                assertThat(modelSettings.dimensions(), equalTo(4));
                assertThat(modelSettings.similarity(), equalTo(SimilarityMeasure.COSINE));

                assertThat(inference.chunks(), hasKey(CONTENT_FIELD));
                List<SemanticTextField.Chunk> chunks = inference.chunks().get(CONTENT_FIELD);
                assertThat(chunks, not(empty()));
                for (SemanticTextField.Chunk chunk : chunks) {
                    assertThat("Each chunk should carry raw embeddings", chunk.rawEmbeddings(), notNullValue());
                    assertThat(chunk.rawEmbeddings().length(), greaterThan(0));
                }
            }
        });
    }
}
