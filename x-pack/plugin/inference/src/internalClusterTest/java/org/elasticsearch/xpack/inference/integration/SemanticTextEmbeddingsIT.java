/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.index.IndexVersions.EXCLUDE_SOURCE_VECTORS_DEFAULT;
import static org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper.USE_NEW_SEMANTIC_TEXT_FORMAT_BY_DEFAULT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 3, maxNumDataNodes = 5)
public class SemanticTextEmbeddingsIT extends ESIntegTestCase {
    private final String indexName = randomIdentifier();
    private final Map<String, TaskType> inferenceIds = new HashMap<>();

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

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @After
    public void cleanUp() {
        assertAcked(
            safeGet(
                client().admin()
                    .indices()
                    .prepareDelete(indexName)
                    .setIndicesOptions(
                        IndicesOptions.builder().concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true)).build()
                    )
                    .execute()
            )
        );

        for (var entry : inferenceIds.entrySet()) {
            assertAcked(
                safeGet(
                    client().execute(
                        DeleteInferenceEndpointAction.INSTANCE,
                        new DeleteInferenceEndpointAction.Request(entry.getKey(), entry.getValue(), true, false)
                    )
                )
            );
        }
    }

    // TODO: Complete test once fix is implemented
    public void testSourceDisabledAndIncludeVectors() throws Exception {
        // Get a random index version after when we default to the new semantic text format, and before we exclude vectors in source by
        // default
        final IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            USE_NEW_SEMANTIC_TEXT_FORMAT_BY_DEFAULT,
            IndexVersionUtils.getPreviousVersion(EXCLUDE_SOURCE_VECTORS_DEFAULT)
        );

        final String sparseEmbeddingInferenceId = randomIdentifier();
        final String textEmbeddingInferenceId = randomIdentifier();
        createInferenceEndpoint(TaskType.SPARSE_EMBEDDING, sparseEmbeddingInferenceId, SPARSE_EMBEDDING_SERVICE_SETTINGS);
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, textEmbeddingInferenceId, TEXT_EMBEDDING_SERVICE_SETTINGS);

        final String sparseEmbeddingField = randomIdentifier();
        final String textEmbeddingField = randomIdentifier();
        XContentBuilder mappings = generateMapping(
            Map.of(sparseEmbeddingField, sparseEmbeddingInferenceId, textEmbeddingField, textEmbeddingInferenceId)
        );

        assertAcked(prepareCreate(indexName).setSettings(generateIndexSettings(indexVersion)).setMapping(mappings));
        indexDocuments(sparseEmbeddingField, 10);
        indexDocuments(textEmbeddingField, 10);

        QueryBuilder sparseEmbeddingFieldQuery = new SemanticQueryBuilder(sparseEmbeddingField, randomAlphaOfLength(10));
        assertSearchResponse(
            sparseEmbeddingFieldQuery,
            10,
            request -> request.source().fetchSource(false).fetchField(sparseEmbeddingField),
            response -> {
                for (SearchHit hit : response.getHits()) {
                    hit.getDocumentFields();
                }
            }
        );
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        final String service = switch (taskType) {
            case TEXT_EMBEDDING -> TestDenseInferenceServiceExtension.TestInferenceService.NAME;
            case SPARSE_EMBEDDING -> TestSparseInferenceServiceExtension.TestInferenceService.NAME;
            default -> throw new IllegalArgumentException("Unhandled task type [" + taskType + "]");
        };

        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", service);
            builder.field("service_settings", serviceSettings);
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        PutInferenceModelAction.Request request = new PutInferenceModelAction.Request(
            taskType,
            inferenceId,
            content,
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );
        var responseFuture = client().execute(PutInferenceModelAction.INSTANCE, request);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(), equalTo(inferenceId));

        inferenceIds.put(inferenceId, taskType);
    }

    private Settings generateIndexSettings(IndexVersion indexVersion) {
        int numDataNodes = internalCluster().numDataNodes();
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numDataNodes)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    private void indexDocuments(String field, int count) {
        for (int i = 0; i < count; i++) {
            Map<String, Object> source = Map.of(field, randomAlphaOfLength(10));
            DocWriteResponse response = client().prepareIndex(indexName).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        }

        client().admin().indices().prepareRefresh(indexName).get();
    }

    private void assertSearchResponse(
        QueryBuilder queryBuilder,
        long expectedHits,
        Consumer<SearchRequest> searchRequestModifier,
        Consumer<SearchResponse> searchResponseValidator
    ) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
        SearchRequest searchRequest = new SearchRequest(new String[] { indexName }, searchSourceBuilder);
        if (searchRequestModifier != null) {
            searchRequestModifier.accept(searchRequest);
        }

        assertResponse(client().search(searchRequest), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(expectedHits));
            searchResponseValidator.accept(response);
        });
    }

    private static XContentBuilder generateMapping(Map<String, String> semanticTextFields) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (var entry : semanticTextFields.entrySet()) {
            mapping.startObject(entry.getKey());
            mapping.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
            mapping.field("inference_id", entry.getValue());
            mapping.endObject();
        }
        mapping.endObject().endObject();

        return mapping;
    }

    public static class FakeMlPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return new MlInferenceNamedXContentProvider().getNamedWriteables();
        }
    }
}
