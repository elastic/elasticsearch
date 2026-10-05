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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
abstract class AbstractInferenceFieldsIT extends ESIntegTestCase {
    private final Map<String, TaskType> inferenceIds = new HashMap<>();
    private String currentIndexName = null;

    private static final Map<String, Object> SPARSE_EMBEDDING_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");
    private static final Map<String, Object> DENSE_EMBEDDING_SERVICE_SETTINGS = Map.of(
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
    private void cleanUp() {
        if (currentIndexName != null) {
            IntegrationTestUtils.deleteIndex(client(), currentIndexName);
        }
        for (var entry : inferenceIds.entrySet()) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), entry.getValue(), entry.getKey());
        }
    }

    abstract XContentBuilder generateMapping(Map<String, String> fieldNameToInferenceIdMap) throws IOException;

    abstract Object generateFieldValue();

    final void excludeInferenceFieldsFromSourceTestCase(
        Set<TaskType> taskTypes,
        IndexVersion minIndexVersion,
        IndexVersion maxIndexVersion,
        int iterations
    ) throws Exception {
        Map<String, String> fieldNameToInferenceIdMap = new HashMap<>();
        for (TaskType taskType : taskTypes) {
            String fieldName = randomIdentifier();
            String inferenceId = randomIdentifier();
            createInferenceEndpoint(taskType, inferenceId);
            fieldNameToInferenceIdMap.put(fieldName, inferenceId);
        }

        for (int i = 0; i < iterations; i++) {
            currentIndexName = randomIndexName();
            final IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(minIndexVersion, maxIndexVersion);
            final Settings indexSettings = generateIndexSettings(indexVersion);
            XContentBuilder mappings = generateMapping(fieldNameToInferenceIdMap);
            assertAcked(prepareCreate(currentIndexName).setSettings(indexSettings).setMapping(mappings));

            final int docCount = randomIntBetween(10, 50);
            for (String fieldName : fieldNameToInferenceIdMap.keySet()) {
                indexDocuments(currentIndexName, fieldName, docCount);
            }

            for (String fieldName : fieldNameToInferenceIdMap.keySet()) {
                QueryBuilder semanticQuery = new SemanticQueryBuilder(fieldName, randomAlphaOfLength(10));
                assertSearchResponse(semanticQuery, currentIndexName, indexSettings, docCount, request -> {
                    request.source().fetchSource(generateRandomFetchSourceContext()).fetchField(fieldName);
                }, response -> {
                    for (SearchHit hit : response.getHits()) {
                        Map<String, DocumentField> documentFields = hit.getDocumentFields();
                        assertThat(documentFields.size(), is(1));
                        assertThat(documentFields.containsKey(fieldName), is(true));
                    }
                });
            }

            IntegrationTestUtils.deleteIndex(client(), currentIndexName);
        }
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId) throws IOException {
        var serviceSettings = switch (taskType) {
            case TEXT_EMBEDDING, EMBEDDING -> DENSE_EMBEDDING_SERVICE_SETTINGS;
            case SPARSE_EMBEDDING -> SPARSE_EMBEDDING_SERVICE_SETTINGS;
            default -> throw new AssertionError("Unhandled task type: " + taskType);
        };
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
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

    private void indexDocuments(String indexName, String field, int count) {
        for (int i = 0; i < count; i++) {
            Map<String, Object> source = Map.of(field, generateFieldValue());
            DocWriteResponse response = client().prepareIndex(indexName).setSource(source).get(TEST_REQUEST_TIMEOUT);
            assertThat(response.getResult(), is(DocWriteResponse.Result.CREATED));
        }

        client().admin().indices().prepareRefresh(indexName).get();
    }

    private void assertSearchResponse(
        QueryBuilder queryBuilder,
        String indexName,
        Settings indexSettings,
        int expectedHits,
        @Nullable Consumer<SearchRequest> searchRequestModifier,
        @Nullable Consumer<SearchResponse> searchResponseValidator
    ) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder).size(expectedHits);
        SearchRequest searchRequest = new SearchRequest(new String[] { indexName }, searchSourceBuilder);
        if (searchRequestModifier != null) {
            searchRequestModifier.accept(searchRequest);
        }

        ExpectedSource expectedSource = getExpectedSource(indexSettings, searchRequest.source().fetchSource());
        assertResponse(client().search(searchRequest), response -> {
            assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) expectedHits));

            for (SearchHit hit : response.getHits()) {
                switch (expectedSource) {
                    case NONE -> assertThat(hit.getSourceAsMap(), nullValue());
                    case INFERENCE_FIELDS_EXCLUDED -> {
                        Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                        assertThat(sourceAsMap, notNullValue());
                        assertThat(sourceAsMap.containsKey(InferenceMetadataFieldsMapper.NAME), is(false));
                    }
                    case INFERENCE_FIELDS_INCLUDED -> {
                        Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                        assertThat(sourceAsMap, notNullValue());
                        assertThat(sourceAsMap.containsKey(InferenceMetadataFieldsMapper.NAME), is(true));
                    }
                }
            }

            if (searchResponseValidator != null) {
                searchResponseValidator.accept(response);
            }
        });
    }

    private static ExpectedSource getExpectedSource(Settings indexSettings, FetchSourceContext fetchSourceContext) {
        if (fetchSourceContext != null && fetchSourceContext.fetchSource() == false) {
            return ExpectedSource.NONE;
        } else if (InferenceMetadataFieldsMapper.isEnabled(indexSettings) == false) {
            return ExpectedSource.INFERENCE_FIELDS_EXCLUDED;
        }

        if (fetchSourceContext != null) {
            SourceFilter filter = fetchSourceContext.filter();
            if (filter != null) {
                if (Arrays.asList(filter.getExcludes()).contains(InferenceMetadataFieldsMapper.NAME)) {
                    return ExpectedSource.INFERENCE_FIELDS_EXCLUDED;
                } else if (filter.getIncludes().length > 0) {
                    return Arrays.asList(filter.getIncludes()).contains(InferenceMetadataFieldsMapper.NAME)
                        ? ExpectedSource.INFERENCE_FIELDS_INCLUDED
                        : ExpectedSource.INFERENCE_FIELDS_EXCLUDED;
                }
            }

            Boolean excludeInferenceFieldsExplicit = fetchSourceContext.excludeInferenceFields();
            if (excludeInferenceFieldsExplicit != null) {
                return excludeInferenceFieldsExplicit ? ExpectedSource.INFERENCE_FIELDS_EXCLUDED : ExpectedSource.INFERENCE_FIELDS_INCLUDED;
            }
        }

        return ExpectedSource.INFERENCE_FIELDS_EXCLUDED;
    }

    private static FetchSourceContext generateRandomFetchSourceContext() {
        FetchSourceContext fetchSourceContext = switch (randomIntBetween(0, 4)) {
            case 0 -> FetchSourceContext.FETCH_SOURCE;
            case 1 -> FetchSourceContext.FETCH_ALL_SOURCE;
            case 2 -> FetchSourceContext.FETCH_ALL_SOURCE_EXCLUDE_INFERENCE_FIELDS;
            case 3 -> FetchSourceContext.DO_NOT_FETCH_SOURCE;
            case 4 -> null;
            default -> throw new IllegalStateException("Unhandled randomized case");
        };

        if (fetchSourceContext != null && fetchSourceContext.fetchSource()) {
            String[] includes = null;
            String[] excludes = null;
            if (randomBoolean()) {
                // Randomly include a non-existent field to test explicit inclusion handling
                String field = randomBoolean() ? InferenceMetadataFieldsMapper.NAME : randomIdentifier();
                includes = new String[] { field };
            }
            if (randomBoolean()) {
                // Randomly exclude a non-existent field to test implicit inclusion handling
                String field = randomBoolean() ? InferenceMetadataFieldsMapper.NAME : randomIdentifier();
                excludes = new String[] { field };
            }

            if (includes != null || excludes != null) {
                fetchSourceContext = FetchSourceContext.of(
                    fetchSourceContext.fetchSource(),
                    fetchSourceContext.excludeVectors(),
                    fetchSourceContext.excludeInferenceFields(),
                    includes,
                    excludes
                );
            }
        }

        return fetchSourceContext;
    }

    private enum ExpectedSource {
        NONE,
        INFERENCE_FIELDS_EXCLUDED,
        INFERENCE_FIELDS_INCLUDED
    }
}
