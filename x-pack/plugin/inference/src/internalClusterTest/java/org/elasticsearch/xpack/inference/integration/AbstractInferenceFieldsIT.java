/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

abstract class AbstractInferenceFieldsIT extends ESIntegTestCase {

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

    protected Settings generateIndexSettings(IndexVersion indexVersion) {
        int numDataNodes = internalCluster().numDataNodes();
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numDataNodes)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    protected void assertSearchResponse(
        String indexName,
        QueryBuilder queryBuilder,
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

    protected static FetchSourceContext generateRandomFetchSourceContext() {
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

    protected enum ExpectedSource {
        NONE,
        INFERENCE_FIELDS_EXCLUDED,
        INFERENCE_FIELDS_INCLUDED
    }
}
