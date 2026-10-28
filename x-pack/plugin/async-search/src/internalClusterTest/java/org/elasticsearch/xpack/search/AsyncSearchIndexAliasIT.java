/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.async.AsyncResultsIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.XPackPlugin.ASYNC_RESULTS_INDEX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

/**
 * Makes .async-search index an alias pointing to another index and ensures it's cleaned up
 */
public class AsyncSearchIndexAliasIT extends AsyncSearchIntegTestCase {

    private static final String BACKING_INDEX = ASYNC_RESULTS_INDEX + "-000001";

    public static class AsyncResultsWithAliasPlugin extends AsyncResultsIndexPlugin {

        public AsyncResultsWithAliasPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings unused) {
            SystemIndexDescriptor original = AsyncTaskIndexService.getSystemIndexDescriptor();
            return List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(original.getIndexPattern())
                    .setDescription(original.getDescription())
                    .setPrimaryIndex(BACKING_INDEX)
                    .setAliasName(ASYNC_RESULTS_INDEX)
                    .setMappings(original.getMappings())
                    .setSettings(original.getSettings())
                    .setOrigin(ASYNC_SEARCH_ORIGIN)
                    .build()
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            LocalStateCompositeXPackPlugin.class,
            AsyncSearch.class,
            AsyncResultsWithAliasPlugin.class,
            ReindexPlugin.class
        );
    }

    public void testSearchAsyncIndexIsAlias() throws Exception {
        String dataIndex = randomIndexName();
        createIndex(dataIndex, Settings.builder().put("index.number_of_shards", 1).build());

        int numDocs = randomIntBetween(1, 10);
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk(dataIndex).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            bulkRequestBuilder.add(prepareIndex(dataIndex).setSource("field", "value" + i));
        }
        bulkRequestBuilder.get();

        final SubmitAsyncSearchRequest submitRequest = new SubmitAsyncSearchRequest(
            new SearchSourceBuilder().query(new MatchAllQueryBuilder()),
            dataIndex
        ).setKeepOnCompletion(true).setKeepAlive(TimeValue.timeValueSeconds(2));

        int asyncSearches = randomIntBetween(1, 10);
        for (int i = 0; i < asyncSearches; i++) {
            AsyncSearchResponse submitResponse = submitAsyncSearch(submitRequest);
            String searchId = submitResponse.getId();
            try {
                assertNotNull(searchId);
            } finally {
                submitResponse.decRef();
            }
            ensureTaskNotRunning(searchId);
        }

        refresh(ASYNC_RESULTS_INDEX);
        assertAsyncSearchIndexIsAlias();
        assertAsyncSearchIndexHasNoDocuments();
    }

    private void assertAsyncSearchIndexIsAlias() {
        var aliasResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, ASYNC_RESULTS_INDEX).get();
        var aliases = aliasResponse.getAliases();
        assertThat(aliases, hasKey(BACKING_INDEX));
        List<AliasMetadata> aliasList = aliases.get(BACKING_INDEX);
        assertThat(aliasList, hasSize(1));
        assertThat(aliasList.get(0).alias(), equalTo(ASYNC_RESULTS_INDEX));
        assertTrue(aliasList.get(0).writeIndex());
    }

    private void assertAsyncSearchIndexHasNoDocuments() throws Exception {
        assertBusy(() -> {
            SearchResponse resp = client().prepareSearch(ASYNC_RESULTS_INDEX).setSize(0).get();
            try {
                assertThat(resp.getHits().getTotalHits().value(), equalTo(0L));
            } finally {
                resp.decRef();
            }
        });
    }
}
