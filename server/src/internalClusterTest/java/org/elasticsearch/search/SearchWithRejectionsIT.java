/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SearchWithRejectionsIT extends ESIntegTestCase {
    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", 1)
            .build();
    }

    public void testOpenContextsAfterRejections() throws Exception {
        createIndex("test");
        ensureGreen("test");
        final int docs = scaledRandomIntBetween(20, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().get();
        assertThat(indicesStats.getTotal().getSearch().getOpenContexts(), equalTo(0L));
        refresh();

        int numSearches = 10;
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Future<SearchResponse>[] responses = new Future[numSearches];
        SearchType searchType = randomFrom(SearchType.DEFAULT, SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH);
        logger.info("search type is {}", searchType);
        for (int i = 0; i < numSearches; i++) {
            responses[i] = client().prepareSearch().setQuery(matchAllQuery()).setSearchType(searchType).execute();
        }
        for (int i = 0; i < numSearches; i++) {
            try {
                responses[i].get();
            } catch (Exception t) {}
        }
        assertBusy(
            () -> assertThat(client().admin().indices().prepareStats().get().getTotal().getSearch().getOpenContexts(), equalTo(0L)),
            1,
            TimeUnit.SECONDS
        );
    }
}
