/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
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
        Future<SearchResponse>[] responses = new Future[numSearches];
        SearchType searchType = randomFrom(SearchType.DEFAULT, SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH);
        logger.info("search type is {}", searchType);
        for (int i = 0; i < numSearches; i++) {
            responses[i] = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .setSearchType(searchType)
                    .execute();
        }
        for (int i = 0; i < numSearches; i++) {
            try {
                responses[i].get();
            } catch (Exception t) {
            }
        }
        assertBusy(
            () -> assertThat(client().admin().indices().prepareStats().get().getTotal().getSearch().getOpenContexts(), equalTo(0L)),
            1, TimeUnit.SECONDS);
    }
}
