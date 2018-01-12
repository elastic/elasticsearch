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

package org.elasticsearch.index.fieldstats;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class FieldStatsProviderRefreshTests extends ESSingleNodeTestCase {

    public void testQueryRewriteOnRefresh() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index").addMapping("type", "s", "type=text")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                .get());

        // Index some documents
        indexDocument("1", "d");
        indexDocument("2", "e");
        indexDocument("3", "f");
        refreshIndex();

        // check request cache stats are clean
        assertRequestCacheStats(0, 0);

        // Search for a range and check that it missed the cache (since its the
        // first time it has run)
        final SearchResponse r1 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("a").lte("g")).get();
        assertSearchResponse(r1);
        assertThat(r1.getHits().getTotalHits(), equalTo(3L));
        assertRequestCacheStats(0, 1);

        // Search again and check it hits the cache
        final SearchResponse r2 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("a").lte("g")).get();
        assertSearchResponse(r2);
        assertThat(r2.getHits().getTotalHits(), equalTo(3L));
        assertRequestCacheStats(1, 1);

        // Index some more documents in the query range and refresh
        indexDocument("4", "c");
        indexDocument("5", "g");
        refreshIndex();

        // Search again and check the request cache for another miss since request cache should be invalidated by refresh
        final SearchResponse r3 = client().prepareSearch("index").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                .setQuery(QueryBuilders.rangeQuery("s").gte("a").lte("g")).get();
        assertSearchResponse(r3);
        assertThat(r3.getHits().getTotalHits(), equalTo(5L));
        assertRequestCacheStats(1, 2);
    }

    private void assertRequestCacheStats(long expectedHits, long expectedMisses) {
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
                equalTo(expectedHits));
        assertThat(client().admin().indices().prepareStats("index").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
                equalTo(expectedMisses));
    }

    private void refreshIndex() {
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("index").get();
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(refreshResponse.getSuccessfulShards()));
    }

    private void indexDocument(String id, String sValue) {
        IndexResponse response = client().prepareIndex("index", "type", id).setSource("s", sValue).get();
        assertThat(response.status(), anyOf(equalTo(RestStatus.OK), equalTo(RestStatus.CREATED)));
    }
}
