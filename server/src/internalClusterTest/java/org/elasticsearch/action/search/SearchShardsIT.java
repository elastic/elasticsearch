/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class SearchShardsIT extends ESIntegTestCase {

    public void testBasic() {
        for (int i = 0; i < 6; i++) {
            String index = "index-with-data-" + i;
            ElasticsearchAssertions.assertAcked(
                admin().indices().prepareCreate(index).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            );
            int numDocs = randomIntBetween(1, 10);
            for (int j = 0; j < numDocs; j++) {
                client().prepareIndex(index).setSource("value", i).setId(Integer.toString(i)).get();
            }
            client().admin().indices().prepareRefresh(index).get();
        }
        for (int i = 0; i < 5; i++) {
            String index = "index-without-data-" + i;
            ElasticsearchAssertions.assertAcked(
                admin().indices().prepareCreate(index).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            );
        }
        // Range query
        {
            SearchRequest rangeQuery = new SearchRequest().indices("index-*")
                .source(new SearchSourceBuilder().query(new RangeQueryBuilder("value").from(0).includeLower(true)));
            SearchShardsResponse resp = client().execute(SearchShardsAction.INSTANCE, new SearchShardsRequest(rangeQuery)).actionGet();
            assertThat(resp.getGroups(), hasSize(11));
            int skipped = 0;
            for (SearchShardsGroup g : resp.getGroups()) {
                String indexName = g.shardId().getIndexName();
                assertThat(g.allocatedNodes(), not(empty()));
                assertTrue(g.preFiltered());
                if (indexName.contains("without")) {
                    assertTrue(g.skipped());
                    skipped++;
                } else {
                    assertFalse(g.skipped());
                }
            }
            assertThat(skipped, equalTo(5));
        }
        // Match all
        {
            SearchRequest matchAll = new SearchRequest().indices("index-*")
                .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
            SearchShardsResponse resp = client().execute(SearchShardsAction.INSTANCE, new SearchShardsRequest(matchAll)).actionGet();
            assertThat(resp.getGroups(), hasSize(11));
            for (SearchShardsGroup g : resp.getGroups()) {
                assertFalse(g.skipped());
                assertTrue(g.preFiltered());
            }
        }
    }

    public void testSearchShards() {
        int numIndices = randomIntBetween(1, 10);
        for (int i = 0; i < numIndices; i++) {
            String index = "index-" + i;
            ElasticsearchAssertions.assertAcked(
                admin().indices()
                    .prepareCreate(index)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            );
            int numDocs = randomIntBetween(10, 1000);
            for (int j = 0; j < numDocs; j++) {
                client().prepareIndex(index).setSource("value", i).setId(Integer.toString(i)).get();
            }
            client().admin().indices().prepareRefresh(index).get();
        }
        int iterations = iterations(2, 10);
        for (int i = 0; i < iterations; i++) {
            long from = randomLongBetween(1, 100);
            long to = randomLongBetween(from, from + 100);

            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("value").from(from).to(to).includeUpper(true).includeLower(true);
            SearchRequest searchRequest = new SearchRequest().indices("index-*").source(new SearchSourceBuilder().query(rangeQuery));
            searchRequest.setPreFilterShardSize(1);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            SearchShardsResponse searchShardsResponse = client().execute(
                SearchShardsAction.INSTANCE,
                new SearchShardsRequest(searchRequest)
            ).actionGet();

            assertThat(searchShardsResponse.getGroups(), hasSize(searchResponse.getTotalShards()));
            long skippedShards = searchShardsResponse.getGroups().stream().filter(SearchShardsGroup::skipped).count();
            assertThat(skippedShards, equalTo((long) searchResponse.getSkippedShards()));
        }
    }
}
