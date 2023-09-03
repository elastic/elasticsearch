/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Queue;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class SearchShardsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testBasic() {
        int indicesWithData = between(1, 10);
        for (int i = 0; i < indicesWithData; i++) {
            String index = "index-with-data-" + i;
            ElasticsearchAssertions.assertAcked(
                indicesAdmin().prepareCreate(index).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            );
            int numDocs = randomIntBetween(1, 10);
            for (int j = 0; j < numDocs; j++) {
                client().prepareIndex(index).setSource("value", i).setId(Integer.toString(i)).get();
            }
            indicesAdmin().prepareRefresh(index).get();
        }
        int indicesWithoutData = between(1, 10);
        for (int i = 0; i < indicesWithoutData; i++) {
            String index = "index-without-data-" + i;
            ElasticsearchAssertions.assertAcked(
                indicesAdmin().prepareCreate(index).setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            );
        }
        // Range query
        {
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("value").from(0).includeLower(true);
            var request = new SearchShardsRequest(
                new String[] { "index-*" },
                SearchRequest.DEFAULT_INDICES_OPTIONS,
                rangeQuery,
                null,
                null,
                randomBoolean(),
                randomBoolean() ? null : randomAlphaOfLength(10)
            );
            var resp = client().execute(SearchShardsAction.INSTANCE, request).actionGet();
            assertThat(resp.getGroups(), hasSize(indicesWithData + indicesWithoutData));
            int skipped = 0;
            for (SearchShardsGroup g : resp.getGroups()) {
                String indexName = g.shardId().getIndexName();
                assertThat(g.allocatedNodes(), not(empty()));
                if (indexName.contains("without")) {
                    assertTrue(g.skipped());
                    skipped++;
                } else {
                    assertFalse(g.skipped());
                }
            }
            assertThat(skipped, equalTo(indicesWithoutData));
        }
        // Match all
        {
            MatchAllQueryBuilder matchAll = new MatchAllQueryBuilder();
            var request = new SearchShardsRequest(
                new String[] { "index-*" },
                SearchRequest.DEFAULT_INDICES_OPTIONS,
                matchAll,
                null,
                null,
                randomBoolean(),
                randomBoolean() ? null : randomAlphaOfLength(10)
            );
            SearchShardsResponse resp = client().execute(SearchShardsAction.INSTANCE, request).actionGet();
            assertThat(resp.getGroups(), hasSize(indicesWithData + indicesWithoutData));
            for (SearchShardsGroup g : resp.getGroups()) {
                assertFalse(g.skipped());
            }
        }
    }

    public void testRandom() {
        int numIndices = randomIntBetween(1, 10);
        for (int i = 0; i < numIndices; i++) {
            String index = "index-" + i;
            ElasticsearchAssertions.assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            );
            int numDocs = randomIntBetween(10, 1000);
            for (int j = 0; j < numDocs; j++) {
                client().prepareIndex(index).setSource("value", i).setId(Integer.toString(i)).get();
            }
            indicesAdmin().prepareRefresh(index).get();
        }
        int iterations = iterations(2, 10);
        for (int i = 0; i < iterations; i++) {
            long from = randomLongBetween(1, 100);
            long to = randomLongBetween(from, from + 100);
            String preference = randomBoolean() ? null : randomAlphaOfLength(10);
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("value").from(from).to(to).includeUpper(true).includeLower(true);
            SearchRequest searchRequest = new SearchRequest().indices("index-*").source(new SearchSourceBuilder().query(rangeQuery));
            searchRequest.setPreFilterShardSize(1);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            var searchShardsRequest = new SearchShardsRequest(
                new String[] { "index-*" },
                SearchRequest.DEFAULT_INDICES_OPTIONS,
                rangeQuery,
                null,
                preference,
                randomBoolean(),
                randomBoolean() ? null : randomAlphaOfLength(10)
            );
            var searchShardsResponse = client().execute(SearchShardsAction.INSTANCE, searchShardsRequest).actionGet();

            assertThat(searchShardsResponse.getGroups(), hasSize(searchResponse.getTotalShards()));
            long skippedShards = searchShardsResponse.getGroups().stream().filter(SearchShardsGroup::skipped).count();
            assertThat(skippedShards, equalTo((long) searchResponse.getSkippedShards()));
        }
    }

    public void testNoCanMatchWithoutQuery() {
        Queue<CanMatchNodeRequest> canMatchRequests = ConcurrentCollections.newQueue();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService ts = (MockTransportService) transportService;
            ts.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(SearchTransportService.QUERY_CAN_MATCH_NODE_NAME)) {
                    canMatchRequests.add((CanMatchNodeRequest) request);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        try {
            int numIndices = randomIntBetween(1, 10);
            int totalShards = 0;
            for (int i = 0; i < numIndices; i++) {
                String index = "index-" + i;
                int numShards = between(1, 5);
                ElasticsearchAssertions.assertAcked(
                    indicesAdmin().prepareCreate(index)
                        .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards))
                );
                totalShards += numShards;
                int numDocs = randomIntBetween(10, 100);
                for (int j = 0; j < numDocs; j++) {
                    client().prepareIndex(index).setSource("value", i).setId(Integer.toString(i)).get();
                }
                indicesAdmin().prepareRefresh(index).get();
            }
            SearchShardsRequest request = new SearchShardsRequest(
                new String[] { "index-*" },
                IndicesOptions.LENIENT_EXPAND_OPEN,
                randomBoolean() ? new MatchAllQueryBuilder() : null,
                null,
                null,
                randomBoolean(),
                null
            );
            SearchShardsResponse resp = client().execute(SearchShardsAction.INSTANCE, request).actionGet();
            assertThat(resp.getGroups(), hasSize(totalShards));
            for (SearchShardsGroup group : resp.getGroups()) {
                assertFalse(group.skipped());
            }
            assertThat(canMatchRequests, emptyIterable());
        } finally {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                ((MockTransportService) transportService).clearAllRules();
            }
        }
    }
}
