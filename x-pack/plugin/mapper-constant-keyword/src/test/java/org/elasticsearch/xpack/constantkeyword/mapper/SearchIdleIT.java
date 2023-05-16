/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class SearchIdleIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ConstantKeywordMapperPlugin.class);
    }

    public void testSearchIdleConstantKeywordMatchNoIndex() throws InterruptedException {
        // GIVEN
        final String idleIndex = "test1";
        final String activeIndex = "test2";
        // NOTE: we need many shards because shard pre-filtering and the "can match" phase
        // are executed only if we have enough shards.
        int idleIndexShardsCount = 60;
        int activeIndexShardsCount = 70;
        assertAcked(createIndex(idleIndex, idleIndexShardsCount));
        assertAcked(createIndex(activeIndex, activeIndexShardsCount));
        assertAcked(
            createIndexMapping(idleIndex, "constant_keyword", "type=constant_keyword,value=constant_value1", "keyword", "type=keyword")
        );
        assertAcked(
            createIndexMapping(activeIndex, "constant_keyword", "type=constant_keyword,value=constant_value2", "keyword", "type=keyword")
        );

        assertEquals(RestStatus.CREATED, client().prepareIndex(idleIndex).setSource("keyword", randomAlphaOfLength(10)).get().status());
        assertEquals(RestStatus.CREATED, client().prepareIndex(activeIndex).setSource("keyword", randomAlphaOfLength(10)).get().status());
        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh(idleIndex, activeIndex).get().getStatus());

        waitUntil(
            () -> Arrays.stream(client().admin().indices().prepareStats(idleIndex, activeIndex).get().getShards())
                .allMatch(ShardStats::isSearchIdle),
            2,
            TimeUnit.SECONDS
        );

        final IndicesStatsResponse statsResponse = client().admin().indices().prepareStats("test*").get();
        Arrays.stream(statsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.isSearchIdle()));
        Arrays.stream(statsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.getSearchIdleTime() >= 100));

        // WHEN
        final SearchResponse searchResponse = search("test*", "constant_keyword", randomAlphaOfLength(5));
        assertEquals(RestStatus.OK, searchResponse.status());
        // NOTE: we need an empty result from at least one shard
        assertEquals(idleIndexShardsCount + activeIndexShardsCount - 1, searchResponse.getSkippedShards());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(0, searchResponse.getHits().getHits().length);

        // THEN
        final IndicesStatsResponse afterStatsResponse = client().admin().indices().prepareStats("test*").get();
        final List<ShardStats> activeShards = Arrays.stream(afterStatsResponse.getShards()).filter(x -> x.isSearchIdle() == false).toList();
        // NOTE: we need an empty result from at least one shard
        assertEquals(1, activeShards.size());
    }

    public void testSearchIdleConstantKeywordMatchOneIndex() throws InterruptedException {
        // GIVEN
        final String idleIndex = "test1";
        final String activeIndex = "test2";
        // NOTE: we need many shards because shard pre-filtering and the "can match" phase
        // are executed only if we have enough shards.
        int idleIndexShardsCount = 60;
        int activeIndexShardsCount = 70;
        assertAcked(createIndex(idleIndex, idleIndexShardsCount));
        assertAcked(createIndex(activeIndex, activeIndexShardsCount));
        assertAcked(
            createIndexMapping(idleIndex, "constant_keyword", "type=constant_keyword,value=constant_value1", "keyword", "type=keyword")
        );
        assertAcked(
            createIndexMapping(activeIndex, "constant_keyword", "type=constant_keyword,value=constant_value2", "keyword", "type=keyword")
        );

        assertEquals(RestStatus.CREATED, client().prepareIndex(idleIndex).setSource("keyword", randomAlphaOfLength(10)).get().status());
        assertEquals(RestStatus.CREATED, client().prepareIndex(activeIndex).setSource("keyword", randomAlphaOfLength(10)).get().status());
        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh(idleIndex, activeIndex).get().getStatus());

        waitUntil(
            () -> Arrays.stream(client().admin().indices().prepareStats(idleIndex, activeIndex).get().getShards())
                .allMatch(ShardStats::isSearchIdle),
            2,
            TimeUnit.SECONDS
        );

        final IndicesStatsResponse statsResponse = client().admin().indices().prepareStats("test*").get();
        Arrays.stream(statsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.isSearchIdle()));
        Arrays.stream(statsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.getSearchIdleTime() >= 100));

        // WHEN
        final SearchResponse searchResponse = search("test*", "constant_keyword", "constant_value2");
        assertEquals(RestStatus.OK, searchResponse.status());
        assertEquals(idleIndexShardsCount, searchResponse.getSkippedShards());
        assertEquals(0, searchResponse.getFailedShards());
        Arrays.stream(searchResponse.getHits().getHits()).forEach(searchHit -> assertEquals("test2", searchHit.getIndex()));

        // THEN
        final IndicesStatsResponse idleIndexStats = client().admin().indices().prepareStats(idleIndex).get();
        Arrays.stream(idleIndexStats.getShards()).forEach(shardStats -> assertTrue(shardStats.isSearchIdle()));

        final IndicesStatsResponse activeIndexStats = client().admin().indices().prepareStats(activeIndex).get();
        Arrays.stream(activeIndexStats.getShards()).forEach(shardStats -> assertFalse(shardStats.isSearchIdle()));
    }

    public void testSearchIdleConstantKeywordMatchTwoIndices() throws InterruptedException {
        // GIVEN
        final String idleIndex = "test1";
        final String activeIndex = "test2";
        // NOTE: we need many shards because shard pre-filtering and the "can match" phase
        // are executed only if we have enough shards.
        int idleIndexShardsCount = 60;
        int activeIndexShardsCount = 70;
        assertAcked(createIndex(idleIndex, idleIndexShardsCount));
        assertAcked(createIndex(activeIndex, activeIndexShardsCount));
        assertAcked(createIndexMapping(idleIndex, "constant_keyword", "type=constant_keyword,value=constant", "keyword", "type=keyword"));
        assertAcked(createIndexMapping(activeIndex, "constant_keyword", "type=constant_keyword,value=constant", "keyword", "type=keyword"));

        assertEquals(RestStatus.CREATED, client().prepareIndex(idleIndex).setSource("keyword", randomAlphaOfLength(10)).get().status());
        assertEquals(RestStatus.CREATED, client().prepareIndex(activeIndex).setSource("keyword", randomAlphaOfLength(10)).get().status());
        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh(idleIndex, activeIndex).get().getStatus());

        waitUntil(
            () -> Arrays.stream(client().admin().indices().prepareStats(idleIndex, activeIndex).get().getShards())
                .allMatch(ShardStats::isSearchIdle),
            2,
            TimeUnit.SECONDS
        );

        final IndicesStatsResponse beforeStatsResponse = client().admin().indices().prepareStats("test*").get();
        Arrays.stream(beforeStatsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.isSearchIdle()));
        Arrays.stream(beforeStatsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.getSearchIdleTime() >= 100));

        // WHEN
        final SearchResponse searchResponse = search("test*", "constant_keyword", "constant");
        assertEquals(RestStatus.OK, searchResponse.status());
        assertEquals(0, searchResponse.getSkippedShards());
        assertEquals(0, searchResponse.getFailedShards());
        assertArrayEquals(
            new String[] { "test1", "test2" },
            Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getIndex).sorted().toArray()
        );

        // THEN
        final IndicesStatsResponse afterStatsResponse = client().admin().indices().prepareStats("test*").get();
        Arrays.stream(afterStatsResponse.getShards()).forEach(shardStats -> assertFalse(shardStats.isSearchIdle()));
    }

    public void testSearchIdleWildcardQueryMatchOneIndex() throws InterruptedException {
        // GIVEN
        final String idleIndex = "test1";
        final String activeIndex = "test2";
        // NOTE: we need many shards because shard pre-filtering and the "can match" phase
        // are executed only if we have enough shards.
        int idleIndexShardsCount = 60;
        int activeIndexShardsCount = 70;
        assertAcked(createIndex(idleIndex, idleIndexShardsCount));
        assertAcked(createIndex(activeIndex, activeIndexShardsCount));
        assertAcked(createIndexMapping(idleIndex, "constant_keyword", "type=constant_keyword,value=test1_value"));
        assertAcked(createIndexMapping(activeIndex, "constant_keyword", "type=constant_keyword,value=test2_value"));

        assertEquals(RestStatus.CREATED, client().prepareIndex(idleIndex).setSource("keyword", "value").get().status());
        assertEquals(RestStatus.CREATED, client().prepareIndex(activeIndex).setSource("keyword", "value").get().status());
        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh(idleIndex, activeIndex).get().getStatus());

        waitUntil(
            () -> Arrays.stream(client().admin().indices().prepareStats(idleIndex, activeIndex).get().getShards())
                .allMatch(ShardStats::isSearchIdle),
            2,
            TimeUnit.SECONDS
        );

        final IndicesStatsResponse statsResponse = client().admin().indices().prepareStats("test*").get();
        Arrays.stream(statsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.isSearchIdle()));
        Arrays.stream(statsResponse.getShards()).forEach(shardStats -> assertTrue(shardStats.getSearchIdleTime() >= 100));

        // WHEN
        final SearchResponse searchResponse = client().prepareSearch("test*")
            .setQuery(new WildcardQueryBuilder("constant_keyword", "test2*"))
            .get();
        assertEquals(RestStatus.OK, searchResponse.status());
        assertEquals(idleIndexShardsCount, searchResponse.getSkippedShards());
        assertEquals(0, searchResponse.getFailedShards());
        Arrays.stream(searchResponse.getHits().getHits()).forEach(searchHit -> assertEquals("test2", searchHit.getIndex()));

        // THEN
        final IndicesStatsResponse idleIndexStats = client().admin().indices().prepareStats(idleIndex).get();
        Arrays.stream(idleIndexStats.getShards()).forEach(shardStats -> assertTrue(shardStats.isSearchIdle()));

        final IndicesStatsResponse activeIndexStats = client().admin().indices().prepareStats(activeIndex).get();
        Arrays.stream(activeIndexStats.getShards()).forEach(shardStats -> assertFalse(shardStats.isSearchIdle()));
    }

    private CreateIndexResponse createIndex(final String idleIndex, int shardsCount) {
        return client().admin()
            .indices()
            .prepareCreate(idleIndex)
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), "500ms")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardsCount)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            )
            .get();
    }

    private AcknowledgedResponse createIndexMapping(final String indexName, String... source) {
        return client().admin().indices().preparePutMapping(indexName).setSource(source).get();
    }

    private SearchResponse search(final String index, final String field, final String value) {
        return client().prepareSearch(index).setQuery(new MatchPhraseQueryBuilder(field, value)).get();
    }
}
