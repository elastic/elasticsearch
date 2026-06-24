/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.FieldBasedRerankerIT;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.store.BytesReadHeaderIT.assertBytesReadHeader;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests that the bytes-read response header is set when the rank feature phase is exercised.
 */
public class BytesReadRankFeatureIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), FieldBasedRerankerIT.FieldBasedRerankerPlugin.class);
    }

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testRankFeaturePhaseSetsBytesReadHeader() throws InterruptedException {
        final String indexName = randomIndexName();
        assertAcked(
            prepareCreate(indexName).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("rankFeatureField", "type=float", "searchField", "type=text")
        );
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource("rankFeatureField", 0.1, "searchField", "A"),
            prepareIndex(indexName).setId("2").setSource("rankFeatureField", 0.2, "searchField", "B"),
            prepareIndex(indexName).setId("3").setSource("rankFeatureField", 0.3, "searchField", "C"),
            prepareIndex(indexName).setId("4").setSource("rankFeatureField", 0.4, "searchField", "D"),
            prepareIndex(indexName).setId("5").setSource("rankFeatureField", 0.5, "searchField", "E")
        );

        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.matchQuery("searchField", "A"))
                    .should(QueryBuilders.matchQuery("searchField", "B"))
                    .should(QueryBuilders.matchQuery("searchField", "C"))
                    .should(QueryBuilders.matchQuery("searchField", "D"))
                    .should(QueryBuilders.matchQuery("searchField", "E"))
            ).rankBuilder(new FieldBasedRerankerIT.FieldBasedRankBuilder(10, "rankFeatureField")).size(5)
        );

        long bytesRead = assertBytesReadHeader(request);
        assertThat(bytesRead, greaterThan(0L));
    }

    public void testRankFeaturePhaseEmptyDocIdsStillReportsDirectoryMetrics() throws Exception {
        final String indexName = randomIndexName();
        assertAcked(
            prepareCreate(indexName).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("rankFeatureField", "type=float", "searchField", "type=text")
        );
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource("rankFeatureField", 0.1, "searchField", "A"),
            prepareIndex(indexName).setId("2").setSource("rankFeatureField", 0.2, "searchField", "B")
        );

        String dataNode = clusterService().state()
            .nodes()
            .get(clusterService().state().routingTable().index(indexName).shard(0).primaryShard().currentNodeId())
            .getName();
        SearchService searchService = internalCluster().getInstance(SearchService.class, dataNode);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNode);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest(indexName).allowPartialSearchResults(true)
            .source(
                new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                    .rankBuilder(new FieldBasedRerankerIT.FieldBasedRankBuilder(5, "rankFeatureField"))
                    .size(5)
            );
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );
        SearchShardTask task = new SearchShardTask(0, "", "", "", null, Map.of());

        QuerySearchResult queryResult = null;
        RankFeatureResult rankResult = null;
        try {
            PlainActionFuture<SearchPhaseResult> queryFuture = new PlainActionFuture<>();
            searchService.executeQueryPhase(shardRequest, task, queryFuture);
            queryResult = (QuerySearchResult) queryFuture.get();

            RankFeatureShardRequest emptyDocIdsRequest = new RankFeatureShardRequest(
                OriginalIndices.NONE,
                queryResult.getContextId(),
                shardRequest,
                List.of()
            );
            PlainActionFuture<RankFeatureResult> rankFuture = new PlainActionFuture<>();
            searchService.executeRankFeaturePhase(emptyDocIdsRequest, task, rankFuture);
            rankResult = rankFuture.get();

            assertThat(
                "rank feature phase with empty docIds must still report directory metrics",
                rankResult.getDirectoryMetrics().isEmpty(),
                equalTo(false)
            );
        } finally {
            if (queryResult != null) {
                searchService.freeReaderContext(queryResult.getContextId());
            }
        }
    }
}
