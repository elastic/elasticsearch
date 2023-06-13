/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(maxNumDataNodes = 3)
@ESIntegTestCase.SuiteScopeTestCase
public class RRFRankCoordinatorCanMatchIT extends ESIntegTestCase {

    private static class EngineWithExposingTimestamp extends InternalEngine {

        EngineWithExposingTimestamp(EngineConfig engineConfig) {
            super(engineConfig);
        }

        @Override
        public ShardLongFieldRange getRawFieldRange(String field) {
            try (Searcher searcher = acquireSearcher("rrf_rank_can_match_it")) {
                final DirectoryReader directoryReader = searcher.getDirectoryReader();

                final byte[] minPackedValue = PointValues.getMinPackedValue(directoryReader, field);
                final byte[] maxPackedValue = PointValues.getMaxPackedValue(directoryReader, field);
                if (minPackedValue == null || maxPackedValue == null) {
                    assert minPackedValue == null && maxPackedValue == null
                        : Arrays.toString(minPackedValue) + "-" + Arrays.toString(maxPackedValue);
                    return ShardLongFieldRange.EMPTY;
                }

                return ShardLongFieldRange.of(LongPoint.decodeDimension(minPackedValue, 0), LongPoint.decodeDimension(maxPackedValue, 0));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class ExposingTimestampEnginePlugin extends Plugin implements EnginePlugin {

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(EngineWithExposingTimestamp::new);
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RRFRankPlugin.class, ExposingTimestampEnginePlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return 5;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 5;
    }

    @Override
    protected int minimumNumberOfReplicas() {
        return 0;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    public void testCanMatchCoordinator() throws Exception {
        // setup the cluster
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .endObject()
            .endObject();

        assertAcked(prepareCreate("time_index").setMapping(builder));
        ensureGreen("time_index");

        for (int i = 0; i < 500; i++) {
            client().prepareIndex("time_index").setSource("@timestamp", i).setRouting("a").get();
        }
        for (int i = 500; i < 1000; i++) {
            client().prepareIndex("time_index").setSource("@timestamp", i).setRouting("b").get();
        }

        client().admin().indices().prepareRefresh("time_index").get();
        client().admin().indices().prepareClose("time_index").get();
        client().admin().indices().prepareOpen("time_index").get();

        assertBusy(() -> {
            IndexLongFieldRange timestampRange = clusterService().state().metadata().index("time_index").getTimestampRange();
            assertTrue(Strings.toString(timestampRange), timestampRange.containsAllShardRanges());
        });

        // match 2 separate shard with no overlap in queries
        SearchResponse response = client().prepareSearch("time_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gt(495).lte(499)),
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gte(500).lt(505))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(5, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(3, response.getSkippedShards());

        // match 2 shards with overlap in queries
        response = client().prepareSearch("time_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gt(495).lte(505)),
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gte(497).lt(507))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(5, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(3, response.getSkippedShards());

        // match one shard with one query in range and one query out of range
        response = client().prepareSearch("time_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gt(501).lte(505)),
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gte(10000).lt(10005))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(4, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(4, response.getSkippedShards());

        // match no shards, but still use one to generate a search response
        response = client().prepareSearch("time_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gt(4000).lte(5000)),
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gte(10000).lt(10005))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(0, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(4, response.getSkippedShards());

        // match one shard with with no overlap in queries
        response = client().prepareSearch("time_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gt(600).lte(605)),
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gte(700).lt(705))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(5, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(4, response.getSkippedShards());

        // match one shard with exact overlap in queries
        response = client().prepareSearch("time_index")
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .setRankBuilder(new RRFRankBuilder(20, 1))
            .setTrackTotalHits(false)
            .setQueries(
                List.of(
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gt(600).lte(605)),
                    new SubSearchSourceBuilder(QueryBuilders.rangeQuery("@timestamp").gte(600).lt(605))
                )
            )
            .setSize(5)
            .get();

        assertNull(response.getHits().getTotalHits());
        assertEquals(5, response.getHits().getHits().length);
        assertEquals(5, response.getSuccessfulShards());
        assertEquals(4, response.getSkippedShards());
    }
}
