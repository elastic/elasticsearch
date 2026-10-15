/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.TestSearchContext;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ShardSearchStatsTests extends IndexShardTestCase {

    private static final long TEN_MILLIS = 10;

    private ShardSearchStats shardSearchStatsListener;

    @Before
    public void setup() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        SearchStatsSettings searchStatsSettings = new SearchStatsSettings(clusterSettings);
        this.shardSearchStatsListener = new ShardSearchStats(searchStatsSettings);
    }

    public void testQueueTime() {
        assertEquals(0.0, shardSearchStatsListener.queueTimeTaskCount(), 0.0);
        assertEquals(0L, shardSearchStatsListener.totalQueueTimeInNanos());

        shardSearchStatsListener.onQueueTime(TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));
        shardSearchStatsListener.onQueueTime(TimeUnit.MILLISECONDS.toNanos(2 * TEN_MILLIS));

        assertEquals(2.0, shardSearchStatsListener.queueTimeTaskCount(), 1e-9);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(3 * TEN_MILLIS), shardSearchStatsListener.totalQueueTimeInNanos());
    }

    /** A task shared by several shards adds its wait and one task in total across them, so means stay exact. */
    public void testQueueTimeShare() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        ShardSearchStats shardA = shardSearchStatsListener;
        ShardSearchStats shardB = new ShardSearchStats(new SearchStatsSettings(clusterSettings));

        // a 40 ms task shared by 2 shards, and a 20 ms task on shard A only
        long sharedWait = TimeUnit.MILLISECONDS.toNanos(40);
        shardA.onQueueTimeShare(sharedWait, 2);
        shardB.onQueueTimeShare(sharedWait, 2);
        shardA.onQueueTime(TimeUnit.MILLISECONDS.toNanos(20));

        assertEquals(1.5, shardA.queueTimeTaskCount(), 1e-9);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(40), shardA.totalQueueTimeInNanos());
        assertEquals(0.5, shardB.queueTimeTaskCount(), 1e-9);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(20), shardB.totalQueueTimeInNanos());

        assertEquals("tasks add up across shards", 2.0, shardA.queueTimeTaskCount() + shardB.queueTimeTaskCount(), 1e-9);
        assertEquals(
            "waits add up across shards",
            TimeUnit.MILLISECONDS.toNanos(60),
            shardA.totalQueueTimeInNanos() + shardB.totalQueueTimeInNanos()
        );
    }

    public void testDfsPhase() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreDfsPhase(sc);
            shardSearchStatsListener.onDfsPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testDfsPhase_withGroups() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreDfsPhase(sc);
            shardSearchStatsListener.onDfsPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats searchStats = shardSearchStatsListener.stats("_all");
            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertTrue(stats.getSearchLoadRate() > 0.0);

            stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testDfsPhase_Failure() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreDfsPhase(sc);
            shardSearchStatsListener.onFailedDfsPhase(sc);

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0.0, stats.getSearchLoadRate(), 0);
        }
    }

    public void testQueryPhase() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getQueryCurrent());
            assertEquals(1, stats.getQueryCount());
            assertEquals(TEN_MILLIS, stats.getQueryTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testQueryPhase_SuggestOnly() {
        try (SearchContext sc = createSearchContext(true)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getSuggestCurrent());
            assertEquals(1, stats.getSuggestCount());
            assertEquals(TEN_MILLIS, stats.getSuggestTimeInMillis());
            assertEquals(0, stats.getQueryCurrent());
            assertEquals(0, stats.getQueryCount());
            assertEquals(0, stats.getQueryTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testQueryPhase_withGroup() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats searchStats = shardSearchStatsListener.stats("_all");
            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getQueryCurrent());
            assertEquals(1, stats.getQueryCount());
            assertEquals(TEN_MILLIS, stats.getQueryTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);

            stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
            assertEquals(0, stats.getQueryCurrent());
            assertEquals(1, stats.getQueryCount());
            assertEquals(TEN_MILLIS, stats.getQueryTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testQueryPhase_withGroup_SuggestOnly() {
        try (SearchContext sc = createSearchContext(true)) {

            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats searchStats = shardSearchStatsListener.stats("_all");
            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getSuggestCurrent());
            assertEquals(1, stats.getSuggestCount());
            assertEquals(TEN_MILLIS, stats.getSuggestTimeInMillis());
            assertEquals(0, stats.getQueryCurrent());
            assertEquals(0, stats.getQueryCount());
            assertEquals(0, stats.getQueryTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);

            stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
            assertEquals(0, stats.getSuggestCurrent());
            assertEquals(1, stats.getSuggestCount());
            assertEquals(TEN_MILLIS, stats.getSuggestTimeInMillis());
            assertEquals(0, stats.getQueryCurrent());
            assertEquals(0, stats.getQueryCount());
            assertEquals(0, stats.getQueryTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testQueryPhase_SuggestOnly_Failure() {
        try (SearchContext sc = createSearchContext(true)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onFailedQueryPhase(sc);

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getSuggestCurrent());
            assertEquals(0, stats.getSuggestCount());
            assertEquals(0, stats.getQueryCurrent());
            assertEquals(0, stats.getQueryCount());
            assertEquals(0, stats.getQueryFailure());
            assertEquals(0.0, stats.getSearchLoadRate(), 0);
        }
    }

    public void testQueryPhase_Failure() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onFailedQueryPhase(sc);

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getQueryCurrent());
            assertEquals(0, stats.getQueryCount());
            assertEquals(1, stats.getQueryFailure());
            assertEquals(0.0, stats.getSearchLoadRate(), 0);
        }
    }

    public void testFetchPhase() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreFetchPhase(sc);
            shardSearchStatsListener.onFetchPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getFetchCurrent());
            assertEquals(1, stats.getFetchCount());
            assertEquals(TEN_MILLIS, stats.getFetchTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testFetchPhase_withGroup() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreFetchPhase(sc);
            shardSearchStatsListener.onFetchPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats searchStats = shardSearchStatsListener.stats("_all");
            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getFetchCurrent());
            assertEquals(1, stats.getFetchCount());
            assertEquals(TEN_MILLIS, stats.getFetchTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);

            stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
            assertEquals(0, stats.getFetchCurrent());
            assertEquals(1, stats.getFetchCount());
            assertEquals(TEN_MILLIS, stats.getFetchTimeInMillis());
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testFetchPhase_Failure() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreFetchPhase(sc);
            shardSearchStatsListener.onFailedFetchPhase(sc);

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0, stats.getFetchCurrent());
            assertEquals(0, stats.getFetchCount());
            assertEquals(1, stats.getFetchFailure());
            assertEquals(0.0, stats.getSearchLoadRate(), 0);
        }
    }

    public void testReaderContext() throws IOException {
        IndexShard indexShard = newShard(true);
        try (ReaderContext rc = createReaderContext(indexShard)) {
            shardSearchStatsListener.onNewReaderContext(rc);
            SearchStats stats = shardSearchStatsListener.stats();
            assertEquals(1, stats.getOpenContexts());

            shardSearchStatsListener.onFreeReaderContext(rc);
            stats = shardSearchStatsListener.stats();
            assertEquals(0, stats.getOpenContexts());
        } finally {
            closeShards(indexShard);
        }
    }

    public void testScrollContext() throws IOException {
        IndexShard indexShard = newShard(true);
        try (ReaderContext rc = createReaderContext(indexShard)) {
            shardSearchStatsListener.onNewScrollContext(rc);
            SearchStats stats = shardSearchStatsListener.stats();
            assertEquals(1, stats.getTotal().getScrollCurrent());

            shardSearchStatsListener.onFreeScrollContext(rc);
            stats = shardSearchStatsListener.stats();
            assertEquals(0, stats.getTotal().getScrollCurrent());
            assertEquals(1, stats.getTotal().getScrollCount());
        } finally {
            closeShards(indexShard);
        }
    }

    private static ReaderContext createReaderContext(IndexShard indexShard) {
        return new ReaderContext(new ShardSearchContextId("test", 1L), null, indexShard, null, 0L, false, 0L);
    }

    private static SearchContext createSearchContext(boolean suggested) {
        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY
        );

        SearchExecutionContext searchExecutionContext = new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            null,
            null,
            MappingLookup.EMPTY,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.emptyMap(),
            null,
            MapperMetrics.NOOP,
            SearchExecutionContextHelper.SHARD_SEARCH_STATS
        );
        return new TestSearchContext(searchExecutionContext) {
            private final SearchRequest searchquest = new SearchRequest().allowPartialSearchResults(true);
            private final ShardSearchRequest request = new ShardSearchRequest(
                OriginalIndices.NONE,
                suggested ? searchquest.source(new SearchSourceBuilder().suggest(new SuggestBuilder())) : searchquest,
                new ShardId("index", "indexUUID", 0),
                0,
                1,
                AliasFilter.EMPTY,
                1f,
                0L,
                null
            );

            @Override
            public ShardSearchRequest request() {
                return request;
            }

            @Override
            public List<String> groupStats() {
                return Arrays.asList("group1");
            }
        };
    }
}
