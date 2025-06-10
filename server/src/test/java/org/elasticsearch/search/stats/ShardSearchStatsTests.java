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
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ShardSearchStatsTests extends ESTestCase {

    private static final long TEN_MILLIS = 10;

    private ShardSearchStats shardSearchStatsListener;

    @Before
    public void setup() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        SearchStatsSettings searchStatsSettings = new SearchStatsSettings(clusterSettings);
        this.shardSearchStatsListener = new ShardSearchStats(searchStatsSettings);
    }

    public void testQueryPhase() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testQueryPhase_SuggestOnly() {
        try (SearchContext sc = createSearchContext(true)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testQueryPhase_withGroup() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats searchStats = shardSearchStatsListener.stats("_all");
            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertTrue(stats.getSearchLoadRate() > 0.0);

            stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testQueryPhase_withGroup_SuggestOnly() {
        try (SearchContext sc = createSearchContext(true)) {

            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onQueryPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats searchStats = shardSearchStatsListener.stats("_all");
            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertTrue(stats.getSearchLoadRate() > 0.0);

            stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testQueryPhase_SuggestOnly_Failure() {
        try (SearchContext sc = createSearchContext(true)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onFailedQueryPhase(sc);

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0.0, stats.getSearchLoadRate(), 0);
        }
    }

    public void testQueryPhase_Failure() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreQueryPhase(sc);
            shardSearchStatsListener.onFailedQueryPhase(sc);

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0.0, stats.getSearchLoadRate(), 0);
        }
    }

    public void testFetchPhase() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreFetchPhase(sc);
            shardSearchStatsListener.onFetchPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testFetchPhase_withGroup() {
        try (SearchContext sc = createSearchContext(false)) {
        shardSearchStatsListener.onPreFetchPhase(sc);
        shardSearchStatsListener.onFetchPhase(sc, TimeUnit.MILLISECONDS.toNanos(TEN_MILLIS));

        SearchStats searchStats = shardSearchStatsListener.stats("_all");
        SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
        assertTrue(stats.getSearchLoadRate() > 0.0);

        stats = Objects.requireNonNull(searchStats.getGroupStats()).get("group1");
        assertTrue(stats.getSearchLoadRate() > 0.0);
        }
    }

    public void testFetchPhase_Failure() {
        try (SearchContext sc = createSearchContext(false)) {
            shardSearchStatsListener.onPreFetchPhase(sc);
            shardSearchStatsListener.onFailedFetchPhase(sc);

            SearchStats.Stats stats = shardSearchStatsListener.stats().getTotal();
            assertEquals(0.0, stats.getSearchLoadRate(), 0);
        }
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
            MapperMetrics.NOOP
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
