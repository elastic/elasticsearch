/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.ExponentiallyWeightedMovingRate;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;

public final class ShardSearchStats implements SearchOperationListener {

    private final StatsHolder totalStats;
    private final CounterMetric openContexts = new CounterMetric();
    private volatile Map<String, StatsHolder> groupsStats = emptyMap();
    private final SearchStatsSettings searchStatsSettings;

    public ShardSearchStats(SearchStatsSettings searchStatsSettings) {
        this.searchStatsSettings = searchStatsSettings;
        this.totalStats = new StatsHolder(searchStatsSettings);
    }

    /**
     * Returns the stats, including group specific stats. If the groups are null/0 length, then nothing
     * is returned for them. If they are set, then only groups provided will be returned, or
     * {@code _all} for all groups.
     */
    public SearchStats stats(String... groups) {
        SearchStats.Stats total = totalStats.stats();
        Map<String, SearchStats.Stats> groupsSt = null;
        if (CollectionUtils.isEmpty(groups) == false) {
            groupsSt = Maps.newMapWithExpectedSize(groupsStats.size());
            if (groups.length == 1 && groups[0].equals("_all")) {
                for (Map.Entry<String, StatsHolder> entry : groupsStats.entrySet()) {
                    groupsSt.put(entry.getKey(), entry.getValue().stats());
                }
            } else {
                for (Map.Entry<String, StatsHolder> entry : groupsStats.entrySet()) {
                    if (Regex.simpleMatch(groups, entry.getKey())) {
                        groupsSt.put(entry.getKey(), entry.getValue().stats());
                    }
                }
            }
        }
        return new SearchStats(total, openContexts.count(), groupsSt);
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        computeStats(
            searchContext,
            searchContext.hasOnlySuggest() ? statsHolder -> statsHolder.suggestCurrent.inc() : statsHolder -> statsHolder.queryCurrent.inc()
        );
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        computeStats(searchContext, statsHolder -> {
            if (searchContext.hasOnlySuggest()) {
                statsHolder.suggestCurrent.dec();
            } else {
                statsHolder.queryCurrent.dec();
                statsHolder.queryFailure.inc();
            }
        });
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        computeStats(searchContext, searchContext.hasOnlySuggest() ? statsHolder -> {
            statsHolder.recentSearchLoad.addIncrement(tookInNanos, System.nanoTime());
            statsHolder.suggestMetric.inc(tookInNanos);
            statsHolder.suggestCurrent.dec();
        } : statsHolder -> {
            statsHolder.recentSearchLoad.addIncrement(tookInNanos, System.nanoTime());
            statsHolder.queryMetric.inc(tookInNanos);
            statsHolder.queryCurrent.dec();
        });
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        computeStats(searchContext, statsHolder -> statsHolder.fetchCurrent.inc());
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        computeStats(searchContext, statsHolder -> {
            statsHolder.fetchCurrent.dec();
            statsHolder.fetchFailure.inc();
        });
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        computeStats(searchContext, statsHolder -> {
            statsHolder.recentSearchLoad.addIncrement(tookInNanos, System.nanoTime());
            statsHolder.fetchMetric.inc(tookInNanos);
            statsHolder.fetchCurrent.dec();
        });
    }

    private void computeStats(SearchContext searchContext, Consumer<StatsHolder> consumer) {
        consumer.accept(totalStats);
        var groupStats = searchContext.groupStats();
        if (groupStats != null) {
            for (String group : groupStats) {
                consumer.accept(groupStats(group));
            }
        }
    }

    private StatsHolder groupStats(String group) {
        StatsHolder stats = groupsStats.get(group);
        if (stats == null) {
            synchronized (this) {
                stats = groupsStats.get(group);
                if (stats == null) {
                    stats = new StatsHolder(searchStatsSettings);
                    groupsStats = Maps.copyMapWithAddedEntry(groupsStats, group, stats);
                }
            }
        }
        return stats;
    }

    @Override
    public void onNewReaderContext(ReaderContext readerContext) {
        openContexts.inc();
    }

    @Override
    public void onFreeReaderContext(ReaderContext readerContext) {
        openContexts.dec();
    }

    @Override
    public void onNewScrollContext(ReaderContext readerContext) {
        totalStats.scrollCurrent.inc();
    }

    @Override
    public void onFreeScrollContext(ReaderContext readerContext) {
        totalStats.scrollCurrent.dec();
        assert totalStats.scrollCurrent.count() >= 0;
        totalStats.scrollMetric.inc(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - readerContext.getStartTimeInNano()));
    }

    static final class StatsHolder {
        final MeanMetric queryMetric = new MeanMetric();
        final MeanMetric fetchMetric = new MeanMetric();
        /* We store scroll statistics in microseconds because with nanoseconds we run the risk of overflowing the total stats if there are
         * many scrolls. For example, on a system with 2^24 scrolls that have been executed, each executing for 2^10 seconds, then using
         * nanoseconds would require a numeric representation that can represent at least 2^24 * 2^10 * 10^9 > 2^24 * 2^10 * 2^29 = 2^63
         * which exceeds the largest value that can be represented by a long. By using microseconds, we enable capturing one-thousand
         * times as many scrolls (i.e., billions of scrolls which at one per second would take 32 years to occur), or scrolls that execute
         * for one-thousand times as long (i.e., scrolls that execute for almost twelve days on average).
         */
        final MeanMetric scrollMetric = new MeanMetric();
        final MeanMetric suggestMetric = new MeanMetric();
        final CounterMetric queryCurrent = new CounterMetric();
        final CounterMetric fetchCurrent = new CounterMetric();
        final CounterMetric scrollCurrent = new CounterMetric();
        final CounterMetric suggestCurrent = new CounterMetric();

        final CounterMetric queryFailure = new CounterMetric();
        final CounterMetric fetchFailure = new CounterMetric();

        final ExponentiallyWeightedMovingRate recentSearchLoad;

        StatsHolder(SearchStatsSettings searchStatsSettings) {
            double lambdaInInverseNanos = Math.log(2.0) / searchStatsSettings.getRecentReadLoadHalfLifeForNewShards().nanos();
            this.recentSearchLoad = new ExponentiallyWeightedMovingRate(lambdaInInverseNanos, System.nanoTime());
        }

        SearchStats.Stats stats() {
            return new SearchStats.Stats(
                queryMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(queryMetric.sum()),
                queryCurrent.count(),
                queryFailure.count(),
                fetchMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(fetchMetric.sum()),
                fetchCurrent.count(),
                fetchFailure.count(),
                scrollMetric.count(),
                TimeUnit.MICROSECONDS.toMillis(scrollMetric.sum()),
                scrollCurrent.count(),
                suggestMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(suggestMetric.sum()),
                suggestCurrent.count(),
                recentSearchLoad.getRate(System.nanoTime())
            );
        }
    }
}
