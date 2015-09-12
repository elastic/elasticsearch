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

package org.elasticsearch.index.search.stats;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 */
public final class ShardSearchStats {

    private final SearchSlowLog slowLogSearchService;
    private final StatsHolder totalStats = new StatsHolder();
    private final CounterMetric openContexts = new CounterMetric();
    private volatile Map<String, StatsHolder> groupsStats = ImmutableMap.of();

    public ShardSearchStats(Settings indexSettings) {
        this.slowLogSearchService = new SearchSlowLog(indexSettings);
    }

    /**
     * Returns the stats, including group specific stats. If the groups are null/0 length, then nothing
     * is returned for them. If they are set, then only groups provided will be returned, or
     * <tt>_all</tt> for all groups.
     */
    public SearchStats stats(String... groups) {
        SearchStats.Stats total = totalStats.stats();
        Map<String, SearchStats.Stats> groupsSt = null;
        if (groups != null && groups.length > 0) {
            groupsSt = new HashMap<>(groupsStats.size());
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

    public void onPreQueryPhase(SearchContext searchContext) {
        totalStats.queryCurrent.inc();
        if (searchContext.groupStats() != null) {
            for (int i = 0; i < searchContext.groupStats().size(); i++) {
                groupStats(searchContext.groupStats().get(i)).queryCurrent.inc();
            }
        }
    }

    public void onFailedQueryPhase(SearchContext searchContext) {
        totalStats.queryCurrent.dec();
        if (searchContext.groupStats() != null) {
            for (int i = 0; i < searchContext.groupStats().size(); i++) {
                groupStats(searchContext.groupStats().get(i)).queryCurrent.dec();
            }
        }
    }

    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        totalStats.queryMetric.inc(tookInNanos);
        totalStats.queryCurrent.dec();
        if (searchContext.groupStats() != null) {
            for (int i = 0; i < searchContext.groupStats().size(); i++) {
                StatsHolder statsHolder = groupStats(searchContext.groupStats().get(i));
                statsHolder.queryMetric.inc(tookInNanos);
                statsHolder.queryCurrent.dec();
            }
        }
        slowLogSearchService.onQueryPhase(searchContext, tookInNanos);
    }

    public void onPreFetchPhase(SearchContext searchContext) {
        totalStats.fetchCurrent.inc();
        if (searchContext.groupStats() != null) {
            for (int i = 0; i < searchContext.groupStats().size(); i++) {
                groupStats(searchContext.groupStats().get(i)).fetchCurrent.inc();
            }
        }
    }

    public void onFailedFetchPhase(SearchContext searchContext) {
        totalStats.fetchCurrent.dec();
        if (searchContext.groupStats() != null) {
            for (int i = 0; i < searchContext.groupStats().size(); i++) {
                groupStats(searchContext.groupStats().get(i)).fetchCurrent.dec();
            }
        }
    }

    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        totalStats.fetchMetric.inc(tookInNanos);
        totalStats.fetchCurrent.dec();
        if (searchContext.groupStats() != null) {
            for (int i = 0; i < searchContext.groupStats().size(); i++) {
                StatsHolder statsHolder = groupStats(searchContext.groupStats().get(i));
                statsHolder.fetchMetric.inc(tookInNanos);
                statsHolder.fetchCurrent.dec();
            }
        }
        slowLogSearchService.onFetchPhase(searchContext, tookInNanos);
    }

    public void clear() {
        totalStats.clear();
        synchronized (this) {
            if (!groupsStats.isEmpty()) {
                MapBuilder<String, StatsHolder> typesStatsBuilder = MapBuilder.newMapBuilder();
                for (Map.Entry<String, StatsHolder> typeStats : groupsStats.entrySet()) {
                    if (typeStats.getValue().totalCurrent() > 0) {
                        typeStats.getValue().clear();
                        typesStatsBuilder.put(typeStats.getKey(), typeStats.getValue());
                    }
                }
                groupsStats = typesStatsBuilder.immutableMap();
            }
        }
    }

    private StatsHolder groupStats(String group) {
        StatsHolder stats = groupsStats.get(group);
        if (stats == null) {
            synchronized (this) {
                stats = groupsStats.get(group);
                if (stats == null) {
                    stats = new StatsHolder();
                    groupsStats = MapBuilder.newMapBuilder(groupsStats).put(group, stats).immutableMap();
                }
            }
        }
        return stats;
    }

    public void onNewContext(SearchContext context) {
        openContexts.inc();
    }

    public void onFreeContext(SearchContext context) {
        openContexts.dec();
    }

    public void onNewScrollContext(SearchContext context) {
        totalStats.scrollCurrent.inc();
    }

    public void onFreeScrollContext(SearchContext context) {
        totalStats.scrollCurrent.dec();
        totalStats.scrollMetric.inc(System.nanoTime() - context.getOriginNanoTime());
    }

    public void onRefreshSettings(Settings settings) {
        slowLogSearchService.onRefreshSettings(settings);
    }

    final static class StatsHolder {
        public final MeanMetric queryMetric = new MeanMetric();
        public final MeanMetric fetchMetric = new MeanMetric();
        public final MeanMetric scrollMetric = new MeanMetric();
        public final CounterMetric queryCurrent = new CounterMetric();
        public final CounterMetric fetchCurrent = new CounterMetric();
        public final CounterMetric scrollCurrent = new CounterMetric();

        public SearchStats.Stats stats() {
            return new SearchStats.Stats(
                    queryMetric.count(), TimeUnit.NANOSECONDS.toMillis(queryMetric.sum()), queryCurrent.count(),
                    fetchMetric.count(), TimeUnit.NANOSECONDS.toMillis(fetchMetric.sum()), fetchCurrent.count(),
                    scrollMetric.count(), TimeUnit.NANOSECONDS.toMillis(scrollMetric.sum()), scrollCurrent.count()
            );
        }

        public long totalCurrent() {
            return queryCurrent.count() + fetchCurrent.count() + scrollCurrent.count();
        }

        public void clear() {
            queryMetric.clear();
            fetchMetric.clear();
            scrollMetric.clear();
        }
    }
}
