/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 */
public class ShardSearchService extends AbstractIndexShardComponent {

    private final StatsHolder totalStats = new StatsHolder();

    private volatile Map<String, StatsHolder> groupsStats = ImmutableMap.of();

    @Inject public ShardSearchService(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
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
            if (groups.length == 1 && groups[0].equals("_all")) {
                groupsSt = new HashMap<String, SearchStats.Stats>(groupsStats.size());
                for (Map.Entry<String, StatsHolder> entry : groupsStats.entrySet()) {
                    groupsSt.put(entry.getKey(), entry.getValue().stats());
                }
            } else {
                groupsSt = new HashMap<String, SearchStats.Stats>(groups.length);
                for (String group : groups) {
                    StatsHolder statsHolder = groupsStats.get(group);
                    if (statsHolder != null) {
                        groupsSt.put(group, statsHolder.stats());
                    }
                }
            }
        }
        return new SearchStats(total, groupsSt);
    }

    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        totalStats.queryMetric.inc(tookInNanos);
        if (searchContext.groupStats() != null) {
            for (int i = 0; i < searchContext.groupStats().size(); i++) {
                groupStats(searchContext.groupStats().get(i)).queryMetric.inc(tookInNanos);
            }
        }
    }

    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        totalStats.fetchMetric.inc(tookInNanos);
        if (searchContext.groupStats() != null) {
            for (int i = 0; i < searchContext.groupStats().size(); i++) {
                groupStats(searchContext.groupStats().get(i)).fetchMetric.inc(tookInNanos);
            }
        }
    }

    public void clear() {
        totalStats.clear();
        synchronized (this) {
            groupsStats = ImmutableMap.of();
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

    static class StatsHolder {
        public final MeanMetric queryMetric = new MeanMetric();
        public final MeanMetric fetchMetric = new MeanMetric();

        public SearchStats.Stats stats() {
            return new SearchStats.Stats(
                    queryMetric.count(), TimeUnit.NANOSECONDS.toMillis(queryMetric.sum()),
                    fetchMetric.count(), TimeUnit.NANOSECONDS.toMillis(fetchMetric.sum()));
        }

        public void clear() {
            queryMetric.clear();
            fetchMetric.clear();
        }
    }
}
