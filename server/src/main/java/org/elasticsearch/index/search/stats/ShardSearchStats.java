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
import org.elasticsearch.common.util.concurrent.TaskLoadAttribution;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;

public final class ShardSearchStats implements SearchOperationListener {
    private final StatsHolder totalStats;
    private final CounterMetric openContexts = new CounterMetric();
    private volatile Map<String, StatsHolder> groupsStats = emptyMap();
    private final SearchStatsSettings searchStatsSettings;
    private final TaskLoadAttribution.Target searchLoadTarget = this::accumulateSearchLoad;

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
        chargeTook(searchContext, TaskLoadAttribution.chargedByPool(), tookInNanos);
        computeStats(searchContext, searchContext.hasOnlySuggest() ? statsHolder -> {
            statsHolder.suggestMetric.inc(tookInNanos);
            statsHolder.suggestCurrent.dec();
        } : statsHolder -> {
            statsHolder.queryMetric.inc(tookInNanos);
            statsHolder.queryCurrent.dec();
        });
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        // the fetch can complete on a thread that is not a task of its pool
        searchContext.fetchChargedByPool(TaskLoadAttribution.chargedByPool());
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
        chargeTook(searchContext, searchContext.fetchChargedByPool(), tookInNanos);
        computeStats(searchContext, statsHolder -> {
            statsHolder.fetchMetric.inc(tookInNanos);
            statsHolder.fetchCurrent.dec();
        });
    }

    @Override
    public void onDfsPhase(SearchContext searchContext, long tookInNanos) {
        chargeTook(searchContext, TaskLoadAttribution.chargedByPool(), tookInNanos);
    }

    /**
     * Charges the took time of a phase to the search load of its groups, and to that of the shard when its pool does not charge
     * the time of its tasks, like the pools of system indices.
     */
    private void chargeTook(SearchContext searchContext, boolean chargedByPool, long tookInNanos) {
        long nowInNanos = System.nanoTime();
        // the CPU time of the phase is not measured, so it counts as all of it
        if (chargedByPool == false) {
            totalStats.addSearchLoad(tookInNanos, tookInNanos, nowInNanos);
        }
        forEachGroup(searchContext, statsHolder -> statsHolder.addSearchLoad(tookInNanos, tookInNanos, nowInNanos));
    }

    private void computeStats(SearchContext searchContext, Consumer<StatsHolder> consumer) {
        consumer.accept(totalStats);
        forEachGroup(searchContext, consumer);
    }

    private void forEachGroup(SearchContext searchContext, Consumer<StatsHolder> consumer) {
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

    /**
     * Charges the time of tasks to the search load of this shard.
     */
    public TaskLoadAttribution.Target searchLoadTarget() {
        return searchLoadTarget;
    }

    /**
     * Charges the time of tasks equally to {@code shards}, or {@code null} when there are none.
     */
    @Nullable
    public static TaskLoadAttribution.Target target(List<ShardSearchStats> shards) {
        if (shards.isEmpty()) {
            return null;
        }
        return (nanos, cpuNanos, finishNanos) -> charge(shards, nanos, cpuNanos, finishNanos);
    }

    /**
     * Charges {@code nanos}, and the {@code cpuNanos} in it, equally to {@code shards}.
     */
    public static void charge(List<ShardSearchStats> shards, long nanos, long cpuNanos, long finishNanos) {
        assert nanos >= 0 : nanos;
        assert cpuNanos >= 0 && cpuNanos <= nanos : cpuNanos + " of " + nanos;
        if (shards.isEmpty()) {
            return;
        }
        long share = nanos / shards.size();
        long remainder = nanos % shards.size();
        long cpuShare = cpuNanos / shards.size();
        long cpuRemainder = cpuNanos % shards.size();
        for (ShardSearchStats shard : shards) {
            shard.accumulateSearchLoad(remainder-- > 0 ? share + 1 : share, cpuRemainder-- > 0 ? cpuShare + 1 : cpuShare, finishNanos);
        }
    }

    /**
     * Charges {@code nanos}, and the {@code cpuNanos} in it, to {@code shards} in proportion to the first
     * {@code shards.size()} {@code weights}, or equally when they are all 0.
     */
    public static void charge(List<ShardSearchStats> shards, long[] weights, long nanos, long cpuNanos, long finishNanos) {
        assert nanos >= 0 : nanos;
        assert cpuNanos >= 0 && cpuNanos <= nanos : cpuNanos + " of " + nanos;
        assert weights.length >= shards.size() : weights.length + " < " + shards.size();
        double totalWeight = 0.0;
        for (int i = 0; i < shards.size(); i++) {
            assert weights[i] >= 0 : weights[i];
            totalWeight += weights[i];
        }
        if (totalWeight == 0.0) {
            charge(shards, nanos, cpuNanos, finishNanos);
            return;
        }
        // rounds the cumulative shares, so the shares add up to nanos and cpuNanos, but no shard gets more CPU time than time
        double weightSoFar = 0.0;
        long charged = 0;
        long cpuCharged = 0;
        for (int i = 0; i < shards.size(); i++) {
            weightSoFar += weights[i];
            double fraction = weightSoFar / totalWeight;
            long upTo = Math.min(nanos, Math.round(nanos * fraction));
            if (upTo > charged) {
                long cpu = Math.min(Math.min(cpuNanos, Math.round(cpuNanos * fraction)) - cpuCharged, upTo - charged);
                shards.get(i).accumulateSearchLoad(upTo - charged, cpu, finishNanos);
                charged = upTo;
                cpuCharged += cpu;
            }
        }
    }

    /**
     * The total time charged to the search load of this shard, in nanoseconds.
     */
    public long totalSearchLoadNanos() {
        return totalStats.searchLoadNanos.sum();
    }

    /**
     * Accumulates the recent search load for this shard, and the part of it spent on CPU.
     *
     * @param tookInNanos the duration of the search work being accounted for, in nanoseconds
     * @param cpuInNanos  the part of {@code tookInNanos} spent on CPU
     * @param nowInNanos  the current monotonic time, in nanoseconds, obtained from
     *                    {@link System#nanoTime()}, used as the timestamp for the load sample
     */
    public void accumulateSearchLoad(long tookInNanos, long cpuInNanos, long nowInNanos) {
        totalStats.addSearchLoad(tookInNanos, cpuInNanos, nowInNanos);
    }

    /**
     * Accumulates the recent search waiting for this shard: the time a query waited with work for it queued on the node.
     *
     * @param waitedInNanos the time waited, in nanoseconds
     * @param nowInNanos    the time the waiting was measured up to, from {@link System#nanoTime()}
     */
    public void accumulateSearchWaiting(long waitedInNanos, long nowInNanos) {
        totalStats.recentSearchWaiting.addIncrement(waitedInNanos, nowInNanos);
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
        final ExponentiallyWeightedMovingRate recentSearchCpu;
        final ExponentiallyWeightedMovingRate recentSearchWaiting;
        final LongAdder searchLoadNanos = new LongAdder();

        StatsHolder(SearchStatsSettings searchStatsSettings) {
            double lambdaInInverseNanos = Math.log(2.0) / searchStatsSettings.getRecentReadLoadHalfLifeForNewShards().nanos();
            long now = System.nanoTime();
            this.recentSearchLoad = new ExponentiallyWeightedMovingRate(lambdaInInverseNanos, now);
            this.recentSearchCpu = new ExponentiallyWeightedMovingRate(lambdaInInverseNanos, now);
            this.recentSearchWaiting = new ExponentiallyWeightedMovingRate(lambdaInInverseNanos, now);
        }

        void addSearchLoad(long nanos, long cpuNanos, long nowInNanos) {
            recentSearchLoad.addIncrement(nanos, nowInNanos);
            searchLoadNanos.add(nanos);
            if (cpuNanos > 0) {
                recentSearchCpu.addIncrement(cpuNanos, nowInNanos);
            }
        }

        SearchStats.Stats stats() {
            // one time for all the rates, so they are comparable
            long now = System.nanoTime();
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
                recentSearchLoad.getRate(now),
                recentSearchWaiting.getRate(now),
                recentSearchCpu.getRate(now)
            );
        }
    }
}
