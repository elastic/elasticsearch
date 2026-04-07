/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.usage;

import org.elasticsearch.action.admin.cluster.stats.ExtendedSearchUsageLongCounter;
import org.elasticsearch.action.admin.cluster.stats.ExtendedSearchUsageMetric;
import org.elasticsearch.action.admin.cluster.stats.ExtendedSearchUsageStats;
import org.elasticsearch.action.admin.cluster.stats.SearchUsageStats;
import org.elasticsearch.common.util.Maps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * Service responsible for holding search usage statistics, like the number of used search sections and queries.
 * The counting is performed while parsing on the coordinating node.
 * Each distinct query type is counted once as part of a single search request.
 */
public final class SearchUsageHolder {
    private final LongAdder totalSearchCount = new LongAdder();
    private final Map<String, LongAdder> queriesUsage = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> rescorersUsage = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> sectionsUsage = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> retrieversUsage = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Map<String, LongAdder>>> extendedSearchUsage = new ConcurrentHashMap<>();

    SearchUsageHolder() {}

    /**
     * Update stats by adding the provided search request usage
     */
    public void updateUsage(SearchUsage searchUsage) {
        totalSearchCount.increment();
        for (String section : searchUsage.getSectionsUsage()) {
            sectionsUsage.computeIfAbsent(section, q -> new LongAdder()).increment();
        }
        for (String query : searchUsage.getQueryUsage()) {
            queriesUsage.computeIfAbsent(query, q -> new LongAdder()).increment();
        }
        for (String rescorer : searchUsage.getRescorerUsage()) {
            rescorersUsage.computeIfAbsent(rescorer, q -> new LongAdder()).increment();
        }
        for (String retriever : searchUsage.getRetrieverUsage()) {
            retrieversUsage.computeIfAbsent(retriever, q -> new LongAdder()).increment();
        }
        updateExtendedUsage(searchUsage.getExtendedDataUsage());
    }

    /**
     * Returns a snapshot of the search usage statistics
     */
    public SearchUsageStats getSearchUsageStats() {
        Map<String, Long> queriesUsageMap = Maps.newMapWithExpectedSize(queriesUsage.size());
        queriesUsage.forEach((query, adder) -> queriesUsageMap.put(query, adder.longValue()));
        Map<String, Long> sectionsUsageMap = Maps.newMapWithExpectedSize(sectionsUsage.size());
        sectionsUsage.forEach((query, adder) -> sectionsUsageMap.put(query, adder.longValue()));
        Map<String, Long> rescorersUsageMap = Maps.newMapWithExpectedSize(rescorersUsage.size());
        rescorersUsage.forEach((query, adder) -> rescorersUsageMap.put(query, adder.longValue()));
        Map<String, Long> retrieversUsageMap = Maps.newMapWithExpectedSize(retrieversUsage.size());
        retrieversUsage.forEach((retriever, adder) -> retrieversUsageMap.put(retriever, adder.longValue()));
        ExtendedSearchUsageStats extendedSearchUsageStats = new ExtendedSearchUsageStats(getExtendedSearchUsage());

        return new SearchUsageStats(
            Collections.unmodifiableMap(queriesUsageMap),
            Collections.unmodifiableMap(rescorersUsageMap),
            Collections.unmodifiableMap(sectionsUsageMap),
            Collections.unmodifiableMap(retrieversUsageMap),
            extendedSearchUsageStats,
            totalSearchCount.longValue()
        );
    }

    private Map<String, Map<String, ExtendedSearchUsageMetric<?>>> getExtendedSearchUsage() {
        return unmodifiableMap(
            extendedSearchUsage,
            nameMap -> unmodifiableMap(
                nameMap,
                valueMap -> new ExtendedSearchUsageLongCounter(unmodifiableMap(valueMap, LongAdder::longValue))
            )
        );
    }

    private void updateExtendedUsage(Map<String, Map<String, Set<String>>> extendedDataUsage) {
        extendedDataUsage.forEach(
            (category, nameMap) -> nameMap.forEach(
                (name, valueSet) -> valueSet.forEach(
                    value -> extendedSearchUsage.computeIfAbsent(category, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(name, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(value, k -> new LongAdder())
                        .increment()
                )
            )
        );
    }

    private static <K, V, R> Map<K, R> unmodifiableMap(Map<K, V> in, Function<V, R> valueMapper) {
        Map<K, R> map = new HashMap<>(in.size());
        in.forEach((k, v) -> map.put(k, valueMapper.apply(v)));
        return Collections.unmodifiableMap(map);
    }

}
