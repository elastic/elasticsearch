/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.usage;

import org.elasticsearch.action.admin.cluster.stats.SearchUsageStats;
import org.elasticsearch.common.util.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Service responsible for holding search usage statistics, like the number of used search sections and queries.
 * The counting is performed while parsing on the coordinating node.
 * Each distinct query type is counted once as part of a single search request.
 */
public final class SearchUsageHolder {
    private final LongAdder totalSearchCount = new LongAdder();
    private final Map<String, LongAdder> queriesUsage = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> sectionsUsage = new ConcurrentHashMap<>();

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
    }

    /**
     * Returns a snapshot of the search usage statistics
     */
    public SearchUsageStats getSearchUsageStats() {
        Map<String, Long> queriesUsageMap = Maps.newMapWithExpectedSize(queriesUsage.size());
        queriesUsage.forEach((query, adder) -> queriesUsageMap.put(query, adder.longValue()));
        Map<String, Long> sectionsUsageMap = Maps.newMapWithExpectedSize(sectionsUsage.size());
        sectionsUsage.forEach((query, adder) -> sectionsUsageMap.put(query, adder.longValue()));
        return new SearchUsageStats(
            Collections.unmodifiableMap(queriesUsageMap),
            Collections.unmodifiableMap(sectionsUsageMap),
            totalSearchCount.longValue()
        );
    }
}
