/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.usage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Service to count the number of used queries by query type.
 * Queries are counted by a coordinating node.
 * Query types are used in the original form as submitted by a user.
 * Queries types are deduplicated per a search request, thus if a search request contains 2 term queries,
 * this is counted a single term query.
 */
public class QueriesUsageService {
    private final Map<String, LongAdder> queriesUsage;

    public QueriesUsageService() {
        this.queriesUsage = new ConcurrentHashMap<>();
    }

    public void incrementQueryUsage(String query) {
        queriesUsage.computeIfAbsent(query, q -> new LongAdder()).increment();
    }

    public Map<String, Long> getUsageStats() {
        Map<String, Long> queriesUsageMap = new HashMap<>();
        queriesUsage.forEach((query, adder) -> queriesUsageMap.put(query, adder.longValue()));
        return queriesUsageMap;
    }

}
