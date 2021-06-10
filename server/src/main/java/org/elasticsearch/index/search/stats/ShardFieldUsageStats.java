/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ShardFieldUsageStats {

    private final Map<String, InternalFieldStats> perFieldStats = new ConcurrentHashMap<>();

    public void onFieldAggregation(String field) {
        perFieldStats.computeIfAbsent(field, f -> new InternalFieldStats()).aggregationCount.incrementAndGet();
    }

    public void clear() {
        perFieldStats.clear();
    }


    static class InternalFieldStats {
        final AtomicLong aggregationCount = new AtomicLong();
    }

    public static class FieldStats {
        public FieldStats(long aggregationCount) {
            this.aggregationCount = aggregationCount;
        }

        public long getAggregationCount() {
            return aggregationCount;
        }

        private final long aggregationCount;
    }

    public Map<String, FieldStats> getPerFieldStats() {
        final Map<String, FieldStats> stats = new HashMap<>(perFieldStats.size());
        for (Map.Entry<String, InternalFieldStats> entry : perFieldStats.entrySet()) {
            stats.put(entry.getKey(), new FieldStats(entry.getValue().aggregationCount.get()));
        }
        return stats;
    }
}
