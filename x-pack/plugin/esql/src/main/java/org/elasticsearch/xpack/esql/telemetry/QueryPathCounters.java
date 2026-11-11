/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Per-node counts of finished queries by {@link QueryPath}, reported under the {@code query_paths} key of the ES|QL usage.
 * The node stats action adds them to the {@link Counters} from {@link Metrics#stats()}.
 */
public final class QueryPathCounters {

    private static final String PREFIX = QueryPath.USAGE_KEY + ".";
    private static final int TOTAL = 0;
    private static final int FAILED = 1;
    private static final int COMMANDS = 2;

    // Created on a path's first query, so only paths some query took are kept and reported. Each array holds the path's total,
    // its failed count, then one count per FeatureMetric ordinal. Every level is a fixed list, so the number of paths is bounded:
    // (Kibana features + direct clients + the other callers) x leading commands.
    private final Map<QueryPath, AtomicLongArray> counts = new ConcurrentHashMap<>();

    /**
     * Counts a finished query under its {@link QueryPath}, with each {@link FeatureMetric} it used, as {@code features.*} counts
     * them. Called once per query.
     */
    public void total(QueryPath path, BitSet commands) {
        AtomicLongArray pathCounts = counts(path);
        pathCounts.incrementAndGet(TOTAL);
        for (int i = commands.nextSetBit(0); i >= 0; i = commands.nextSetBit(i + 1)) {
            pathCounts.incrementAndGet(COMMANDS + i);
        }
    }

    public void failed(QueryPath path) {
        counts(path).incrementAndGet(FAILED);
    }

    /**
     * Adds {@code query_paths.<caller>.<source>.<leading command>.total|failed|commands.<command>} for every count above zero.
     */
    public void populate(Counters counters) {
        FeatureMetric[] commands = FeatureMetric.values();
        for (Map.Entry<QueryPath, AtomicLongArray> entry : counts.entrySet()) {
            QueryPath path = entry.getKey();
            AtomicLongArray pathCounts = entry.getValue();
            String prefix = PREFIX + path.caller() + "." + path.source() + "." + path.leadingCommand() + ".";
            nonZero(counters, prefix + QueryPath.TOTAL, pathCounts.get(TOTAL));
            nonZero(counters, prefix + QueryPath.FAILED, pathCounts.get(FAILED));
            for (FeatureMetric command : commands) {
                nonZero(counters, prefix + QueryPath.COMMANDS + "." + command, pathCounts.get(COMMANDS + command.ordinal()));
            }
        }
    }

    private AtomicLongArray counts(QueryPath path) {
        return counts.computeIfAbsent(path, p -> new AtomicLongArray(COMMANDS + FeatureMetric.values().length));
    }

    private static void nonZero(Counters counters, String key, long count) {
        if (count > 0) {
            counters.inc(key, count);
        }
    }
}
