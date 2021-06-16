/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader.FieldUsageNotifier;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader.UsageContext;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FieldUsageStats {

    private final Map<String, InternalFieldStats> perFieldStats = new ConcurrentHashMap<>();

    public FieldUsageStatsTrackingSession createSession() {
        return new FieldUsageStatsTrackingSession();
    }

    public Map<String, FieldStats> getPerFieldStats() {
        final Map<String, FieldStats> stats = new HashMap<>(perFieldStats.size());
        for (Map.Entry<String, InternalFieldStats> entry : perFieldStats.entrySet()) {
            Map<UsageContext, Long> usages = new HashMap<>();
            for (Map.Entry<UsageContext, AtomicLong> e : entry.getValue().usages.entrySet()) {
                usages.put(e.getKey(), e.getValue().get());
            }
            stats.put(entry.getKey(), new FieldStats(usages));
        }
        return Collections.unmodifiableMap(stats);
    }

    static class InternalFieldStats {
        private final Map<UsageContext, AtomicLong> usages = new ConcurrentHashMap<>();
    }

    public static class FieldStats {
        private final Map<UsageContext, Long> usages;

        public FieldStats(Map<UsageContext, Long> usages) {
            this.usages = usages;
        }

        public long get(UsageContext usageContext) {
            return usages.get(usageContext);
        }

        public Set<UsageContext> keySet() {
            return usages.keySet();
        }

        public boolean usesDocValues() {
            return usages.containsKey(UsageContext.DOC_VALUES);
        }

        public boolean usesSearch() {
            return usages.containsKey(UsageContext.TERMS);
        }

        public boolean usesStoredFields() {
            return usages.containsKey(UsageContext.STORED_FIELDS);
        }

        public boolean usesFreqs() {
            return usages.containsKey(UsageContext.FREQS);
        }

        public boolean usesPositions() {
            return usages.containsKey(UsageContext.POSITIONS);
        }

        public boolean usesOffsets() {
            return usages.containsKey(UsageContext.OFFSETS);
        }

        public boolean usesNorms() {
            return usages.containsKey(UsageContext.NORMS);
        }

        public boolean usesPoints() {
            return usages.containsKey(UsageContext.POINTS);
        }

        public boolean usesTermVectors() {
            return usages.containsKey(UsageContext.TERM_VECTORS);
        }

        @Override
        public String toString() {
            return "FieldStats{" +
                "usages=" + usages +
                '}';
        }
    }

    public class FieldUsageStatsTrackingSession implements FieldUsageNotifier, Releasable {

        private final Map<String, Set<UsageContext>> usages = new ConcurrentHashMap<>();

        @Override
        public void onFieldUsage(String field, UsageContext usageContext) {
            usages.computeIfAbsent(field, f -> Collections.synchronizedSet(EnumSet.noneOf(UsageContext.class)))
                .add(usageContext);
        }

        @Override
        public void close() {
            usages.entrySet().stream().forEach(e -> {
                InternalFieldStats fieldStats = perFieldStats.computeIfAbsent(e.getKey(), f -> new InternalFieldStats());
                for (UsageContext usageContext : e.getValue()) {
                    fieldStats.usages.computeIfAbsent(usageContext, c -> new AtomicLong())
                        .incrementAndGet();
                }
            });
        }
    }
}
