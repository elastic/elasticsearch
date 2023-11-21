/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.search.stats.FieldUsageStats.PerFieldUsageStats;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader.FieldUsageNotifier;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Records and provides field usage stats
 */
public class ShardFieldUsageTracker {

    private final Map<String, InternalFieldStats> perFieldStats = new ConcurrentHashMap<>();

    /**
     * Returns a new session which can be passed to a {@link FieldUsageTrackingDirectoryReader}
     * to track field usage of a shard. Fields tracked as part of a session are only counted
     * as a single use. The stats are then recorded for this shard when the corresponding
     * session is closed.
     */
    public FieldUsageStatsTrackingSession createSession() {
        return new FieldUsageStatsTrackingSession();
    }

    /**
     * Returns field usage stats for the given fields. If no subset of fields is specified,
     * returns information for all fields.
     */
    public FieldUsageStats stats(String... fields) {
        final Map<String, PerFieldUsageStats> stats = Maps.newMapWithExpectedSize(perFieldStats.size());
        for (Map.Entry<String, InternalFieldStats> entry : perFieldStats.entrySet()) {
            InternalFieldStats ifs = entry.getValue();
            if (CollectionUtils.isEmpty(fields) || Regex.simpleMatch(fields, entry.getKey())) {
                PerFieldUsageStats pf = new PerFieldUsageStats(
                    ifs.any.longValue(),
                    ifs.proximity.longValue(),
                    ifs.terms.longValue(),
                    ifs.postings.longValue(),
                    ifs.termFrequencies.longValue(),
                    ifs.positions.longValue(),
                    ifs.offsets.longValue(),
                    ifs.docValues.longValue(),
                    ifs.storedFields.longValue(),
                    ifs.norms.longValue(),
                    ifs.payloads.longValue(),
                    ifs.termVectors.longValue(),
                    ifs.points.longValue(),
                    ifs.knnVectors.longValue()
                );
                stats.put(entry.getKey(), pf);
            }
        }
        return new FieldUsageStats(Collections.unmodifiableMap(stats));
    }

    static class InternalFieldStats {
        final LongAdder any = new LongAdder();
        final LongAdder proximity = new LongAdder();
        final LongAdder terms = new LongAdder();
        final LongAdder postings = new LongAdder();
        final LongAdder termFrequencies = new LongAdder();
        final LongAdder positions = new LongAdder();
        final LongAdder offsets = new LongAdder();
        final LongAdder docValues = new LongAdder();
        final LongAdder storedFields = new LongAdder();
        final LongAdder norms = new LongAdder();
        final LongAdder payloads = new LongAdder();
        final LongAdder termVectors = new LongAdder();
        final LongAdder points = new LongAdder();
        final LongAdder knnVectors = new LongAdder();
    }

    static class PerField {
        // while these fields are currently only sequentially accessed, we expect concurrent access by future usages (and custom plugins)
        volatile boolean terms;
        volatile boolean postings;
        volatile boolean termFrequencies;
        volatile boolean positions;
        volatile boolean offsets;
        volatile boolean docValues;
        volatile boolean storedFields;
        volatile boolean norms;
        volatile boolean payloads;
        volatile boolean termVectors;
        volatile boolean points;
        volatile boolean knnVectors;
    }

    public class FieldUsageStatsTrackingSession implements FieldUsageNotifier, Releasable {

        // while this map is currently only sequentially accessed, we expect future usages (and custom plugins) to access this concurrently
        private final Map<String, PerField> usages = new ConcurrentHashMap<>();

        @Override
        public void close() {
            usages.entrySet().forEach(e -> {
                InternalFieldStats fieldStats = perFieldStats.computeIfAbsent(e.getKey(), f -> new InternalFieldStats());
                PerField pf = e.getValue();
                boolean any = false;
                boolean proximity = false;
                if (pf.terms) {
                    any = true;
                    fieldStats.terms.increment();
                }
                if (pf.postings) {
                    any = true;
                    fieldStats.postings.increment();
                }
                if (pf.termFrequencies) {
                    any = true;
                    fieldStats.termFrequencies.increment();
                }
                if (pf.positions) {
                    any = true;
                    proximity = true;
                    fieldStats.positions.increment();
                }
                if (pf.offsets) {
                    any = true;
                    proximity = true;
                    fieldStats.offsets.increment();
                }
                if (pf.docValues) {
                    any = true;
                    fieldStats.docValues.increment();
                }
                if (pf.storedFields) {
                    any = true;
                    fieldStats.storedFields.increment();
                }
                if (pf.norms) {
                    any = true;
                    fieldStats.norms.increment();
                }
                if (pf.payloads) {
                    any = true;
                    proximity = true;
                    fieldStats.payloads.increment();
                }
                if (pf.points) {
                    any = true;
                    fieldStats.points.increment();
                }
                if (pf.termVectors) {
                    any = true;
                    fieldStats.termVectors.increment();
                }
                if (pf.knnVectors) {
                    any = true;
                    fieldStats.knnVectors.increment();
                }
                if (any) {
                    fieldStats.any.increment();
                }
                if (proximity) {
                    fieldStats.proximity.increment();
                }
            });
        }

        private PerField getOrAdd(String fieldName) {
            Objects.requireNonNull(fieldName, "fieldName must be non-null");
            return usages.computeIfAbsent(fieldName, k -> new PerField());
        }

        @Override
        public void onTermsUsed(String field) {
            getOrAdd(field).terms = true;
        }

        @Override
        public void onPostingsUsed(String field) {
            getOrAdd(field).postings = true;
        }

        @Override
        public void onTermFrequenciesUsed(String field) {
            getOrAdd(field).termFrequencies = true;
        }

        @Override
        public void onPositionsUsed(String field) {
            getOrAdd(field).positions = true;
        }

        @Override
        public void onOffsetsUsed(String field) {
            getOrAdd(field).offsets = true;
        }

        @Override
        public void onDocValuesUsed(String field) {
            getOrAdd(field).docValues = true;
        }

        @Override
        public void onStoredFieldsUsed(String field) {
            getOrAdd(field).storedFields = true;
        }

        @Override
        public void onNormsUsed(String field) {
            getOrAdd(field).norms = true;
        }

        @Override
        public void onPayloadsUsed(String field) {
            getOrAdd(field).payloads = true;
        }

        @Override
        public void onPointsUsed(String field) {
            getOrAdd(field).points = true;
        }

        @Override
        public void onTermVectorsUsed(String field) {
            getOrAdd(field).termVectors = true;
        }

        @Override
        public void onKnnVectorsUsed(String field) {
            getOrAdd(field).knnVectors = true;
        }
    }
}
