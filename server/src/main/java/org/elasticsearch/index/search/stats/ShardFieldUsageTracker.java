/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.search.stats.FieldUsageStats.PerFieldUsageStats;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader.FieldUsageNotifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ShardFieldUsageTracker {

    private final Map<String, InternalFieldStats> perFieldStats = new ConcurrentHashMap<>();

    public FieldUsageStatsTrackingSession createSession() {
        return new FieldUsageStatsTrackingSession();
    }

    public FieldUsageStats stats() {
        final Map<String, PerFieldUsageStats> stats = new HashMap<>(perFieldStats.size());
        for (Map.Entry<String, InternalFieldStats> entry : perFieldStats.entrySet()) {
            PerFieldUsageStats pf = new PerFieldUsageStats();
            InternalFieldStats ifs = entry.getValue();
            pf.terms = ifs.terms.get();
            pf.freqs = ifs.freqs.get();
            pf.positions = ifs.positions.get();
            pf.offsets = ifs.offsets.get();
            pf.docValues = ifs.docValues.get();
            pf.storedFields = ifs.storedFields.get();
            pf.norms = ifs.norms.get();
            pf.payloads = ifs.payloads.get();
            pf.impacts = ifs.impacts.get();
            pf.termVectors = ifs.termVectors.get();
            pf.points = ifs.points.get();
            stats.put(entry.getKey(), pf);
        }
        return new FieldUsageStats(Collections.unmodifiableMap(stats));
    }

    static class InternalFieldStats {
        final AtomicLong terms = new AtomicLong();
        final AtomicLong freqs = new AtomicLong();
        final AtomicLong positions = new AtomicLong();
        final AtomicLong offsets = new AtomicLong();
        final AtomicLong docValues = new AtomicLong();
        final AtomicLong storedFields = new AtomicLong();
        final AtomicLong norms = new AtomicLong();
        final AtomicLong payloads = new AtomicLong();
        final AtomicLong impacts = new AtomicLong();
        final AtomicLong termVectors = new AtomicLong();
        final AtomicLong points = new AtomicLong();
    }

    public class FieldUsageStatsTrackingSession implements FieldUsageNotifier, Releasable {

        class PerField {
            boolean terms;
            boolean freqs;
            boolean positions;
            boolean offsets;
            boolean docValues;
            boolean storedFields;
            boolean norms;
            boolean payloads;
            boolean impacts;
            boolean termVectors;
            boolean points;

        }

        private final Map<String, PerField> usages = new ConcurrentHashMap<>();

        @Override
        public void close() {
            usages.entrySet().stream().forEach(e -> {
                InternalFieldStats fieldStats = perFieldStats.computeIfAbsent(e.getKey(), f -> new InternalFieldStats());
                PerField pf = e.getValue();
                if (pf.terms) {
                    fieldStats.terms.incrementAndGet();
                }
                if (pf.freqs) {
                    fieldStats.freqs.incrementAndGet();
                }
                if (pf.positions) {
                    fieldStats.positions.incrementAndGet();
                }
                if (pf.offsets) {
                    fieldStats.offsets.incrementAndGet();
                }
                if (pf.docValues) {
                    fieldStats.docValues.incrementAndGet();
                }
                if (pf.storedFields) {
                    fieldStats.storedFields.incrementAndGet();
                }
                if (pf.norms) {
                    fieldStats.norms.incrementAndGet();
                }
                if (pf.payloads) {
                    fieldStats.payloads.incrementAndGet();
                }
                if (pf.impacts) {
                    fieldStats.impacts.incrementAndGet();
                }
                if (pf.points) {
                    fieldStats.points.incrementAndGet();
                }
                if (pf.termVectors) {
                    fieldStats.termVectors.incrementAndGet();
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
        public void onFreqsUsed(String field) {
            getOrAdd(field).freqs = true;
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
        public void onImpactsUsed(String field) {
            getOrAdd(field).impacts = true;
        }

        @Override
        public void onPointsUsed(String field) {
            getOrAdd(field).points = true;
        }

        @Override
        public void onTermVectorsUsed(String field) {
            getOrAdd(field).termVectors = true;
        }
    }
}
