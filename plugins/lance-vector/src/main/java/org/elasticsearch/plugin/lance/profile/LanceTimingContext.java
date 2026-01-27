/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.profile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Timing context for Lance kNN search operations.
 * <p>
 * This class provides thread-local storage for timing data during query execution.
 * It is only active when profiling is enabled to minimize overhead in production.
 * <p>
 * Timing data is automatically cleared after each query to prevent memory leaks
 * and ensure clean measurements for each search request.
 */
public final class LanceTimingContext {

    /**
     * Timing stages for Lance kNN search operations.
     * <p>
     * Stages are categorized into two types:
     * <ul>
     *   <li>One-time costs: Operations that happen once when the dataset is first opened</li>
     *   <li>Per-search costs: Operations that happen on every search request</li>
     * </ul>
     */
    public enum LanceTimingStage {
        // One-time costs (dataset opening)
        URI_SETUP("uri_setup", true),
        NATIVE_DATASET_OPEN("native_dataset_open", true),
        SCHEMA_PARSING("schema_parsing", true),
        INDEX_DETECTION("index_detection", true),

        // Per-search costs (every search)
        REGISTRY_CACHE_LOOKUP("registry_cache_lookup", false),
        VECTOR_SEARCH_SETUP("vector_search_setup", false),
        NATIVE_SCAN_SETUP("native_scan_setup", false),
        NATIVE_SEARCH_EXECUTION("native_search_execution", false),
        BATCH_PROCESSING("batch_processing", false),
        SCORE_CONVERSION("score_conversion", false),
        FILTER_PROCESSING("filter_processing", false),
        ID_MATCHING("id_matching", false),
        SCORE_AGGREGATION("score_aggregation", false);

        private final String name;
        private final boolean oneTime;

        LanceTimingStage(String name, boolean oneTime) {
            this.name = name;
            this.oneTime = oneTime;
        }

        public String getName() {
            return name;
        }

        public boolean isOneTime() {
            return oneTime;
        }

        /**
         * Get the field name for JSON output.
         */
        public String getFieldName() {
            return "lance_" + name + "_ms";
        }
    }

    private static class TimingData {
        final Map<LanceTimingStage, List<Long>> timings = new EnumMap<>(LanceTimingStage.class);
        private boolean active = false;

        void record(LanceTimingStage stage, long durationMs) {
            if (active) {
                timings.computeIfAbsent(stage, k -> new ArrayList<>()).add(durationMs);
            }
        }

        Map<LanceTimingStage, List<Long>> getTimings() {
            return Collections.unmodifiableMap(timings);
        }

        void clear() {
            timings.clear();
        }
    }

    private static final ThreadLocal<TimingData> CONTEXT = ThreadLocal.withInitial(TimingData::new);

    private final TimingData data;

    private LanceTimingContext(TimingData data) {
        this.data = data;
    }

    /**
     * Get or create the timing context for the current thread.
     */
    public static LanceTimingContext getOrCreate() {
        TimingData data = CONTEXT.get();
        return new LanceTimingContext(data);
    }

    /**
     * Get the existing timing context, or null if not present.
     */
    public static LanceTimingContext getOrNull() {
        TimingData data = CONTEXT.get();
        if (data != null && data.active) {
            return new LanceTimingContext(data);
        }
        return null;
    }

    /**
     * Activate timing collection for this thread.
     * This should be called at the start of a search request when profiling is enabled.
     */
    public void activate() {
        data.active = true;
    }

    /**
     * Deactivate timing collection for this thread.
     * This should be called at the end of a search request.
     */
    public void deactivate() {
        data.active = false;
    }

    /**
     * Clear all timing data for this thread.
     * This should be called between queries to prevent data leakage.
     */
    public void clear() {
        data.clear();
    }

    /**
     * Check if timing is currently active.
     */
    public boolean isActive() {
        return data.active;
    }

    /**
     * Record a timing measurement for a specific stage.
     *
     * @param stage The timing stage
     * @param durationMs Duration in milliseconds
     */
    public void record(LanceTimingStage stage, long durationMs) {
        data.record(stage, durationMs);
    }

    /**
     * Convert timing data to a map suitable for debugging output.
     * <p>
     * This method aggregates multiple measurements of the same stage
     * and provides summary statistics (sum, count, avg).
     *
     * @return Map containing timing breakdown in milliseconds
     */
    public Map<String, Object> toDebugMap() {
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        long totalOneTime = 0;
        long totalPerSearch = 0;

        for (LanceTimingStage stage : LanceTimingStage.values()) {
            List<Long> values = data.timings.get(stage);
            if (values != null && values.isEmpty() == false) {
                long sum = 0;
                for (Long v : values) {
                    sum += v;
                }
                result.put(stage.getFieldName(), sum);

                if (stage.isOneTime()) {
                    totalOneTime += sum;
                } else {
                    totalPerSearch += sum;
                }
            } else {
                result.put(stage.getFieldName(), 0L);
            }
        }

        result.put("lance_total_onetime_ms", totalOneTime);
        result.put("lance_total_persearch_ms", totalPerSearch);
        result.put("lance_total_ms", totalOneTime + totalPerSearch);

        return result;
    }

    /**
     * Clear the thread-local context for the current thread.
     * This should be called during cleanup to prevent memory leaks.
     */
    public static void remove() {
        CONTEXT.remove();
    }
}
