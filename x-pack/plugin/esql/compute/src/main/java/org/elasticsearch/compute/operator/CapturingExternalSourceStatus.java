/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.List;
import java.util.Map;

/**
 * Implemented by {@link Operator.Status} subclasses that capture per-file source-metadata
 * contributions during data-node execution. The contributions are aggregated by
 * {@link DriverCompletionInfo} into the return-flow back to the coordinator so the coordinator can
 * enrich its {@code SchemaCacheEntry} for future queries.
 * <p>
 * The capture contract follows the well-known {@code _stats.*} flat-map key convention used by
 * esql's {@code SourceStatisticsSerializer}, so the coordinator's existing aggregation primitive
 * ({@code SourceStatisticsSerializer.mergeStatistics}) can merge across data nodes / chunks without
 * format-specific branching.
 */
public interface CapturingExternalSourceStatus {

    /**
     * Returns a per-file flat-map contribution. Keyed by file path; the value is one
     * {@code Map<String, Object>} with {@code _stats.*} keys for whatever this operator scanned —
     * one chunk's worth of stats for a parallel-parsed file, one split's worth for a macro-split,
     * a whole file's worth for a single-split read.
     * <p>
     * {@link DriverCompletionInfo}'s factories collect these from every completed operator and
     * concatenate per file path into {@code DriverCompletionInfo.capturedSourceMetadata}. The
     * per-path list is merged later by esql-side consumers (the compute module cannot reach the
     * merge primitive). Returning {@link Map#of()} when nothing was captured is fine.
     */
    Map<String, Map<String, Object>> capturedSourceMetadata();

    /**
     * Convenience for operators that want to expose a list rather than a single map per path —
     * default implementation calls {@link #capturedSourceMetadata()} and wraps each value in a
     * singleton list. Most callers will not need to override this.
     */
    default Map<String, List<Map<String, Object>>> capturedSourceMetadataAsLists() {
        Map<String, Map<String, Object>> snapshot = capturedSourceMetadata();
        if (snapshot == null || snapshot.isEmpty()) {
            return Map.of();
        }
        java.util.HashMap<String, List<Map<String, Object>>> out = new java.util.HashMap<>(snapshot.size());
        for (Map.Entry<String, Map<String, Object>> e : snapshot.entrySet()) {
            out.put(e.getKey(), List.of(e.getValue()));
        }
        return out;
    }
}
