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
     * Per-file capture surface. Keyed by file path; the value is the list of flat
     * {@code _stats.*} contributions from each iterator that touched the file — one per chunk for
     * parallel parsing, one per split for macro-splits, one for whole-file reads.
     * <p>
     * {@link DriverCompletionInfo}'s factories collect these from every completed operator and
     * concatenate per file path into {@code DriverCompletionInfo.capturedSourceMetadata}. The
     * per-path list is merged later by esql-side consumers (the compute module cannot reach the
     * merge primitive). Returning {@link Map#of()} when nothing was captured is fine.
     */
    Map<String, List<Map<String, Object>>> capturedSourceMetadata();
}
