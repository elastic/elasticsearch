/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.Map;
import java.util.OptionalLong;

/**
 * Serialization vocabulary and value types for external-text aggregate metadata. The cache itself is
 * the unified {@link SchemaCacheEntry}: captured stats flow data-node → coordinator via
 * {@code DriverCompletionInfo.capturedSourceMetadata}, get reconciled into a {@code SchemaCacheEntry}'s
 * {@code safeMetadata} as the well-known {@code _stats.*} keys below, and the optimizer
 * ({@code PushAggregatesToExternalSource}) short-circuits {@code COUNT(*) / COUNT(col) / MIN(col) /
 * MAX(col)} on warm queries. This type holds only the key names and the {@link Stats} / {@link ColumnStats}
 * records shared across the capture, reconcile, and lookup sites.
 */
public final class ExternalStats {

    /**
     * mtime (epoch millis) published into {@code SourceMetadata.sourceMetadata()} so the warm-path
     * lookup can match the cached entry without a storage round-trip.
     */
    public static final String MTIME_MILLIS_KEY = "_stats.file_mtime_millis";

    /**
     * Node-stable fingerprint of the row-interpretation-affecting config (see
     * {@link SchemaCacheKey#buildFormatConfig}). Distinct fingerprints scope distinct entries so a
     * same-file re-query under different {@code WITH} options does not serve stale stats.
     */
    public static final String CONFIG_FINGERPRINT_KEY = "_stats.config_fingerprint";

    /**
     * Set on per-chunk contributions to signal "this is a partial — sum to a whole only if
     * accompanied by a {@link #FINALIZE_CHUNKS_KEY} marker for the same file". Coordinator-side
     * reconciliation enforces the gate.
     */
    public static final String PARTIAL_CHUNK_KEY = "_stats.partial_chunk";

    /**
     * Published by {@code ParallelParsingCoordinator}'s outer iterator at clean whole-file
     * completion to signal "the per-chunk contributions for this file constitute the entire
     * file." Without this marker, partial contributions are discarded.
     */
    public static final String FINALIZE_CHUNKS_KEY = "_stats.finalize_chunks";

    /**
     * Published by any chunk whose iterator dropped rows (rowsSkipped > 0). The presence of this
     * marker in any contribution for a file poisons the file's merge — the coordinator discards every
     * contribution rather than commit a policy-dependent count. Defeats the SKIP_ROW edge case where
     * one chunk drops rows silently, the others succeed cleanly, the finalize marker fires, and the
     * merged rowCount under-counts the file.
     */
    public static final String CHUNK_HAD_ERRORS_KEY = "_stats.chunk_had_errors";

    private ExternalStats() {}

    /**
     * Structured per-file statistics captured during a clean whole-file (or summed-chunk) cold scan.
     *
     * @param rowCount  total rows in the file (always populated; the capture gate refuses to write a partial count)
     * @param bytesRead bytes consumed from the input stream; present only for stream-only sources where
     *                  {@code StorageObject.length()} is unknown, so the value is captured during scan
     * @param columns   per-column stats keyed by column name; empty when no columns were materialized
     *                  during the cold scan (e.g. {@code STATS COUNT(*)} with no projection)
     */
    public record Stats(long rowCount, OptionalLong bytesRead, Map<String, ColumnStats> columns) {
        public Stats {
            columns = columns == null ? Map.of() : Map.copyOf(columns);
            bytesRead = bytesRead == null ? OptionalLong.empty() : bytesRead;
        }
    }

    /**
     * Per-column statistics. Null {@code min} / {@code max} means the column is untracked at this layer
     * (e.g. type without an ordered comparator at the capture site, or all rows were null).
     */
    public record ColumnStats(long nullCount, Object min, Object max) {}
}
