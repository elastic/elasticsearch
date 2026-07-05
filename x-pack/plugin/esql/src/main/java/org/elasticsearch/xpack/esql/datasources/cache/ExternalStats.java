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
     * Set on per-chunk/per-segment contributions to mark them as a partial cover of the file (as
     * opposed to a whole-file read). A partial also carries a coverage range (see {@link
     * #COVERAGE_START_KEY}); the coordinator reconciler unions partials by range and commits only
     * when they tile the file. Whole-file reads carry neither marker and stay on the authoritative
     * dedup path.
     */
    public static final String PARTIAL_CHUNK_KEY = "_stats.partial_chunk";

    /**
     * Coverage-addressing keys. Every stats contribution describes the half-open byte range
     * {@code [COVERAGE_START_KEY, COVERAGE_END_KEY)} of the file it observed, in that path's own read
     * coordinate system (decompressed-stream offset for stream codecs like gzip/zstd; raw file offset
     * for uncompressed or block-splittable inputs — a single file is read in exactly one coordinate
     * system per {@code (path, config)}, so ranges are always comparable). The range is the
     * contribution's <em>intrinsic identity</em>: the coordinator reconciler unions contributions by
     * range, so a range observed more than once — the two branches of a FORK each re-scanning the
     * source, a schema-probe pass plus the data scan, a retry, a redelivery — is counted once, while
     * disjoint ranges (parallel chunks, record-aligned macro-splits, block splits, splits spread
     * across nodes) are summed. This replaces scan/finalize counting, which was a brittle proxy: "how
     * many times was it read" is an implementation detail, "which bytes did this cover" is not.
     * <p>
     * {@link #COVERAGE_IS_LAST_KEY} marks the contribution that observed the end of the input. A
     * cover is complete — and therefore cacheable as a file-level statistic — only when the unioned
     * ranges tile {@code [0, end)} with no gap and the final range is flagged last. The keys ride
     * inside the opaque {@code _stats.*} map, so there is no transport-version impact; an older node
     * emits no coverage and its contribution is treated as un-addressable (never cached).
     */
    public static final String COVERAGE_START_KEY = "_stats.coverage_start";
    public static final String COVERAGE_END_KEY = "_stats.coverage_end";
    public static final String COVERAGE_IS_LAST_KEY = "_stats.coverage_is_last";

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
