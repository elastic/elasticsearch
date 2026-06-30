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

    /**
     * Canonical-stripe addressing — orthogonal model. A file's stripe grid divides its byte stream
     * (decompressed-stream offset for stream codecs, raw file offset for seekable inputs) into stripes
     * of {@link ExternalSourceCacheSettings#STRIPE_SIZE} bytes. Stripes are a pure ADDRESSING grid,
     * not a partitioning unit: the producing READER attributes each record to the stripe its start
     * offset falls in ({@code ordinal = floor(recordStartOffset / stripe_size)}) as it parses,
     * independently of how the read was chunked, split, or distributed. Chunks are never cut to align
     * with stripes; a chunk spanning stripes contributes one fragment per stripe, and a chunk boundary
     * landing mid-stripe splits that stripe across two chunks' fragments.
     * <p>
     * Because attribution is by record-start offset, a stripe's content is a pure function of the file
     * — identical across any two scans regardless of their chunking — which is what makes the
     * reconciler's per-stripe interval-cover dedup exact (a FORK branch covering a stripe whole and a
     * sibling splitting it differently fold to the same stripe stats).
     * <ul>
     *   <li>{@link #STRIPE_SIZE_KEY} — the grid B (bytes); a grid-consistency check, also identifies a
     *   fragment as stripe-addressed (absent ⇒ not cacheable, a safe miss).</li>
     *   <li>{@link #STRIPE_ORDINAL_KEY} — the reader-assigned stripe ordinal k this fragment belongs
     *   to (NOT inferred from the byte offset; the reader knows it record-canonically).</li>
     *   <li>{@link #COVERAGE_START_KEY}/{@link #COVERAGE_END_KEY} — the record-canonical byte sub-range
     *   of stripe k this fragment covered, for the interval-cover tiling.</li>
     *   <li>{@link #STRIPE_AT_START_KEY} — this fragment holds the stripe's first record (its start is
     *   the stripe's true start).</li>
     *   <li>{@link #STRIPE_AT_END_KEY} — this fragment's end reached the next stripe's first record (or
     *   EOF): the stripe's true end.</li>
     *   <li>{@link #COVERAGE_IS_LAST_KEY} — this fragment observed end-of-input: the file's last
     *   stripe (drives the whole-file completeness marker).</li>
     * </ul>
     */
    public static final String STRIPE_SIZE_KEY = "_stats.stripe_size";
    public static final String STRIPE_ORDINAL_KEY = "_stats.stripe_ordinal";
    public static final String STRIPE_AT_START_KEY = "_stats.stripe_at_start";
    public static final String STRIPE_AT_END_KEY = "_stats.stripe_at_end";

    /**
     * Coordinator-cache keys for per-stripe committed stats, stored inside a {@code SchemaCacheEntry}'s
     * {@code safeMetadata} alongside the whole-file {@code _stats.*} fold. {@code _stats.stripe.<k>.}
     * prefixes one committed stripe's flat stats map (row_count, columns.*, plus its span); the
     * whole-file fold is written only when stripes {@code 0..K} are all committed and the marker
     * (the file-EOF stripe ordinal, {@link #STRIPE_LAST_INDEX_KEY}) is known. Commits are idempotent:
     * re-committing a stripe overwrites with identical content.
     */
    public static final String STRIPE_ENTRY_PREFIX = "_stats.stripe.";
    public static final String STRIPE_LAST_INDEX_KEY = "_stats.stripe_last_index";

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
     *
     * @param nullCount  number of null cells (one per null row position; an empty multivalue is a null)
     * @param valueCount number of non-null VALUES — for a single-valued column this equals
     *                   {@code rows - nullCount}, but for a multivalued column it counts every value
     *                   (e.g. an NDJSON array {@code [a,b,c]} contributes 3). This is what
     *                   {@code COUNT(col)} returns ({@code Count}: "COUNTing a multivalued field returns
     *                   the number of values"), so it is served directly rather than derived from
     *                   {@code rows - nullCount}, which would under-count multivalued columns.
     */
    public record ColumnStats(long nullCount, long valueCount, Object min, Object max) {}
}
