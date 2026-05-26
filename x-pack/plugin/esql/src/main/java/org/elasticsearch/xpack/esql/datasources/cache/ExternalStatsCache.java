/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Per-file aggregate-metadata cache for line-oriented external text formats. Key is {@code (path, mtime)};
 * value is a structured {@link Stats} record carrying {@code rowCount}, optional stream-derived
 * {@code bytesRead}, and per-column {@link ColumnStats} (nullCount + min + max). Lets
 * {@code PushAggregatesToExternalSource} / {@code PushStatsToExternalSource} short-circuit
 * {@code COUNT(*) / COUNT(col) / MIN(col) / MAX(col)} on warm queries.
 * <p>
 * Coarse-resolution mtime collisions are bounded by {@link FooterByteCache#EXPIRE_AFTER_ACCESS_SECONDS}.
 */
public final class ExternalStatsCache {

    /** mtime (epoch millis) published into {@code SourceMetadata.sourceMetadata()} so the warm-path cache lookup can avoid a storage round-trip. */
    public static final String MTIME_MILLIS_KEY = "_stats.file_mtime_millis";

    /**
     * Per-reader fingerprint of effective row-interpretation config (format options + error policy +
     * schema). Distinct fingerprints produce distinct cache entries so a same-file re-query under
     * different {@code WITH} options does not serve stale stats.
     */
    public static final String CONFIG_FINGERPRINT_KEY = "_stats.config_fingerprint";

    /** Used in lookup / put when a caller cannot compute a fingerprint; treats the entry as "unscoped". */
    public static final String NO_FINGERPRINT = "";

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
     * Published by any chunk whose iterator observed parse errors (errorCount > 0). The presence
     * of this marker in any contribution for a file poisons the file's merge — the coordinator
     * discards every contribution rather than commit a policy-dependent count. Defeats the
     * SKIP_ROW edge case where one chunk drops rows silently, the others succeed cleanly, the
     * finalize marker fires, and the merged rowCount under-counts the file.
     */
    public static final String CHUNK_HAD_ERRORS_KEY = "_stats.chunk_had_errors";

    private static final int MAX_ENTRIES = 10_000;

    private record Key(String path, long mtimeMillis, String configFingerprint) {}

    private static final Cache<Key, Stats> CACHE = CacheBuilder.<Key, Stats>builder()
        .setMaximumWeight(MAX_ENTRIES)
        .setExpireAfterAccess(TimeValue.timeValueSeconds(FooterByteCache.EXPIRE_AFTER_ACCESS_SECONDS))
        .build();

    private ExternalStatsCache() {}

    /**
     * Structured per-file statistics captured during a clean whole-file cold scan.
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

        public static Stats rowCountOnly(long rowCount) {
            return new Stats(rowCount, OptionalLong.empty(), Map.of());
        }
    }

    /**
     * Per-column statistics. Null {@code min} / {@code max} means the column is untracked at this layer
     * (e.g. type without an ordered comparator at the capture site, or all rows were null).
     */
    public record ColumnStats(long nullCount, Object min, Object max) {}

    public static Optional<Stats> lookup(StorageObject object) {
        return lookup(object, NO_FINGERPRINT);
    }

    public static Optional<Stats> lookup(StorageObject object, String configFingerprint) {
        try {
            Instant mtime = object.lastModified();
            if (mtime == null) {
                return Optional.empty();
            }
            return lookup(object.path().toString(), mtime.toEpochMilli(), configFingerprint);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Overload used by the warm-query path, where mtime is already resolved from the cached schema entry. */
    public static Optional<Stats> lookup(String path, long mtimeMillis) {
        return lookup(path, mtimeMillis, NO_FINGERPRINT);
    }

    public static Optional<Stats> lookup(String path, long mtimeMillis, String configFingerprint) {
        try {
            Stats v = CACHE.get(new Key(path, mtimeMillis, configFingerprint == null ? NO_FINGERPRINT : configFingerprint));
            return v == null ? Optional.empty() : Optional.of(v);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Convenience wrapper preserved for call sites that only need the row count. */
    public static OptionalLong lookupRowCount(StorageObject object) {
        return lookup(object).map(s -> OptionalLong.of(s.rowCount())).orElse(OptionalLong.empty());
    }

    /** Convenience wrapper preserved for call sites that only need the row count. */
    public static OptionalLong lookupRowCount(String path, long mtimeMillis) {
        return lookup(path, mtimeMillis).map(s -> OptionalLong.of(s.rowCount())).orElse(OptionalLong.empty());
    }

    /** Convenience wrapper for call sites that only have a row count. */
    public static void put(StorageObject object, long rowCount) {
        put(object, Stats.rowCountOnly(rowCount));
    }

    /** The gate (whole-file, natural EOF, zero errors) is the caller's. Null mtime drops the write — no trusted identity. */
    public static void put(StorageObject object, Stats stats) {
        try {
            Instant mtime = object.lastModified();
            if (mtime == null) {
                return;
            }
            put(object.path().toString(), mtime.toEpochMilli(), NO_FINGERPRINT, stats);
        } catch (Exception e) {
            // Cache write failures degrade silently — next query repopulates.
        }
    }

    /**
     * Pinned-mtime overload (no fingerprint). Backwards-compatible entry point for callers that do
     * not yet scope their cache entries by config fingerprint.
     */
    public static void put(String path, long mtimeMillis, Stats stats) {
        put(path, mtimeMillis, NO_FINGERPRINT, stats);
    }

    /**
     * Full key overload. Callers pin mtime at scan-open and pass a {@code configFingerprint} derived
     * from the format reader's effective options + schema so a same-file re-query under different
     * {@code WITH} options gets a distinct cache entry. {@link #NO_FINGERPRINT} for callers that
     * cannot compute one — they share an "unscoped" cache entry and accept the drift risk.
     */
    public static void put(String path, long mtimeMillis, String configFingerprint, Stats stats) {
        try {
            CACHE.put(new Key(path, mtimeMillis, configFingerprint == null ? NO_FINGERPRINT : configFingerprint), stats);
        } catch (Exception e) {
            // Cache write failures degrade silently — next query repopulates.
        }
    }

    /** Test isolation. */
    public static void clearForTests() {
        CACHE.invalidateAll();
    }

    /**
     * Test-only: returns the first cached entry matching {@code (path, mtime)} regardless of
     * fingerprint. Production code paths always look up by full key — this affordance exists so
     * tests can assert "the iterator's capture hook wrote something" without needing to recompute
     * the format reader's exact fingerprint.
     */
    public static Optional<Stats> lookupAnyForTests(StorageObject object) {
        try {
            Instant mtime = object.lastModified();
            if (mtime == null) {
                return Optional.empty();
            }
            String pathStr = object.path().toString();
            long mtimeMillis = mtime.toEpochMilli();
            for (var key : CACHE.keys()) {
                if (key.path().equals(pathStr) && key.mtimeMillis() == mtimeMillis) {
                    Stats v = CACHE.get(key);
                    if (v != null) {
                        return Optional.of(v);
                    }
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
