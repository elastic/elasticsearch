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

    private static final int MAX_ENTRIES = 10_000;

    private record Key(String path, long mtimeMillis) {}

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
        try {
            Instant mtime = object.lastModified();
            if (mtime == null) {
                return Optional.empty();
            }
            return lookup(object.path().toString(), mtime.toEpochMilli());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Overload used by the warm-query path, where mtime is already resolved from the cached schema entry. */
    public static Optional<Stats> lookup(String path, long mtimeMillis) {
        try {
            Stats v = CACHE.get(new Key(path, mtimeMillis));
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
            CACHE.put(new Key(object.path().toString(), mtime.toEpochMilli()), stats);
        } catch (Exception e) {
            // Cache write failures degrade silently — next query repopulates.
        }
    }

    /** Test isolation. */
    public static void clearForTests() {
        CACHE.invalidateAll();
    }
}
