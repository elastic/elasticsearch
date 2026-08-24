/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Invariants over the {@code _stats.*} vocabulary — what each statistic depends on, and therefore what invalidates it.
 *
 * <p>Two statistics can be damaged by different things. A per-column extremum is damaged when that column is read at a
 * different type; every per-column count and extremum in a file is damaged when a whole ROW disappears, because the
 * surviving row set is what every column statistic is computed over. Guards that name columns therefore cannot repair
 * row-scoped damage, and a guard written for one scope silently under-covers the other.
 */
public class StatsInvalidationScopeTests extends ESTestCase {

    /**
     * Statistics computed over the set of rows that SURVIVED the read. Any change to which rows survive — a declared
     * type that fails to coerce under {@code skip_row}, a narrower declared width, a pinned read type — invalidates
     * EVERY one of these, for EVERY column, not only for the column whose declaration changed.
     */
    private static final Set<String> ROW_SCOPED_SUFFIXES = Set.of(".null_count", ".value_count", ".min", ".max", ".size_bytes");

    /** Whole-source statistics with the same row-scoped dependency. */
    private static final Set<String> ROW_SCOPED_WHOLE_SOURCE = Set.of(SourceStatisticsSerializer.STATS_ROW_COUNT);

    /** Keys that depend on the file's bytes or the listing, never on how rows were interpreted. */
    private static final Set<String> BYTE_OR_LISTING_SCOPED = Set.of(
        SourceStatisticsSerializer.STATS_SIZE_BYTES,
        SourceStatisticsSerializer.STATS_FILE_COUNT
    );

    /** Keys that identify WHICH read produced the entry rather than carrying a measurement. */
    private static final Set<String> IDENTITY_SCOPED = Set.of(ExternalStats.MTIME_MILLIS_KEY, ExternalStats.CONFIG_FINGERPRINT_KEY);

    /**
     * Bookkeeping the cache itself consumes — completeness markers, stripe addressing, coverage ranges. Not served to
     * the planner as a measurement, so row-scope does not apply; they are invalidated with the entry they live in.
     */
    private static final Set<String> BOOKKEEPING = Set.of(
        SourceStatisticsSerializer.STATS_KEY_PREFIX,
        SourceStatisticsSerializer.STATS_COL_PREFIX,
        SourceStatisticsSerializer.STATS_PARTIAL,
        ExternalStats.PARTIAL_CHUNK_KEY,
        ExternalStats.COVERAGE_START_KEY,
        ExternalStats.COVERAGE_END_KEY,
        ExternalStats.COVERAGE_IS_LAST_KEY,
        ExternalStats.CHUNK_HAD_ERRORS_KEY,
        ExternalStats.STRIPE_SIZE_KEY,
        ExternalStats.STRIPE_ORDINAL_KEY,
        ExternalStats.STRIPE_AT_START_KEY,
        ExternalStats.STRIPE_AT_END_KEY,
        ExternalStats.STRIPE_ENTRY_PREFIX,
        ExternalStats.STRIPE_LAST_INDEX_KEY,
        ExternalStats.STRIPE_GRID_KEY
    );

    /**
     * Every {@code _stats.*} key declared anywhere must be classified above. A new statistic added without deciding
     * what invalidates it defaults to "nothing invalidates it", which is how a measurement ends up served to a read
     * that could never have produced it.
     */
    public void testEveryStatsKeyIsClassified() {
        Set<String> classified = new HashSet<>();
        classified.addAll(ROW_SCOPED_WHOLE_SOURCE);
        classified.addAll(BYTE_OR_LISTING_SCOPED);
        classified.addAll(IDENTITY_SCOPED);
        classified.addAll(BOOKKEEPING);

        Set<String> unclassified = new TreeSet<>();
        for (Class<?> holder : new Class<?>[] { SourceStatisticsSerializer.class, ExternalStats.class }) {
            // getFields(), not getDeclaredFields() + setAccessible: the repo forbids reaching past java's access
            // system, and every full _stats.* key is public by contract (the package-private constants in these
            // classes are statistic SUFFIXES, which are not keys on their own).
            for (Field f : holder.getFields()) {
                if (f.getType() != String.class || Modifier.isStatic(f.getModifiers()) == false) {
                    continue;
                }
                String value;
                try {
                    value = (String) f.get(null);
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
                if (value != null && value.startsWith("_stats.") && classified.contains(value) == false) {
                    unclassified.add(holder.getSimpleName() + "." + f.getName() + " = " + value);
                }
            }
        }
        assertTrue(
            "unclassified _stats key(s) "
                + unclassified
                + ": add each to one of ROW_SCOPED_WHOLE_SOURCE / BYTE_OR_LISTING_SCOPED / IDENTITY_SCOPED / "
                + "BOOKKEEPING after deciding what invalidates it",
            unclassified.isEmpty()
        );
    }

    /**
     * A guard told that rows were dropped must leave no row-scoped statistic behind, for any column — not only for the
     * columns it was handed. The columns named in the call are the ones whose TYPE changed; the dropped rows damage
     * every column in the file.
     */
    public void testPinnedServeGuardClearsRowScopedStatsForEveryColumn() {
        Map<String, Object> stats = statsForColumns("declared_col", "untouched_col");
        Map<String, Object> guarded = SourceStatisticsSerializer.overlayPinnedColumnsOnStats(stats, Set.of("declared_col"), true);
        assertNoRowScopedStatsSurvive("overlayPinnedColumnsOnStats", guarded);
    }

    /** The commit-side sibling of the above; same invariant, opposite direction. */
    public void testPinnedCommitGuardClearsRowScopedStatsForEveryColumn() {
        Map<String, Object> stats = statsForColumns("declared_col", "untouched_col");
        Map<String, Object> guarded = SourceStatisticsSerializer.removeColumnStatFamilies(stats, Set.of("declared_col"), true);
        assertNoRowScopedStatsSurvive("removeColumnStatFamilies", guarded);
    }

    private static void assertNoRowScopedStatsSurvive(String guard, Map<String, Object> guarded) {
        Set<String> survivors = new TreeSet<>();
        for (String key : guarded.keySet()) {
            if (ROW_SCOPED_WHOLE_SOURCE.contains(key)) {
                survivors.add(key);
                continue;
            }
            if (key.startsWith(SourceStatisticsSerializer.STATS_COL_PREFIX)) {
                for (String suffix : ROW_SCOPED_SUFFIXES) {
                    if (key.endsWith(suffix)) {
                        survivors.add(key);
                    }
                }
            }
        }
        assertTrue(
            guard
                + " was told rows were dropped but left row-scoped statistics "
                + survivors
                + " behind: the surviving row set is what every column statistic is computed over, so a row drop "
                + "invalidates every column's counts and extrema, not only the named columns'",
            survivors.isEmpty()
        );
    }

    private static Map<String, Object> statsForColumns(String... columns) {
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        stats.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, 4096L);
        for (String column : columns) {
            stats.put(SourceStatisticsSerializer.columnNullCountKey(column), 1L);
            stats.put(SourceStatisticsSerializer.columnValueCountKey(column), 99L);
            stats.put(SourceStatisticsSerializer.columnMinKey(column), 1L);
            stats.put(SourceStatisticsSerializer.columnMaxKey(column), 42L);
        }
        return stats;
    }
}
