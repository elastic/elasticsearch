/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
@SuppressForbidden(reason = "enumerating declared constants is the point: a package-private key must not escape the gate")
public class StatsInvalidationScopeTests extends ESTestCase {

    /**
     * Statistics computed over the set of rows that SURVIVED the read. Any change to which rows survive — a declared
     * type that fails to coerce under {@code skip_row}, a narrower declared width, a pinned read type — invalidates
     * EVERY one of these, for EVERY column, not only for the column whose declaration changed.
     */
    private static final Set<String> ROW_SCOPED_SUFFIXES = Set.of(
        SourceStatisticsSerializer.NULL_COUNT_SUFFIX,
        SourceStatisticsSerializer.VALUE_COUNT_SUFFIX,
        SourceStatisticsSerializer.MIN_SUFFIX,
        SourceStatisticsSerializer.MAX_SUFFIX,
        // Bytes of the surviving values, so a dropped row moves it — and its only consumer is a cost estimate, which
        // is why it is cheap to clear and why clearing it cannot produce a wrong answer.
        SourceStatisticsSerializer.SIZE_BYTES_SUFFIX
    );

    /** Whole-source statistics with the same row-scoped dependency. */
    private static final Set<String> ROW_SCOPED_WHOLE_SOURCE = Set.of(SourceStatisticsSerializer.STATS_ROW_COUNT);

    /** Keys that depend on the file's bytes or the listing, never on how rows were interpreted. */
    private static final Set<String> BYTE_OR_LISTING_SCOPED = Set.of(
        SourceStatisticsSerializer.STATS_SIZE_BYTES,
        SourceStatisticsSerializer.STATS_FILE_COUNT
    );

    /** Keys that identify WHICH read produced the entry rather than carrying a measurement. */
    private static final Set<String> IDENTITY_SCOPED = Set.of(
        ExternalStats.MTIME_MILLIS_KEY,
        ExternalStats.CONFIG_FINGERPRINT_KEY,
        // Which read produced the entry, not a measurement over its rows — so a row drop does not invalidate it.
        ExternalStats.READ_CONFIG_FINGERPRINT_KEY
    );

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
        ExternalStats.STRIPE_GRID_KEY,
        // A licence attached to the row count, not a measurement of its own: it records that the producing policy
        // makes the count read-config-independent. Invalidated with the entry that carries it.
        ExternalStats.ROW_COUNT_READ_CONFIG_INDEPENDENT_KEY
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
            // Declared fields, not public ones: a key added package-private must not slip past this gate, and the
            // suffix constants right beside these keys are already package-private, so the precedent for a
            // non-public constant here is live. Mirrors CsvFormatReaderRecognizedKeysTests, which reaches declared
            // fields the same way for the same reason.
            for (Field f : holder.getDeclaredFields()) {
                if (f.getType() != String.class || Modifier.isStatic(f.getModifiers()) == false) {
                    continue;
                }
                String value;
                try {
                    f.setAccessible(true);
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
     * The second axis: how each key must behave when a MULTI-FILE read folds its files' statistics together. The
     * classification above says what invalidates a key; this one says what happens to it in a merge, and the two are
     * independent. A fold that rebuilds its output map from a fixed list of recognised keys drops everything else in
     * silence — no exception, no wrong value at the fold, just a key that stops existing. The damage lands later and
     * somewhere else: an identity key that vanishes turns a configuration-bound measurement into a configuration-less
     * one, and the serve gate then hands it to a read that could never have produced it.
     *
     * <p>That is not hypothetical. It is exactly how the multi-file merge came to serve one dataset another dataset's
     * row count and extrema, and the mechanism had been found twice before by review and fixed twice at a single site
     * without anyone enumerating the rest. Hence a gate rather than a memory: a new key must declare its fold
     * behaviour, and the declaration is checked against the merge that actually runs.
     */
    private enum FoldBehaviour {
        /** Summed across files — the fold's value is the total. */
        SUM,
        /** Must be equal across every input, and is carried through unchanged. Absent inputs make the fold absent. */
        CARRIED_IF_UNANIMOUS,
        /** A licence: true for the fold only when true for every input. */
        AND,
        /** Re-attached by the CALLER after the merge, because its correct fold is caller-specific. */
        CALLER_REATTACHED,
        /** Per-column families and cache-internal bookkeeping, folded or dropped by the compact model's own rules. */
        MODEL_INTERNAL
    }

    private static final Map<String, FoldBehaviour> FOLD_BEHAVIOUR = Map.ofEntries(
        Map.entry(SourceStatisticsSerializer.STATS_ROW_COUNT, FoldBehaviour.SUM),
        Map.entry(SourceStatisticsSerializer.STATS_SIZE_BYTES, FoldBehaviour.SUM),
        Map.entry(SourceStatisticsSerializer.STATS_FILE_COUNT, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.READ_CONFIG_FINGERPRINT_KEY, FoldBehaviour.CARRIED_IF_UNANIMOUS),
        Map.entry(ExternalStats.ROW_COUNT_READ_CONFIG_INDEPENDENT_KEY, FoldBehaviour.AND),
        // The cache-identity pair: stripes within one entry share an mtime, files in a glob do not, so the right
        // fold depends on who is calling. mergeStripesAndRekey re-attaches them from the entry it is committing to.
        Map.entry(ExternalStats.MTIME_MILLIS_KEY, FoldBehaviour.CALLER_REATTACHED),
        Map.entry(ExternalStats.CONFIG_FINGERPRINT_KEY, FoldBehaviour.CALLER_REATTACHED),
        Map.entry(SourceStatisticsSerializer.STATS_KEY_PREFIX, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(SourceStatisticsSerializer.STATS_COL_PREFIX, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(SourceStatisticsSerializer.STATS_PARTIAL, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.PARTIAL_CHUNK_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.COVERAGE_START_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.COVERAGE_END_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.COVERAGE_IS_LAST_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.CHUNK_HAD_ERRORS_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.STRIPE_SIZE_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.STRIPE_ORDINAL_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.STRIPE_AT_START_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.STRIPE_AT_END_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.STRIPE_ENTRY_PREFIX, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.STRIPE_LAST_INDEX_KEY, FoldBehaviour.MODEL_INTERNAL),
        Map.entry(ExternalStats.STRIPE_GRID_KEY, FoldBehaviour.MODEL_INTERNAL)
    );

    /** Every classified key must also declare what the multi-file merge does to it. */
    public void testEveryStatsKeyDeclaresFoldBehaviour() {
        Set<String> classified = new HashSet<>();
        classified.addAll(ROW_SCOPED_WHOLE_SOURCE);
        classified.addAll(BYTE_OR_LISTING_SCOPED);
        classified.addAll(IDENTITY_SCOPED);
        classified.addAll(BOOKKEEPING);

        Set<String> undeclared = new TreeSet<>(classified);
        undeclared.removeAll(FOLD_BEHAVIOUR.keySet());
        assertTrue(
            "stats key(s) "
                + undeclared
                + " have no declared fold behaviour: add each to FOLD_BEHAVIOUR after deciding"
                + " what a multi-file merge must do with it — a key nobody decided about is a key the merge drops",
            undeclared.isEmpty()
        );
    }

    /**
     * The declaration checked against the merge that actually runs. Two single-file maps carrying the key go through
     * {@code mergeStatistics}, and the fold's output must match what the key declared — so a fold that silently stops
     * carrying a key fails here rather than in production, and a key whose declaration drifts from the code fails too.
     */
    public void testDeclaredFoldBehaviourMatchesTheMerge() {
        for (Map.Entry<String, FoldBehaviour> declared : FOLD_BEHAVIOUR.entrySet()) {
            String key = declared.getKey();
            switch (declared.getValue()) {
                case SUM -> {
                    Map<String, Object> merged = mergeTwo(Map.of(key, 100L), Map.of(key, 200L));
                    assertEquals(key + " must sum across files", 300L, merged.get(key));
                }
                case CARRIED_IF_UNANIMOUS -> {
                    Map<String, Object> agreed = mergeTwo(Map.of(key, "same"), Map.of(key, "same"));
                    assertEquals(key + " must survive a merge every input agreed on", "same", agreed.get(key));
                    Map<String, Object> absent = mergeTwo(Map.of(), Map.of());
                    assertFalse(key + " must not be invented when no input carried it", absent.containsKey(key));
                    Map<String, Object> disagreeing = mergeTwo(Map.of(key, "a"), Map.of(key, "b"));
                    assertNotEquals(key + " must not present a disagreeing fold as either input's value", "a", disagreeing.get(key));
                    assertNotEquals(key + " must not present a disagreeing fold as either input's value", "b", disagreeing.get(key));
                }
                case AND -> {
                    Map<String, Object> both = mergeTwo(Map.of(key, Boolean.TRUE), Map.of(key, Boolean.TRUE));
                    assertEquals(key + " holds for the fold when it held for every input", Boolean.TRUE, both.get(key));
                    Map<String, Object> one = mergeTwo(Map.of(key, Boolean.TRUE), Map.of());
                    assertFalse(key + " must not survive an input that did not carry it", one.containsKey(key));
                }
                // Deliberately unchecked here: the merge is not their author. CALLER_REATTACHED keys are written by
                // the caller afterwards, and MODEL_INTERNAL keys are the compact model's own, covered by its tests.
                case CALLER_REATTACHED, MODEL_INTERNAL -> {
                }
            }
        }
    }

    private static Map<String, Object> mergeTwo(Map<String, Object> first, Map<String, Object> second) {
        Map<String, Object> a = new HashMap<>(first);
        Map<String, Object> b = new HashMap<>(second);
        // A row count in both, so the merge has a measurement to fold and does not short-circuit on empty input.
        a.putIfAbsent(SourceStatisticsSerializer.STATS_ROW_COUNT, 1L);
        b.putIfAbsent(SourceStatisticsSerializer.STATS_ROW_COUNT, 1L);
        return SourceStatisticsSerializer.mergeStatistics(List.of(a, b));
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
            stats.put(SourceStatisticsSerializer.STATS_COL_PREFIX + column + SourceStatisticsSerializer.SIZE_BYTES_SUFFIX, 512L);
        }
        return stats;
    }
}
