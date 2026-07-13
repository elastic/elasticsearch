/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * Lazily merges statistics from a list of child {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats} instances.
 * Used by {@link CoalescedSplit#splitStats()} to expose aggregate statistics for
 * composite splits without eagerly materializing a full {@link SplitStats}.
 * <p>
 * Each accessor method computes its result on demand by iterating the children.
 * This is appropriate because the optimizer calls these methods at most a few times
 * per planning phase, and the child count is typically small (a few dozen splits).
 */
public final class MergedSplitStats implements org.elasticsearch.xpack.esql.datasources.spi.SplitStats {

    private final List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children;

    public MergedSplitStats(List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children) {
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("children cannot be null or empty");
        }
        this.children = List.copyOf(children);
    }

    /** Returns the list of child stats that this instance merges. */
    public List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children() {
        return children;
    }

    @Override
    public long rowCount() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            total += child.rowCount();
        }
        return total;
    }

    /**
     * Returns the sum of children's uncompressed sizes, or {@code -1} if any child
     * reports an unknown size. A single unknown child poisons the aggregate because
     * we cannot give a meaningful partial sum to the optimizer.
     */
    @Override
    public long sizeInBytes() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long s = child.sizeInBytes();
            if (s < 0) {
                return -1;
            }
            total += s;
        }
        return total;
    }

    /**
     * Returns the sum of children's compressed sizes, or {@code -1} if any child
     * reports an unknown compressed size.
     */
    @Override
    public long compressedSizeInBytes() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long s = child.compressedSizeInBytes();
            if (s < 0) {
                return -1;
            }
            total += s;
        }
        return total;
    }

    /**
     * Returns the sum of null counts across children for the named column under the SPI's
     * "implicit nulls" contract: a child whose split lacks the column contributes its full
     * row count (every row is an implicit null), and explicit nulls in present columns are
     * summed normally. Returns {@code -1} only if a child returns {@code -1}, which signals
     * the rare "column physically present but stats unknown" case (Parquet stats disabled).
     */
    @Override
    public long columnNullCount(String name) {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long nc = child.columnNullCount(name);
            if (nc < 0) {
                return -1;
            }
            total += nc;
        }
        return total;
    }

    /**
     * Returns the sum of non-null value counts across children for the named column (multivalue-aware),
     * or {@code -1} if any child returns {@code -1} (the column was not observed in that child's stats).
     * A single unavailable child poisons the aggregate because the summed COUNT(col) would otherwise
     * under-count that child's rows; the caller then falls back to {@code rowCount - columnNullCount}.
     */
    @Override
    public long columnValueCount(String name) {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long vc = child.columnValueCount(name);
            if (vc < 0) {
                return -1;
            }
            total += vc;
        }
        return total;
    }

    /**
     * Returns {@code true} only when <b>every</b> child observed the column in its stats. A single
     * child that lacks the column makes the merged answer "not fully harvested": for a text-format
     * multi-file query where one file's scan harvested the column and another's did not, the merged
     * {@code COUNT(col)} cannot be served from stats without under-counting the unharvested file's
     * rows, so the all-children predicate forces a safe-miss. Footer formats never consult this (their
     * {@link org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport} applies implicit
     * nulls), so genuinely-absent columns in a UNION_BY_NAME mix are unaffected.
     */
    @Override
    public boolean hasColumn(String name) {
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            if (child.hasColumn(name) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the minimum of children's min values for the named column under the SPI's
     * "implicit nulls" contract:
     * <ul>
     *   <li>A child that reports a <b>known min value</b> contributes it to the merge via
     *       {@link SplitStats#mergedMin}, <b>regardless of its null count</b>. A min stat is the
     *       minimum of the column's non-null values, so it is a valid extremum candidate even when
     *       the child's null count is unknown ({@code columnNullCount(name) < 0}) — null count only
     *       bears on {@code COUNT}, never on {@code MIN}/{@code MAX}. This is the multi-FILE case
     *       where a per-file {@code SplitStats} carries a folded min/max but its null_count was
     *       poison-dropped during that file's own multi-stripe fold.</li>
     *   <li>A child with <b>no min value</b> is then classified by its null count:
     *       {@code columnNullCount(name) == child.rowCount()} means the column is absent or all-null,
     *       so the child has no candidate and is <b>skipped</b>; {@code columnNullCount(name) < 0}
     *       (present but stats unknown — e.g. Parquet stats disabled) <b>poisons</b> the aggregate
     *       because the child may hold a smaller value we cannot see; a finite null count below the
     *       row count with no min is an inconsistent reader output and poisons defensively.</li>
     * </ul>
     * Incompatible numeric types across known mins poison the aggregate. Returns {@code null} when
     * poisoned or when no child contributes a value.
     */
    @Override
    @Nullable
    public Object columnMin(String name) {
        return mergeExtremum(name, true);
    }

    /**
     * Returns the maximum of children's max values for the named column under the SPI's
     * "implicit nulls" contract. Mirrors {@link #columnMin} — a child with a known max contributes
     * it regardless of its null count (null count bears only on {@code COUNT}); a child with no max
     * is skipped when the column is absent/all-null and otherwise poisons defensively.
     */
    @Override
    @Nullable
    public Object columnMax(String name) {
        return mergeExtremum(name, false);
    }

    /**
     * Shared min/max value-fold across children under the SPI's implicit-nulls contract (a child whose null
     * count equals its row count contributes no candidate; a child present with no extremum, or whose null count
     * is unknown so all-null-ness cannot be proven, poisons defensively; incompatible numeric types across
     * contributing children poison).
     * <p>
     * This is a PURE value-fold — it does NOT reconcile units or representations. Per-file stats are normalized
     * to the reconciled query type at the source boundary
     * ({@link SourceStatisticsSerializer#normalizeStatsToReconciled}, applied in
     * {@code ExternalSourceResolver.aggregateFileStatistics} for the whole-file fold and in
     * {@code FileSplitProvider} for footer split stats) BEFORE any split or source carries them, so every child
     * here is already in one unit/representation. That boundary is the single owner of the unit axis; a second
     * reconciliation here would double-apply it (e.g. re-rescale an already-{@code DATE_NANOS} value ×1e6).
     */
    @Nullable
    private Object mergeExtremum(String name, boolean wantMin) {
        Object result = null;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            // Value-first: a known extremum is a valid candidate regardless of this child's null_count. MIN/MAX
            // ignore nulls, so the min/max of the non-null values is correct even when null_count is unknown (-1)
            // — this preserves the multi-file warm short-circuit when a per-file fold dropped only the null_count
            // (see MergedSplitStatsTests.testColumnMinMaxUsesChildValueWhenNullCountUnknownButMinMaxPresent).
            Object value = wantMin ? child.columnMin(name) : child.columnMax(name);
            if (value == null) {
                // No extremum candidate. All-null (null_count == rowCount) contributes nothing → skip. Otherwise
                // the column is present without an extremum, or its null_count is unknown so all-null-ness can't be
                // proven — either way poison defensively (safe-miss, never a wrong value).
                long nc = child.columnNullCount(name);
                if (nc == child.rowCount()) {
                    continue;
                }
                return null;
            }
            result = wantMin ? SplitStats.mergedMin(result, value) : SplitStats.mergedMax(result, value);
            if (result == null) {
                // Incompatible types — clear the stat.
                return null;
            }
        }
        return result;
    }

    /**
     * Returns the sum of per-column sizes across children, or {@code -1} if any child
     * returns an unknown size for the column.
     */
    @Override
    public long columnSizeBytes(String name) {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long sz = child.columnSizeBytes(name);
            if (sz < 0) {
                return -1;
            }
            total += sz;
        }
        return total;
    }

    @Override
    public String toString() {
        return "MergedSplitStats[children=" + children.size() + "]";
    }
}
