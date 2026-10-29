/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

/**
 * Per-split statistics used by the optimizer for aggregate pushdown and filter pruning.
 * Implemented by {@link org.elasticsearch.xpack.esql.datasources.SplitStats} for
 * individual file splits and by {@link org.elasticsearch.xpack.esql.datasources.MergedSplitStats}
 * for composite splits that aggregate statistics lazily from their children.
 * <p>
 * Unknown numeric values are represented as {@code -1}; unknown object values as {@code null}.
 * This matches the sentinel convention used throughout the datasources layer
 * (see {@link ExternalSplit#estimatedSizeInBytes()}).
 */
public interface SplitStats {

    /** Total row count for this split. Always {@code >= 0}. */
    long rowCount();

    /**
     * Uncompressed size of the data in this split, in bytes.
     * Returns {@code -1} if unknown.
     */
    long sizeInBytes();

    /**
     * On-disk (compressed) size of this split, in bytes.
     * Returns {@code -1} if unknown.
     */
    default long compressedSizeInBytes() {
        return -1;
    }

    /**
     * Number of null values in the named column under the "implicit nulls" contract:
     * a column that is physically absent from this split contributes {@code rowCount()}
     * implicit nulls (every row would deserialize as {@code null}). Returns {@code -1}
     * only when the column is physically present in the split but the reader could not
     * extract a null count (the rare Parquet present-but-stats-less case; ORC always emits one).
     * <p>
     * This contract makes {@code Count(col) = rowCount - columnNullCount} correct for
     * UNION_BY_NAME pushdown across files where some files lack the column, and lets
     * {@code IS NULL}/{@code IS NOT NULL} classifiers treat absent-column splits as
     * unconditionally null without needing a separate "column present?" probe.
     * <p>
     * <b>Producer contract:</b> implementations must mark a column as physically present
     * by carrying at least one column-family stat (e.g. {@code size_bytes}, a min/max value,
     * or a null count). Both Parquet and ORC readers satisfy this — Parquet always emits
     * {@code size_bytes} and ORC always emits {@code null_count}. Producers that build
     * stats by hand must follow the same rule, otherwise this method will silently report
     * a present column as absent and inflate the implicit-null contribution.
     */
    long columnNullCount(String name);

    /**
     * Number of non-null values in the named column (multivalue-aware: a multivalued cell
     * contributes one per value). This is what {@code COUNT(col)} returns, served directly
     * rather than derived from {@code rowCount - columnNullCount} (which under-counts
     * multivalued columns).
     * <p>
     * Returns {@code -1} when not available; the caller then falls back to
     * {@code rowCount - nullCount}.
     */
    default long columnValueCount(String name) {
        return -1;
    }

    /**
     * Whether the named column carries any per-column statistics in this split (a null count, a
     * min/max, or a size). This is the "column was observed by the stats layer" predicate, distinct
     * from {@link #columnNullCount}'s implicit-nulls contract: for footer formats an absent column is
     * genuinely all-null and {@code columnNullCount} returns {@code rowCount}, but a text-format
     * partial harvest can leave a physically present column with no stats at all, and the two cases
     * are indistinguishable through {@code columnNullCount} alone. Callers that must not apply the
     * implicit-nulls contract for unharvested columns (see
     * {@link org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport#appliesImplicitNullsForAbsentColumn()})
     * use this to safe-miss instead.
     *
     * <p>Defaults to {@code false} (conservative — "no per-column stats observed"). Footer-format
     * implementations never consult it (they declare {@code appliesImplicitNullsForAbsentColumn}), so
     * they need not override it; partial-harvest text formats override it with the real predicate.
     */
    default boolean hasColumn(String name) {
        return false;
    }

    /**
     * Minimum value for the named column, or {@code null} if unknown or the column
     * is not present in this split's statistics.
     */
    @Nullable
    Object columnMin(String name);

    /**
     * Maximum value for the named column, or {@code null} if unknown or the column
     * is not present in this split's statistics.
     */
    @Nullable
    Object columnMax(String name);

    /**
     * Uncompressed size in bytes for the named column, or {@code -1} if unknown or the
     * column is not present in this split's statistics.
     */
    default long columnSizeBytes(String name) {
        return -1;
    }
}
