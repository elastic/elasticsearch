/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Determines whether a row group's statistics guarantee that ALL rows satisfy a
 * {@link FilterPredicate}. This is the logical complement of parquet-mr's
 * {@link StatisticsFilter}, which proves that NO rows match (so the row group can be skipped).
 *
 * <p>When this check returns {@code true} for a row group, the iterator can route that row
 * group through the standard read path: filter evaluation, survivor mask construction, and
 * compaction are all skipped. Decoding of predicate columns themselves is unchanged when those
 * columns are also part of the projection (the common case in this iterator).
 *
 * <p><b>Correctness invariants:</b>
 * <ul>
 *   <li>The check is conservative: any uncertainty (missing stats, unknown predicate forms,
 *       null counts that could affect the predicate) yields {@code false}.</li>
 *   <li>For comparison predicates the column must have NO nulls — a single null row would make
 *       the predicate return {@code false} for that row, so the filter cannot trivially pass.</li>
 *   <li>Row groups for which {@link Statistics#hasNonNullValue()} is {@code false} or
 *       {@link Statistics#isEmpty()} is {@code true} cannot be reasoned about.</li>
 * </ul>
 */
final class TriviallyPassesChecker {

    private TriviallyPassesChecker() {}

    /**
     * Returns {@code true} when row-group statistics prove every row satisfies {@code predicate}.
     *
     * @param predicate     the filter predicate (must be non-null)
     * @param rowGroup      the row group whose statistics are consulted
     * @return {@code true} if every row in the row group is guaranteed to pass the filter
     */
    static boolean check(FilterPredicate predicate, BlockMetaData rowGroup) {
        if (predicate == null || rowGroup == null) {
            return false;
        }
        return predicate.accept(new Visitor(rowGroup));
    }

    private static final class Visitor implements FilterPredicate.Visitor<Boolean> {

        private final BlockMetaData rowGroup;
        private final Map<ColumnPath, ColumnChunkMetaData> columns;

        Visitor(BlockMetaData rowGroup) {
            this.rowGroup = rowGroup;
            this.columns = new HashMap<>();
            for (ColumnChunkMetaData c : rowGroup.getColumns()) {
                columns.put(c.getPath(), c);
            }
        }

        private ColumnChunkMetaData column(Operators.Column<?> col) {
            return columns.get(col.getColumnPath());
        }

        /**
         * Returns column statistics suitable for reasoning about a comparison whose semantics
         * fail on null rows (Eq, NotEq, Lt, LtEq, Gt, GtEq, In, NotIn with non-null literals).
         * The result is non-null only when:
         * <ul>
         *   <li>the column exists and stats are populated ({@code !isEmpty} and {@code isNumNullsSet}),</li>
         *   <li>the stats observed at least one non-null value ({@code hasNonNullValue}),</li>
         *   <li>the column has no nulls ({@code getNumNulls() == 0}) — a single null row would make
         *       the predicate return false for that row, breaking the trivial-pass guarantee.</li>
         * </ul>
         * The single rawtype suppression here keeps the per-{@code visit} methods clean: callers
         * use the returned reference to invoke {@link Statistics#compareMinToValue}/{@code compareMaxToValue}
         * with the operator's typed value.
         */
        @SuppressWarnings("rawtypes")
        private Statistics nonNullableStats(Operators.Column<?> col) {
            ColumnChunkMetaData chunk = column(col);
            if (chunk == null) {
                return null;
            }
            Statistics<?> stats = chunk.getStatistics();
            if (stats == null || stats.isEmpty() || stats.isNumNullsSet() == false) {
                return null;
            }
            if (stats.hasNonNullValue() == false || stats.getNumNulls() > 0) {
                return null;
            }
            return stats;
        }

        // ---------- comparison predicates ----------

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Boolean visit(Operators.Eq<T> eq) {
            T value = eq.getValue();
            if (value == null) {
                // Eq(col, null) == IsNull(col): trivially passes iff every row is null.
                ColumnChunkMetaData chunk = column(eq.getColumn());
                return chunk != null && chunk.getValueCount() == numNulls(chunk);
            }
            Statistics stats = nonNullableStats(eq.getColumn());
            if (stats == null) {
                return false;
            }
            // min == max == value
            return stats.compareMinToValue(value) == 0 && stats.compareMaxToValue(value) == 0;
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Boolean visit(Operators.NotEq<T> notEq) {
            T value = notEq.getValue();
            if (value == null) {
                // NotEq(col, null) == IsNotNull(col): trivially passes iff no nulls.
                // Intentionally goes through nonNullableStats (which also requires hasNonNullValue)
                // so this branch is consistent with the comparison branches: stats with numNulls == 0
                // but no observed non-null value are too weak to prove "every row is non-null".
                return nonNullableStats(notEq.getColumn()) != null;
            }
            Statistics stats = nonNullableStats(notEq.getColumn());
            if (stats == null) {
                return false;
            }
            // value is strictly outside [min, max]
            return stats.compareMinToValue(value) > 0 || stats.compareMaxToValue(value) < 0;
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Boolean visit(Operators.Lt<T> lt) {
            Statistics stats = nonNullableStats(lt.getColumn());
            // max < value
            return stats != null && stats.compareMaxToValue(lt.getValue()) < 0;
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Boolean visit(Operators.LtEq<T> ltEq) {
            Statistics stats = nonNullableStats(ltEq.getColumn());
            return stats != null && stats.compareMaxToValue(ltEq.getValue()) <= 0;
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Boolean visit(Operators.Gt<T> gt) {
            Statistics stats = nonNullableStats(gt.getColumn());
            return stats != null && stats.compareMinToValue(gt.getValue()) > 0;
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Boolean visit(Operators.GtEq<T> gtEq) {
            Statistics stats = nonNullableStats(gtEq.getColumn());
            return stats != null && stats.compareMinToValue(gtEq.getValue()) >= 0;
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Boolean visit(Operators.In<T> in) {
            Set<T> values = in.getValues();
            if (values == null || values.isEmpty()) {
                return false;
            }
            // In(col, set) where the set already includes null is treated like
            // Eq(col, null) || In(col, set\{null}) — handled conservatively here.
            Statistics stats = nonNullableStats(in.getColumn());
            if (stats == null) {
                return false;
            }
            // Conservative: trivially passes only if the column has a single distinct value
            // (min == max) and that value is in the set. Stronger conditions exist in theory
            // but are rare in practice and add complexity.
            for (T candidate : values) {
                if (candidate == null) {
                    continue;
                }
                if (stats.compareMinToValue(candidate) == 0 && stats.compareMaxToValue(candidate) == 0) {
                    return true;
                }
            }
            return false;
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Boolean visit(Operators.NotIn<T> notIn) {
            Set<T> values = notIn.getValues();
            if (values == null || values.isEmpty()) {
                // NotIn(col, {}) is trivially true; but parquet-mr doesn't generate this.
                return false;
            }
            Statistics stats = nonNullableStats(notIn.getColumn());
            if (stats == null) {
                return false;
            }
            // Trivially passes when every set value is strictly outside [min, max]: no value in
            // the column can equal any value in the set.
            for (T candidate : values) {
                if (candidate == null) {
                    return false;
                }
                if (stats.compareMinToValue(candidate) <= 0 && stats.compareMaxToValue(candidate) >= 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(Operators.Contains<T> contains) {
            // Contains is for repeated columns; we don't reason about it.
            return false;
        }

        // ---------- logical predicates ----------

        @Override
        public Boolean visit(Operators.And and) {
            return and.getLeft().accept(this) && and.getRight().accept(this);
        }

        @Override
        public Boolean visit(Operators.Or or) {
            return or.getLeft().accept(this) || or.getRight().accept(this);
        }

        @Override
        public Boolean visit(Operators.Not not) {
            // Not(p) trivially passes iff p is provably empty for this row group — i.e., no row
            // matches p. Delegate to parquet-mr's StatisticsFilter for that direction so the
            // inversion is consistent with row-group pruning.
            //
            // parquet-mr's StatisticsFilter rejects nested Not nodes (its visit(Not) throws
            // IllegalArgumentException recommending LogicalInverseRewriter). Any other RuntimeException
            // thrown by parquet on malformed input is also degraded to "no optimization" so a
            // best-effort guard never breaks an otherwise-valid scan.
            FilterPredicate inner = not.getPredicate();
            List<ColumnChunkMetaData> cols = rowGroup.getColumns();
            try {
                return StatisticsFilter.canDrop(inner, cols);
            } catch (RuntimeException e) {
                return false;
            }
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(Operators.UserDefined<T, U> ud) {
            return false;
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(Operators.LogicalNotUserDefined<T, U> ud) {
            return false;
        }

        // ---------- helpers ----------

        private static long numNulls(ColumnChunkMetaData col) {
            Statistics<?> stats = col.getStatistics();
            return stats != null && stats.isNumNullsSet() ? stats.getNumNulls() : -1L;
        }
    }
}
