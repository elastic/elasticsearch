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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decides, per statistics-survivor row group, whether parquet-mr's DICTIONARY or BLOOMFILTER
 * levels could still drop the group. The open path uses this to skip preloading dictionary and
 * bloom pages that cannot change the survivor set.
 *
 * <p>The check is conservative: a group is useful if any leaf under AND/OR/NOT is useful.
 * Over-fetching is today's behaviour and cannot drop rows incorrectly. Skipping a useful level
 * only keeps extra row groups; the pushed predicate still filters rows.
 *
 * <p>{@code Contains} is not pushed by ES|QL today and always returns {@code NONE}. Revisit the
 * leaf if a contains pushdown is added.
 *
 * <p>Per leaf on a stats-survivor group:
 * <ul>
 *   <li>MISS ({@link StatisticsFilter#canDrop}): the leaf cannot keep the group; neither level
 *       helps.</li>
 *   <li>MATCH (same null-safe min/max rules as {@link TriviallyPassesChecker}): every row already
 *       satisfies the leaf, so neither level can drop the group.</li>
 *   <li>AMBIGUOUS or missing stats: dictionary is useful when
 *       {@link ParquetFormatReader#dictionaryFilterReadsDictionary} and the op is {@code Eq} /
 *       {@code In} / {@code NotIn} / {@code UserDefined}. Comparison and {@code NotEq} are useful
 *       only when min/max are missing. parquet-mr does not shorten row-group min/max in the footer
 *       by default (it does for page indexes), so the dictionary's min and max equal the footer's:
 *       an extra dictionary GET cannot drop more groups than STATISTICS. Writers that truncate
 *       footer stats would lose that extra pruning; those files still filter rows correctly, they
 *       just keep extra groups. This is the rule that lets the VPC shape skip the dictionary GET.
 *       Bloom is useful when the chunk records a bloom offset and length and the op is {@code Eq}
 *       / {@code In}. Null-valued {@code Eq}/{@code NotEq} and {@code In}/{@code NotIn} sets that
 *       contain null return {@code NONE}: parquet-mr's dictionary and bloom visitors answer
 *       {@code BLOCK_MIGHT_MATCH} without reading those pages.</li>
 * </ul>
 */
final class FilterLevelUsefulness {

    private static final Logger logger = LogManager.getLogger(FilterLevelUsefulness.class);

    private FilterLevelUsefulness() {}

    /**
     * Fills {@code dictUseful} and {@code bloomUseful} for each stats-survivor in {@code blocks}.
     * Indexes where {@code statsSurvivors[i]} is {@code false} stay {@code false} — MISS groups
     * do not pre-warm dictionary or bloom pages. A {@code null} predicate is a no-op. Arrays must
     * be {@code blocks.size()} long. A {@code false} bit is the skip; {@code true} is today's
     * preload for that group.
     */
    static void fill(
        FilterPredicate predicate,
        List<BlockMetaData> blocks,
        boolean[] statsSurvivors,
        boolean[] dictUseful,
        boolean[] bloomUseful
    ) {
        if (predicate == null) {
            return;
        }
        for (int i = 0; i < blocks.size(); i++) {
            if (statsSurvivors[i] == false) {
                continue;
            }
            Usefulness usefulness = predicate.accept(new Visitor(blocks.get(i)));
            dictUseful[i] = usefulness.dict;
            bloomUseful[i] = usefulness.bloom;
        }
    }

    private record Usefulness(boolean dict, boolean bloom) {
        static final Usefulness NONE = new Usefulness(false, false);

        Usefulness or(Usefulness other) {
            return new Usefulness(dict || other.dict, bloom || other.bloom);
        }
    }

    private static final class Visitor implements FilterPredicate.Visitor<Usefulness> {

        private final List<ColumnChunkMetaData> columnsList;
        private final Map<ColumnPath, ColumnChunkMetaData> columns;

        Visitor(BlockMetaData rowGroup) {
            this.columnsList = rowGroup.getColumns();
            this.columns = new HashMap<>();
            for (ColumnChunkMetaData c : columnsList) {
                columns.put(c.getPath(), c);
            }
        }

        private ColumnChunkMetaData column(Operators.Column<?> col) {
            return columns.get(col.getColumnPath());
        }

        /**
         * Returns column statistics suitable for MATCH reasoning on a comparison whose semantics
         * fail on null rows. Same contract as {@code TriviallyPassesChecker.Visitor#nonNullableStats}:
         * populated min/max, observed non-null values, and {@code getNumNulls() == 0}.
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

        /**
         * Whether min/max exist so STATISTICS already reasoned about comparison / {@code NotEq}
         * leaves. When they do, those ops cannot uniquely drop after an AMBIGUOUS stats result.
         */
        private boolean hasMinMax(Operators.Column<?> col) {
            ColumnChunkMetaData chunk = column(col);
            if (chunk == null) {
                return false;
            }
            Statistics<?> stats = chunk.getStatistics();
            return stats != null && stats.isEmpty() == false && stats.hasNonNullValue();
        }

        private boolean miss(FilterPredicate leaf) {
            try {
                return StatisticsFilter.canDrop(leaf, columnsList);
            } catch (RuntimeException e) {
                logger.debug("StatisticsFilter.canDrop failed; treating leaf as not MISS", e);
                return false;
            }
        }

        /**
         * AMBIGUOUS / missing-stats usefulness for one leaf. {@code dictAlways} ops can drop when
         * min/max already exist (equality / set / user-defined). {@code dictIfStatsMissing} ops
         * (comparisons, {@code NotEq}) are useful only when min/max cannot.
         */
        private Usefulness ambiguous(Operators.Column<?> col, boolean dictAlways, boolean dictIfStatsMissing, boolean bloomOp) {
            ColumnChunkMetaData chunk = column(col);
            if (chunk == null) {
                return Usefulness.NONE;
            }
            boolean dictOp = dictAlways || (dictIfStatsMissing && hasMinMax(col) == false);
            boolean dict = dictOp && ParquetFormatReader.dictionaryFilterReadsDictionary(chunk);
            boolean bloom = bloomOp && chunk.getBloomFilterOffset() > 0 && chunk.getBloomFilterLength() > 0;
            return new Usefulness(dict, bloom);
        }

        // ---------- comparison predicates ----------

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Usefulness visit(Operators.Eq<T> eq) {
            if (miss(eq)) {
                return Usefulness.NONE;
            }
            T value = eq.getValue();
            if (value == null) {
                return Usefulness.NONE;
            }
            Statistics stats = nonNullableStats(eq.getColumn());
            if (stats != null && stats.compareMinToValue(value) == 0 && stats.compareMaxToValue(value) == 0) {
                return Usefulness.NONE;
            }
            return ambiguous(eq.getColumn(), true, false, true);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Usefulness visit(Operators.NotEq<T> notEq) {
            if (miss(notEq)) {
                return Usefulness.NONE;
            }
            T value = notEq.getValue();
            if (value == null) {
                return Usefulness.NONE;
            }
            Statistics stats = nonNullableStats(notEq.getColumn());
            if (stats != null && (stats.compareMinToValue(value) > 0 || stats.compareMaxToValue(value) < 0)) {
                return Usefulness.NONE;
            }
            return ambiguous(notEq.getColumn(), false, true, false);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Usefulness visit(Operators.Lt<T> lt) {
            if (miss(lt)) {
                return Usefulness.NONE;
            }
            Statistics stats = nonNullableStats(lt.getColumn());
            if (stats != null && stats.compareMaxToValue(lt.getValue()) < 0) {
                return Usefulness.NONE;
            }
            return ambiguous(lt.getColumn(), false, true, false);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Usefulness visit(Operators.LtEq<T> ltEq) {
            if (miss(ltEq)) {
                return Usefulness.NONE;
            }
            Statistics stats = nonNullableStats(ltEq.getColumn());
            if (stats != null && stats.compareMaxToValue(ltEq.getValue()) <= 0) {
                return Usefulness.NONE;
            }
            return ambiguous(ltEq.getColumn(), false, true, false);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Usefulness visit(Operators.Gt<T> gt) {
            if (miss(gt)) {
                return Usefulness.NONE;
            }
            Statistics stats = nonNullableStats(gt.getColumn());
            if (stats != null && stats.compareMinToValue(gt.getValue()) > 0) {
                return Usefulness.NONE;
            }
            return ambiguous(gt.getColumn(), false, true, false);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Usefulness visit(Operators.GtEq<T> gtEq) {
            if (miss(gtEq)) {
                return Usefulness.NONE;
            }
            Statistics stats = nonNullableStats(gtEq.getColumn());
            if (stats != null && stats.compareMinToValue(gtEq.getValue()) >= 0) {
                return Usefulness.NONE;
            }
            return ambiguous(gtEq.getColumn(), false, true, false);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Usefulness visit(Operators.In<T> in) {
            if (miss(in)) {
                return Usefulness.NONE;
            }
            Set<T> values = in.getValues();
            if (containsNull(values)) {
                return Usefulness.NONE;
            }
            if (values != null && values.isEmpty() == false) {
                Statistics stats = nonNullableStats(in.getColumn());
                if (stats != null) {
                    for (T candidate : values) {
                        if (stats.compareMinToValue(candidate) == 0 && stats.compareMaxToValue(candidate) == 0) {
                            return Usefulness.NONE;
                        }
                    }
                }
            }
            return ambiguous(in.getColumn(), true, false, true);
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T extends Comparable<T>> Usefulness visit(Operators.NotIn<T> notIn) {
            if (miss(notIn)) {
                return Usefulness.NONE;
            }
            Set<T> values = notIn.getValues();
            if (containsNull(values)) {
                return Usefulness.NONE;
            }
            if (values != null && values.isEmpty() == false) {
                Statistics stats = nonNullableStats(notIn.getColumn());
                if (stats != null) {
                    boolean match = true;
                    for (T candidate : values) {
                        if (stats.compareMinToValue(candidate) <= 0 && stats.compareMaxToValue(candidate) >= 0) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        return Usefulness.NONE;
                    }
                }
            }
            return ambiguous(notIn.getColumn(), true, false, false);
        }

        /** ES|QL does not push {@code Contains}; neither level is useful. */
        @Override
        public <T extends Comparable<T>> Usefulness visit(Operators.Contains<T> contains) {
            return Usefulness.NONE;
        }

        // ---------- logical predicates ----------

        @Override
        public Usefulness visit(Operators.And and) {
            return and.getLeft().accept(this).or(and.getRight().accept(this));
        }

        @Override
        public Usefulness visit(Operators.Or or) {
            return or.getLeft().accept(this).or(or.getRight().accept(this));
        }

        @Override
        public Usefulness visit(Operators.Not not) {
            return not.getPredicate().accept(this);
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Usefulness visit(Operators.UserDefined<T, U> ud) {
            if (miss(ud)) {
                return Usefulness.NONE;
            }
            return ambiguous(ud.getColumn(), true, false, false);
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Usefulness visit(Operators.LogicalNotUserDefined<T, U> ud) {
            if (miss(ud)) {
                return Usefulness.NONE;
            }
            return ambiguous(ud.getUserDefined().getColumn(), true, false, false);
        }

        private static boolean containsNull(Set<?> values) {
            if (values == null) {
                return false;
            }
            for (Object value : values) {
                if (value == null) {
                    return true;
                }
            }
            return false;
        }
    }
}
