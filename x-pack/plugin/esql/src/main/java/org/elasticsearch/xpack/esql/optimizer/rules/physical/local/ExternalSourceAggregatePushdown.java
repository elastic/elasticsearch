/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.MergedSplitStats;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared helpers for aggregate pushdown rules ({@link PushStatsToExternalSource} and
 * {@link PushAggregatesToExternalSource}) that extract an {@link ExternalSourceExec}
 * from the plan tree and resolve filtered metadata using {@link SplitFilterClassifier}.
 */
public final class ExternalSourceAggregatePushdown {

    private ExternalSourceAggregatePushdown() {}

    /**
     * Whether a column-statistic lookup ({@code COUNT(col)}, {@code MIN}/{@code MAX}) cannot be served
     * from {@code stats} and must safe-miss to a re-scan.
     * <p>
     * The {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats} "implicit nulls" contract makes
     * an absent column key mean "all rows null" — true for footer formats (Parquet/ORC), which emit a stat
     * for every physically present column. Line-oriented text formats harvest per-column stats partially
     * (the {@code count}/{@code projected} scopes leave some present columns un-summarised), so for them an
     * absent key means "not harvested": applying the contract would serve {@code rowCount - rowCount = 0} for
     * {@code COUNT(col)} or a subset extremum for {@code MIN}/{@code MAX} over a column that may be entirely
     * non-null. When the format declares it does not apply implicit nulls
     * ({@code implicitNullsForAbsentColumn == false}, via
     * {@link org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport#appliesImplicitNullsForAbsentColumn()})
     * and the column was not observed ({@code stats.hasColumn(name) == false} —
     * {@link org.elasticsearch.xpack.esql.datasources.MergedSplitStats} requires every child to have observed
     * it), the lookup is unservable. Both {@link PushStatsToExternalSource} and
     * {@link PushAggregatesToExternalSource} gate on this so the invariant lives in one place.
     */
    static boolean columnStatUnservable(
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        String name,
        boolean implicitNullsForAbsentColumn
    ) {
        return implicitNullsForAbsentColumn == false && stats.hasColumn(name) == false;
    }

    /**
     * Returns a cached MIN/MAX extremum if it can be served as {@code type} without loss, else {@code null}
     * (safe-miss). A harvest may legitimately hand a wider Java type than the column's ESQL type — an
     * IN-RANGE {@code Long} for an {@code INTEGER} column narrows exactly and {@code buildBlock} handles it.
     * But any value that is NOT an exact integer in range for an integral column — a fractional or
     * out-of-range {@code Double}, OR a {@code Long} beyond the target's range for an {@code INTEGER} column —
     * would be truncated/overflowed when {@code buildBlock} coerces it via {@code longValue()}/{@code intValue()}
     * (the divergent-inferred-type case where stripes were harvested under a wider type). Rather than serve
     * overflow garbage, safe-miss so a full scan answers. The integral set mirrors {@code buildBlock}'s
     * {@code intValue()}/{@code longValue()} coercion targets (its consumer), not the cache's harvest-time
     * coercion — each layer guards against its own type reference.
     */
    static Object servableExtremum(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case INTEGER -> exactIntegerInRange(value, Integer.MIN_VALUE, Integer.MAX_VALUE) ? value : null;
            case LONG, DATETIME, DATE_NANOS, UNSIGNED_LONG, COUNTER_LONG -> exactIntegerInRange(value, Long.MIN_VALUE, Long.MAX_VALUE)
                ? value
                : null;
            default -> value; // DOUBLE / KEYWORD / BOOLEAN / IP etc. — buildBlock coerces without integral truncation
        };
    }

    /** True iff {@code value} is an exact integer in {@code [min, max]}; false for fractional, out-of-range, or non-numeric. */
    private static boolean exactIntegerInRange(Object value, long min, long max) {
        if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (Double.isFinite(d) == false) {
                return false;
            }
            long asLong = (long) d;
            return (double) asLong == d && asLong >= min && asLong <= max;
        }
        if (value instanceof Number n) {
            long l = n.longValue(); // Long / Integer / Short / Byte round-trip exactly through longValue()
            return l >= min && l <= max;
        }
        return false;
    }

    /**
     * Resolves one aggregate function's value from split statistics, or {@code null} to safe-miss
     * (the aggregate re-scans). This is the ONE resolution used by both pushdown rules — the two used
     * to carry near-verbatim copies, which had already drifted: one guarded against virtual columns
     * ({@code _file.*} metadata, absent from column stats), the other did not. A virtual column
     * reaching the footer-format implicit-nulls contract would serve {@code COUNT(col) = rowCount -
     * rowCount = 0} — wrong data — so the shared resolution carries the union of both rules' guards.
     */
    static Object resolveFromStats(
        Expression aggFunction,
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        boolean implicitNullsForAbsentColumn
    ) {
        if (aggFunction instanceof Count count) {
            return resolveCount(count, stats, implicitNullsForAbsentColumn);
        } else if (aggFunction instanceof Min min) {
            return resolveMin(min, stats, implicitNullsForAbsentColumn);
        } else if (aggFunction instanceof Max max) {
            return resolveMax(max, stats, implicitNullsForAbsentColumn);
        }
        return null;
    }

    /**
     * Resolves {@code COUNT(*)} as the row count and {@code COUNT(col)} preferentially from the harvested
     * per-column value count (multivalue-correct: an NDJSON array {@code [a,b,c]} contributes 3), falling
     * back to {@code rowCount - columnNullCount} for footer formats that don't harvest a value count (their
     * columns are single-valued, and the {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats}
     * "implicit nulls" contract makes the subtraction exact across UNION_BY_NAME mixes). A return of
     * {@code -1} from {@code columnNullCount} signals the rare present-but-stats-less case: bail out.
     */
    private static Object resolveCount(
        Count count,
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        boolean implicitNullsForAbsentColumn
    ) {
        if (count.hasFilter()) {
            return null;
        }
        Expression target = count.field();
        if (target.foldable()) {
            return stats.rowCount();
        }
        // Virtual columns ({@code _file.*}) are not present in the split's column stats; under the footer
        // implicit-nulls contract an absent column reads as all-null, which would serve COUNT(col) = 0.
        // Refuse here even if a format-level gate happens to let one through (defense in depth).
        if (target instanceof Attribute ref && PushdownPredicates.isVirtualColumn(ref) == false) {
            // For text formats under partial harvest an unobserved column means "not harvested," not
            // "all-null": serving rowCount - rowCount = 0 would be wrong. Safe-miss so the engine re-scans.
            if (columnStatUnservable(stats, ref.name(), implicitNullsForAbsentColumn)) {
                return null;
            }
            long vc = stats.columnValueCount(ref.name());
            if (vc >= 0) {
                return vc;
            }
            long nc = stats.columnNullCount(ref.name());
            if (nc >= 0) {
                return stats.rowCount() - nc;
            }
        }
        return null;
    }

    private static Object resolveMin(
        Min min,
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        boolean implicitNullsForAbsentColumn
    ) {
        if (min.hasFilter()) {
            return null;
        }
        if (min.field() instanceof Attribute ref && PushdownPredicates.isVirtualColumn(ref) == false) {
            // A partially-harvested column would serve a subset extremum (one file's range while a
            // sibling's is invisible). Safe-miss; MergedSplitStats requires every child to have observed
            // the column for hasColumn to be true.
            if (columnStatUnservable(stats, ref.name(), implicitNullsForAbsentColumn)) {
                return null;
            }
            return servableExtremum(stats.columnMin(ref.name()), ref.dataType());
        }
        return null;
    }

    private static Object resolveMax(
        Max max,
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        boolean implicitNullsForAbsentColumn
    ) {
        if (max.hasFilter()) {
            return null;
        }
        if (max.field() instanceof Attribute ref && PushdownPredicates.isVirtualColumn(ref) == false) {
            if (columnStatUnservable(stats, ref.name(), implicitNullsForAbsentColumn)) {
                return null;
            }
            return servableExtremum(stats.columnMax(ref.name()), ref.dataType());
        }
        return null;
    }

    /** One constant block per resolved value — the FINAL-mode substitution shape. */
    static Block[] buildFinalBlocks(List<Object> values, List<DataType> dataTypes) {
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[values.size()];
        for (int i = 0; i < values.size(); i++) {
            blocks[i] = buildBlock(blockFactory, values.get(i), dataTypes.get(i));
        }
        return blocks;
    }

    /** Value + seen-flag block pairs — the INITIAL/intermediate-mode substitution shape. */
    static Block[] buildIntermediateBlocks(List<Object> values, List<DataType> dataTypes) {
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[values.size() * 2];
        for (int i = 0; i < values.size(); i++) {
            blocks[i * 2] = buildBlock(blockFactory, values.get(i), dataTypes.get(i));
            blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
        }
        return blocks;
    }

    /**
     * Builds a single-value constant block, coercing the stat value to match the expected ESQL data
     * type. Format readers may return stats in wider Java types than the column's ESQL type; the
     * integral coercions here are exactly what {@link #servableExtremum} guards against lossy inputs.
     */
    static Block buildBlock(BlockFactory blockFactory, Object value, DataType dataType) {
        if (value == null) {
            return blockFactory.newConstantNullBlock(1);
        }
        return switch (dataType) {
            case INTEGER -> blockFactory.newConstantIntBlockWith(((Number) value).intValue(), 1);
            case LONG, COUNTER_LONG, DATETIME -> blockFactory.newConstantLongBlockWith(((Number) value).longValue(), 1);
            case DOUBLE, COUNTER_DOUBLE -> blockFactory.newConstantDoubleBlockWith(((Number) value).doubleValue(), 1);
            case BOOLEAN -> blockFactory.newConstantBooleanBlockWith(
                value instanceof Boolean b ? b : Booleans.parseBoolean(value.toString()),
                1
            );
            // IP is harvested as its 16-byte InetAddressPoint encoding (ColumnStatsAccumulator maps
            // KEYWORD/TEXT/IP -> T_BYTESREF, whose byte-lex order matches IP address order), which is exactly
            // the representation an ES|QL IP block holds, so it round-trips through a constant BytesRef block.
            case KEYWORD, TEXT, IP -> blockFactory.newConstantBytesRefBlockWith(toBytesRef(value), 1);
            default -> {
                if (value instanceof Number n) {
                    yield blockFactory.newConstantLongBlockWith(n.longValue(), 1);
                }
                yield blockFactory.newConstantNullBlock(1);
            }
        };
    }

    private static BytesRef toBytesRef(Object value) {
        if (value instanceof BytesRef br) {
            return br;
        }
        if (value instanceof byte[] bytes) {
            return new BytesRef(bytes);
        }
        return new BytesRef(value.toString());
    }

    /**
     * Parsed result from the subtree below an {@code AggregateExec}: the external source,
     * any alias mapping from intermediate {@code EvalExec}/{@code ProjectExec} nodes, and
     * the filter condition from any intermediate {@code FilterExec}.
     */
    record ExternalSourceInfo(ExternalSourceExec externalExec, AttributeMap<Attribute> aliasReplacedBy, Expression filterCondition) {}

    /**
     * Light-weight projection of {@link #extractExternalSource(PhysicalPlan)} that returns just the
     * {@link ExternalSourceExec} (or {@code null}) for callers that don't need the alias map or filter
     * condition. Cross-package callers (the planner, other optimizer rules) use this so they share the
     * same set of recognized wrapper shapes — adding a new shape here automatically propagates.
     */
    public static ExternalSourceExec findExternalSource(PhysicalPlan child) {
        ExternalSourceInfo info = extractExternalSource(child);
        return info == null ? null : info.externalExec();
    }

    /**
     * Extracts the ExternalSourceExec and optional filter/alias information from the plan
     * subtree below an AggregateExec. Supports these patterns:
     * <ul>
     *   <li>{@code ExternalSourceExec}</li>
     *   <li>{@code EvalExec -> ExternalSourceExec}</li>
     *   <li>{@code ProjectExec -> ExternalSourceExec}</li>
     *   <li>{@code FilterExec -> ExternalSourceExec}</li>
     *   <li>{@code FilterExec -> EvalExec -> ExternalSourceExec}</li>
     *   <li>{@code FilterExec -> ProjectExec -> ExternalSourceExec}</li>
     * </ul>
     * Returns null if the subtree doesn't match any recognized pattern.
     */
    static ExternalSourceInfo extractExternalSource(PhysicalPlan child) {
        if (child instanceof ExternalSourceExec ext) {
            if (ext.pushedFilter() != null) {
                return null;
            }
            return new ExternalSourceInfo(ext, AttributeMap.emptyAttributeMap(), null);
        }
        if (child instanceof EvalExec evalExec && evalExec.child() instanceof ExternalSourceExec ext) {
            if (ext.pushedFilter() != null) {
                return null;
            }
            return new ExternalSourceInfo(ext, PushFiltersToSource.getAliasReplacedBy(evalExec), null);
        }
        if (child instanceof ProjectExec projectExec && projectExec.child() instanceof ExternalSourceExec ext) {
            if (ext.pushedFilter() != null) {
                return null;
            }
            return new ExternalSourceInfo(ext, PushFiltersToSource.getAliasReplacedBy(projectExec), null);
        }
        if (child instanceof FilterExec filterExec) {
            PhysicalPlan filterChild = filterExec.child();
            if (filterChild instanceof ExternalSourceExec ext) {
                return new ExternalSourceInfo(ext, AttributeMap.emptyAttributeMap(), filterExec.condition());
            }
            if (filterChild instanceof EvalExec evalExec && evalExec.child() instanceof ExternalSourceExec ext) {
                return new ExternalSourceInfo(ext, PushFiltersToSource.getAliasReplacedBy(evalExec), filterExec.condition());
            }
            if (filterChild instanceof ProjectExec projectExec && projectExec.child() instanceof ExternalSourceExec ext) {
                return new ExternalSourceInfo(ext, PushFiltersToSource.getAliasReplacedBy(projectExec), filterExec.condition());
            }
        }
        return null;
    }

    /**
     * Resolves effective stats for splits filtered by the given condition. Evaluates
     * the filter against per-split statistics, classifying each split as MATCH, MISS, or
     * AMBIGUOUS. Returns merged statistics from MATCH-only splits, or null if any split
     * is AMBIGUOUS or classification fails.
     * <p>
     * When a single split is present and has its own statistics, those are preferred over
     * file-level metadata to avoid misclassification when split stats differ from the whole.
     * <p>
     * Uses {@link ExternalSplit#splitStats()} on each split, which handles both
     * {@link org.elasticsearch.xpack.esql.datasources.FileSplit} and
     * {@link org.elasticsearch.xpack.esql.datasources.CoalescedSplit} transparently.
     */
    static org.elasticsearch.xpack.esql.datasources.spi.SplitStats resolveFilteredStats(
        ExternalSourceExec externalExec,
        Expression filterCondition,
        boolean implicitNullsForAbsentColumn
    ) {
        List<? extends ExternalSplit> splits = externalExec.splits();

        if (splits.isEmpty() || splits.size() == 1) {
            org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats = null;
            if (splits.size() == 1) {
                stats = splits.getFirst().splitStats();
            }
            if (stats == null) {
                stats = SplitStats.of(externalExec.sourceMetadata());
            }
            if (stats == null) {
                return null;
            }
            SplitFilterClassifier.SplitMatch result = SplitFilterClassifier.classifyExpression(
                filterCondition,
                stats,
                implicitNullsForAbsentColumn
            );
            return switch (result) {
                case MATCH -> stats;
                case MISS -> SplitStats.EMPTY;
                case AMBIGUOUS -> null;
            };
        }

        List<ExternalSplit> flatSplits = CoalescedSplit.flatten(splits);
        List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> matchedStats = new ArrayList<>();
        for (ExternalSplit split : flatSplits) {
            org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats = split.splitStats();
            if (stats == null) {
                return null;
            }
            SplitFilterClassifier.SplitMatch result = SplitFilterClassifier.classifyExpression(
                filterCondition,
                stats,
                implicitNullsForAbsentColumn
            );
            switch (result) {
                case MATCH -> matchedStats.add(stats);
                case MISS -> {
                }
                case AMBIGUOUS -> {
                    return null;
                }
            }
        }

        if (matchedStats.isEmpty()) {
            return SplitStats.EMPTY;
        }
        return new MergedSplitStats(matchedStats);
    }
}
