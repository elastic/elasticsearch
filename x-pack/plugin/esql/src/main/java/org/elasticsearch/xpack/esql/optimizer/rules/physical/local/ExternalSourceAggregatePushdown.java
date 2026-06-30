/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.MergedSplitStats;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

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
        Expression filterCondition
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
            SplitFilterClassifier.SplitMatch result = SplitFilterClassifier.classifyExpression(filterCondition, stats);
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
            SplitFilterClassifier.SplitMatch result = SplitFilterClassifier.classifyExpression(filterCondition, stats);
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
