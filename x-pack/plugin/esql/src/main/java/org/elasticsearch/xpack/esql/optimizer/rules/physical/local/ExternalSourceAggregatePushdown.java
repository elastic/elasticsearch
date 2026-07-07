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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.ColumnStatTypeSupport;
import org.elasticsearch.xpack.esql.datasources.MergedSplitStats;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared helpers for the aggregate pushdown rule ({@link PushStatsToExternalSource}) and the
 * split-discovery gate ({@code ComputeService.canSkipForAggregateOverExternal}) that extract an
 * {@link ExternalSourceExec} from the plan tree and resolve filtered metadata using
 * {@link SplitFilterClassifier}.
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
     * it), the lookup is unservable. Both the fold ({@link PushStatsToExternalSource}) and the
     * split-discovery gate (via {@link #canServeAllFromStats}) go through {@link #resolveFromStats}, which
     * gates on this so the invariant lives in one place.
     */
    static boolean columnStatUnservable(
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        String name,
        boolean implicitNullsForAbsentColumn
    ) {
        return implicitNullsForAbsentColumn == false && stats.hasColumn(name) == false;
    }

    /**
     * Unwraps the {@link AggregateFunction} operands of an ungrouped aggregate's output, dropping any
     * non-aggregate expressions (e.g. bare literals). This is the input every format's
     * {@link org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport#canPushAggregates}
     * type-gate consumes — shared by {@link PushStatsToExternalSource} (fold) and
     * {@code ComputeService.canSkipForAggregateOverExternal} (split-discovery gate) so both feed the
     * gate the same function list.
     */
    public static List<Expression> extractAggregateFunctions(List<? extends Expression> aggregates) {
        List<Expression> result = new ArrayList<>(aggregates.size());
        for (Expression agg : aggregates) {
            Expression toCheck = agg instanceof Alias alias ? alias.child() : agg;
            if (toCheck instanceof AggregateFunction) {
                result.add(toCheck);
            }
        }
        return result;
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
        ColumnStatTypeSupport support = ColumnStatTypeSupport.of(type);
        if (support == null || support.blockKind() == null) {
            // A null support (or a support with no block kind — e.g. VERSION) has NO buildBlock arm: serving it
            // would hit buildBlock's throwing default, a crash on an otherwise-valid query (e.g. MIN(version),
            // whose pushdown gate has no type filter). Safe-miss instead so the aggregate re-scans and
            // answers correctly. INVARIANT: this servable set MUST equal buildBlock's arm set (both dispatch on
            // ColumnStatTypeSupport.of, so they cannot drift).
            return null;
        }
        return switch (support.blockKind()) {
            case INT -> exactIntegerInRange(value, Integer.MIN_VALUE, Integer.MAX_VALUE) ? value : null;
            case LONG -> exactIntegerInRange(value, Long.MIN_VALUE, Long.MAX_VALUE) ? value : null;
            // The non-integral buildBlock arms coerce without integral truncation, so these serve as-is.
            case DOUBLE, BOOLEAN, BYTES_REF -> value;
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
     * Whether EVERY aggregate in {@code aggregates} would resolve from {@code stats} — the boolean twin of
     * {@link PushStatsToExternalSource}'s value-collecting loop. The split-discovery gate
     * ({@code ComputeService.canSkipSplitDiscovery}) must consult THIS check, not just the type-level
     * {@link org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport#canPushAggregates}: the two
     * previously diverged on per-column servability, so the gate would skip discovery (leaving a zero-split
     * scan) for an aggregate the fold then safe-missed, and the scan's un-pruned union_by_name mapping tripped
     * {@code SchemaAdaptingIterator}'s width guard (the union_by_name zero-split servability guard). Sharing {@code resolveFromStats}
     * with the fold guarantees "gate skips" implies "fold serves". The bail conditions here must stay identical
     * to {@link PushStatsToExternalSource}'s value loop.
     */
    public static boolean canServeAllFromStats(
        List<? extends NamedExpression> aggregates,
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        boolean implicitNullsForAbsentColumn,
        Set<String> pathDerivedColumns
    ) {
        for (NamedExpression agg : aggregates) {
            if (agg instanceof Alias == false) {
                return false;
            }
            if (resolveFromStats(((Alias) agg).child(), stats, implicitNullsForAbsentColumn, pathDerivedColumns) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * The columns whose values are derived from the file's directory PATH (Hive-style partition keys), not its
     * payload. They are absent from every file's column stats, so the implicit-nulls contract reads them as
     * all-null — any {@code COUNT} over one would serve 0. Both the fold and the split-discovery gate feed this
     * set to {@link #resolveFromStats} so a partition-column aggregate safe-misses on footer formats.
     * <p>
     * Read from the SERIALIZED {@code sourceMetadata} (stamped at resolution — see
     * {@code SourceStatisticsSerializer#PARTITION_COLUMNS_KEY}), NOT the {@code FileList}: the fileList that carries
     * {@code PartitionMetadata} is coordinator-only and deserializes to {@code UNRESOLVED} on a data node, so the
     * data-node fold would otherwise see an empty set and fold {@code COUNT(partition_col)} to 0. Empty when the
     * source is not partitioned (no {@code hive_partitioning}).
     */
    @SuppressWarnings("unchecked")
    public static Set<String> partitionColumnNames(Map<String, Object> sourceMetadata) {
        if (sourceMetadata == null) {
            return Set.of();
        }
        Object names = sourceMetadata.get(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY);
        if (names instanceof Collection<?> collection && collection.isEmpty() == false) {
            return Set.copyOf((Collection<String>) collection);
        }
        return Set.of();
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
        boolean implicitNullsForAbsentColumn,
        Set<String> pathDerivedColumns
    ) {
        if (aggFunction instanceof Count count) {
            return resolveCount(count, stats, implicitNullsForAbsentColumn, pathDerivedColumns);
        } else if (aggFunction instanceof Min min) {
            return resolveMin(min, stats, implicitNullsForAbsentColumn, pathDerivedColumns);
        } else if (aggFunction instanceof Max max) {
            return resolveMax(max, stats, implicitNullsForAbsentColumn, pathDerivedColumns);
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
        boolean implicitNullsForAbsentColumn,
        Set<String> pathDerivedColumns
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
            // Partition columns live in the directory path, not the file payload, so they are absent from every
            // file's column stats -> columnNullCount returns rowCount (implicit-nulls contract) -> COUNT(p) would
            // serve rowCount - rowCount = 0. Safe-miss so the scan's VirtualColumnIterator answers correctly.
            if (pathDerivedColumns.contains(ref.name())) {
                return null;
            }
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
        boolean implicitNullsForAbsentColumn,
        Set<String> pathDerivedColumns
    ) {
        if (min.hasFilter()) {
            return null;
        }
        if (min.field() instanceof Attribute ref && PushdownPredicates.isVirtualColumn(ref) == false) {
            // MIN/MAX of a partition column already safe-miss (columnMin returns null for an absent column), but
            // guard explicitly so a future stats producer that starts emitting a partition-named column can't
            // serve a path-derived value from payload stats.
            if (pathDerivedColumns.contains(ref.name())) {
                return null;
            }
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
        boolean implicitNullsForAbsentColumn,
        Set<String> pathDerivedColumns
    ) {
        if (max.hasFilter()) {
            return null;
        }
        if (max.field() instanceof Attribute ref && PushdownPredicates.isVirtualColumn(ref) == false) {
            if (pathDerivedColumns.contains(ref.name())) {
                return null;
            }
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
        ColumnStatTypeSupport support = ColumnStatTypeSupport.of(dataType);
        if (support == null || support.blockKind() == null) {
            // Fail loud rather than serve a silent NULL/long coercion for an unhandled type: every harvestable
            // type (TextAggregatePushdownSupport) must resolve to a StatBlockKind. A silent default is how
            // MIN/MAX(ip) once served NULL. servableExtremum gates on the same ColumnStatTypeSupport.of, so a
            // type reaching here without a block kind is a genuine invariant break.
            throw new IllegalStateException("buildBlock has no arm for pushed aggregate type [" + dataType + "]");
        }
        // Exhaustive over StatBlockKind (no default): adding a new StatBlockKind without a buildBlock arm is a
        // compile error, guaranteeing every servable type materializes into a block.
        return switch (support.blockKind()) {
            case INT -> blockFactory.newConstantIntBlockWith(((Number) value).intValue(), 1);
            // DATE_NANOS, like DATETIME, is a nanos/millis-since-epoch long — served as a constant long block;
            // the coordinator renders it per the column's resolved type.
            case LONG -> blockFactory.newConstantLongBlockWith(((Number) value).longValue(), 1);
            case DOUBLE -> blockFactory.newConstantDoubleBlockWith(((Number) value).doubleValue(), 1);
            case BOOLEAN -> blockFactory.newConstantBooleanBlockWith(
                value instanceof Boolean b ? b : Booleans.parseBoolean(value.toString()),
                1
            );
            // IP is harvested as its 16-byte InetAddressPoint encoding (ColumnStatsAccumulator maps
            // KEYWORD/TEXT/IP -> T_BYTESREF, whose byte-lex order matches IP address order), which is exactly
            // the representation an ES|QL IP block holds, so it round-trips through a constant BytesRef block.
            case BYTES_REF -> blockFactory.newConstantBytesRefBlockWith(toBytesRef(value), 1);
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
                // Fall back to the whole-file cache stats — but honor STATS_PARTIAL exactly as the unfiltered
                // path does (SplitStats.resolveEffectiveStats): a partial whole-file row_count (e.g. a single
                // CoalescedSplit whose stat-less child collapsed splitStats() to null) cannot answer a FILTERED
                // count. Serving it would emit a partial COUNT. Safe-miss instead.
                Map<String, Object> sourceMetadata = externalExec.sourceMetadata();
                if (sourceMetadata != null && Boolean.TRUE.equals(sourceMetadata.get(SourceStatisticsSerializer.STATS_PARTIAL))) {
                    return null;
                }
                stats = externalExec.sourceMetadataStats();
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
            // The classifier compares the filter literal against the split's stats. Those stats are normalized
            // to the reconciled query type at split construction (FileSplitProvider), so the compare is in one
            // unit -- no reconciliation is needed or done here.
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
        // Pure value-fold: matched splits' stats are already normalized to the reconciled type upstream.
        return new MergedSplitStats(matchedStats);
    }
}
