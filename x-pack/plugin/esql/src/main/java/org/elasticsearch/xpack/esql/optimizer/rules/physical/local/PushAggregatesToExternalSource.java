/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalAggregatePushdownExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces {@code AggregateExec → ExternalSourceExec} with a more efficient plan when
 * ungrouped aggregates ({@code COUNT(*)}, {@code COUNT(field)}, {@code MIN(field)},
 * {@code MAX(field)}) can be answered without a full data scan. Two paths:
 * <ul>
 *   <li><b>Planning-time:</b> when merged file-level statistics fully cover every
 *       aggregate, replace the plan with {@link LocalSourceExec} containing constant
 *       blocks. Supports both SINGLE and INITIAL modes (intermediate-format blocks for
 *       INITIAL: typed value block + {@code seen} boolean block per aggregate).</li>
 *   <li><b>Runtime:</b> when stats are partially missing (e.g. multi-file glob), and the
 *       format reader implements {@link AggregateScanReader}, emit
 *       {@link ExternalAggregatePushdownExec}. The runtime operator iterates per-row-group
 *       intermediate pages from the reader, which decides per row group whether to derive
 *       results from stats (fast path) or by scanning row data (slow path). INITIAL mode
 *       only — SINGLE has no FINAL reducer to combine the per-row-group pages.</li>
 * </ul>
 * Filter pushdown does not compose: when a filter has been pushed onto the reader,
 * {@code aggregatePushdownSupport().canPushAggregates(...)} reports non-pushable, so neither
 * path activates.
 * <p>
 * FINAL mode is never pushed because the rule matches {@code AggregateExec → ExternalSourceExec}
 * and a FINAL aggregate's child is always another aggregate or exchange, never an external source.
 */
public class PushAggregatesToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        if (aggregateExec.child() instanceof ExternalSourceExec == false) {
            return aggregateExec;
        }
        ExternalSourceExec externalExec = (ExternalSourceExec) aggregateExec.child();

        AggregatorMode mode = aggregateExec.getMode();
        if (mode != AggregatorMode.SINGLE && mode != AggregatorMode.INITIAL) {
            return aggregateExec;
        }

        if (aggregateExec.groupings().isEmpty() == false) {
            return aggregateExec;
        }

        FormatReaderRegistry formatReaderRegistry = ctx != null ? ctx.formatReaderRegistry() : null;
        if (formatReaderRegistry == null) {
            return aggregateExec;
        }
        FormatReader formatReader = formatReaderRegistry.findByName(externalExec.sourceType());
        if (formatReader == null || formatReader.aggregatePushdownSupport() == AggregatePushdownSupport.UNSUPPORTED) {
            return aggregateExec;
        }

        List<Expression> aggFunctions = extractAggregateFunctions(aggregateExec.aggregates());
        if (aggFunctions.isEmpty()) {
            return aggregateExec;
        }
        AggregatePushdownSupport.Pushability pushability = formatReader.aggregatePushdownSupport()
            .canPushAggregates(aggFunctions, List.of());
        if (pushability != AggregatePushdownSupport.Pushability.YES) {
            return aggregateExec;
        }

        var stats = SplitStats.resolveEffectiveStats(externalExec.splits(), externalExec.sourceMetadata());
        if (stats == null) {
            // Planning-time pushdown is unavailable (e.g. multi-file glob where stats would
            // need to be read serially). Try runtime aggregate pushdown: if the reader
            // implements AggregateScanReader, it can emit intermediate-state pages directly
            // from each split (per-row-group fast path on stats; slow path scans row data).
            // Otherwise, preserve the original plan so the data remains correct (full scan).
            PhysicalPlan runtime = tryRuntimeAggregatePushdown(aggregateExec, externalExec, formatReader);
            return runtime != null ? runtime : aggregateExec;
        }
        List<Object> values = new ArrayList<>(aggregateExec.aggregates().size());
        List<DataType> dataTypes = new ArrayList<>(aggregateExec.aggregates().size());
        if (resolveAggregateValues(aggregateExec.aggregates(), stats, values, dataTypes) == false) {
            return aggregateExec;
        }

        List<Attribute> outputAttrs;
        Block[] blocks;
        if (mode == AggregatorMode.SINGLE) {
            outputAttrs = new ArrayList<>(aggregateExec.aggregates().size());
            for (NamedExpression agg : aggregateExec.aggregates()) {
                outputAttrs.add(agg.toAttribute());
            }
            blocks = buildFinalBlocks(values, dataTypes);
        } else {
            outputAttrs = aggregateExec.intermediateAttributes();
            blocks = buildIntermediateBlocks(values, dataTypes);
        }

        return new LocalSourceExec(aggregateExec.source(), outputAttrs, LocalSupplier.of(new Page(blocks)));
    }

    /**
     * Attempt to replace the {@code AggregateExec → ExternalSourceExec} pair with an
     * {@link ExternalAggregatePushdownExec} that aggregates per-file row groups (stats fast
     * path or row-data slow path, chosen by the reader) in parallel at execution time.
     * Returns {@code null} if the runtime path is not applicable, in which case the caller
     * preserves the original plan.
     */
    private PhysicalPlan tryRuntimeAggregatePushdown(
        AggregateExec aggregateExec,
        ExternalSourceExec externalExec,
        FormatReader formatReader
    ) {
        if (formatReader instanceof AggregateScanReader == false) {
            return null;
        }
        // Requires the reducer above to combine per-row-group intermediate pages into a
        // single row. SINGLE-mode aggregates have no parent reducer, so emitting N
        // intermediate pages would produce N rows instead of 1. INITIAL-mode is always
        // followed by a FINAL reducer.
        if (aggregateExec.getMode() != AggregatorMode.INITIAL) {
            return null;
        }
        if (externalExec.splits().isEmpty()) {
            return null;
        }
        // Verify every aggregate is one of Count/Min/Max with a simple Attribute (or foldable
        // in the case of COUNT(*)). Anything else falls back to a normal scan.
        for (NamedExpression agg : aggregateExec.aggregates()) {
            if (agg instanceof Alias alias) {
                if (isSupportedAggregateShape(alias.child()) == false) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return new ExternalAggregatePushdownExec(
            aggregateExec.source(),
            externalExec.sourcePath(),
            externalExec.sourceType(),
            externalExec.config(),
            externalExec.splits(),
            new ArrayList<>(aggregateExec.aggregates()),
            aggregateExec.intermediateAttributes()
        );
    }

    /**
     * Whether a single aggregate child expression is one the runtime aggregate-scan operator
     * understands: {@code COUNT(*)}, {@code COUNT(field)}, {@code MIN(field)}, or
     * {@code MAX(field)} where {@code field} is a plain {@link Attribute} reference, with no
     * runtime filter ({@code .. WHERE ..}). Anything else falls back to a normal scan.
     */
    private static boolean isSupportedAggregateShape(Expression aggFunction) {
        if (aggFunction instanceof Count count) {
            if (count.hasFilter()) {
                return false;
            }
            Expression target = count.field();
            return target.foldable() || target instanceof Attribute;
        }
        if (aggFunction instanceof Min min) {
            return min.hasFilter() == false && min.field() instanceof Attribute;
        }
        if (aggFunction instanceof Max max) {
            return max.hasFilter() == false && max.field() instanceof Attribute;
        }
        // Sum unsupported because Parquet metadata doesn't carry sums.
        return false;
    }

    private boolean resolveAggregateValues(
        List<? extends NamedExpression> aggregates,
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        List<Object> values,
        List<DataType> dataTypes
    ) {
        for (int i = 0; i < aggregates.size(); i++) {
            NamedExpression agg = aggregates.get(i);
            if (agg instanceof Alias == false) {
                return false;
            }
            Expression child = ((Alias) agg).child();
            Object value = resolveFromStats(child, stats);
            if (value == null) {
                return false;
            }
            values.add(value);
            dataTypes.add(child instanceof AggregateFunction af ? af.dataType() : DataType.LONG);
        }
        return true;
    }

    /**
     * Resolves a single aggregate function from per-split statistics. Returns the constant
     * value the aggregate would produce, or {@code null} when stats are insufficient (the
     * caller then falls back to a normal scan).
     * <p>
     * <b>COUNT(field) on multi-valued columns:</b> this returns {@code rowCount - nullCount}
     * which gives "rows with at least one non-null value", not "total non-null values" as the
     * standard ESQL scan path computes (see {@link org.elasticsearch.compute.aggregation.CountAggregatorFunction}).
     * The runtime aggregate-pushdown path mirrors the same formula. Tracked separately so a
     * future fix can correct both call sites consistently using
     * {@code ColumnChunkMetaData.getValueCount() - getNumNulls()} for Parquet (and the
     * equivalent metadata field for other formats); this requires plumbing a {@code valueCount}
     * field through {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats}
     * and {@link org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics}.
     */
    private Object resolveFromStats(Expression aggFunction, org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats) {
        if (aggFunction instanceof Count count) {
            if (count.hasFilter()) {
                return null;
            }
            Expression target = count.field();
            if (target.foldable()) {
                return stats.rowCount();
            }
            if (target instanceof Attribute ref) {
                long nc = stats.columnNullCount(ref.name());
                if (nc >= 0) {
                    return stats.rowCount() - nc;
                }
            }
            return null;
        } else if (aggFunction instanceof Min min) {
            if (min.hasFilter()) {
                return null;
            }
            if (min.field() instanceof Attribute ref) {
                return stats.columnMin(ref.name());
            }
            return null;
        } else if (aggFunction instanceof Max max) {
            if (max.hasFilter()) {
                return null;
            }
            if (max.field() instanceof Attribute ref) {
                return stats.columnMax(ref.name());
            }
            return null;
        }
        return null;
    }

    private static Block[] buildFinalBlocks(List<Object> values, List<DataType> dataTypes) {
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[values.size()];
        for (int i = 0; i < values.size(); i++) {
            blocks[i] = buildBlock(blockFactory, values.get(i), dataTypes.get(i));
        }
        return blocks;
    }

    private static Block[] buildIntermediateBlocks(List<Object> values, List<DataType> dataTypes) {
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[values.size() * 2];
        for (int i = 0; i < values.size(); i++) {
            blocks[i * 2] = buildBlock(blockFactory, values.get(i), dataTypes.get(i));
            blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
        }
        return blocks;
    }

    /**
     * Builds a single-value constant block, coercing the stat value to match the expected ESQL
     * data type. Format readers may return stats in wider Java types than the column's ESQL type
     * (e.g. ORC returns {@code long} for all integer stats including INT32 columns).
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
            case KEYWORD, TEXT -> blockFactory.newConstantBytesRefBlockWith(new BytesRef(value.toString()), 1);
            default -> {
                if (value instanceof Number n) {
                    yield blockFactory.newConstantLongBlockWith(n.longValue(), 1);
                }
                yield blockFactory.newConstantNullBlock(1);
            }
        };
    }

    private List<Expression> extractAggregateFunctions(List<? extends NamedExpression> aggregates) {
        List<Expression> result = new ArrayList<>();
        for (int i = 0; i < aggregates.size(); i++) {
            NamedExpression agg = aggregates.get(i);
            Expression toCheck = agg;
            if (agg instanceof Alias alias) {
                toCheck = alias.child();
            }
            if (toCheck instanceof AggregateFunction == false) {
                continue;
            }
            result.add(toCheck);
        }
        return result;
    }
}
