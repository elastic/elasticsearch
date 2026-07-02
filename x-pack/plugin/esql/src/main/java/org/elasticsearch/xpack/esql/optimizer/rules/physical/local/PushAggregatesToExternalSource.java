/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces {@code AggregateExec → ExternalSourceExec} with {@code LocalSourceExec}
 * when ungrouped aggregates (COUNT(*), MIN, MAX) can be computed from file-level statistics.
 * <p>
 * Supports both SINGLE and INITIAL modes. In SINGLE mode the replacement produces final-value
 * blocks (one block per aggregate). In INITIAL mode the replacement produces intermediate-format
 * blocks matching {@link AggregateExec#intermediateAttributes()}: for each aggregate, a typed
 * value block followed by a {@code seen} boolean block (all supported aggregates — Count, Min,
 * Max — share this two-channel layout).
 * <p>
 * FINAL mode is never pushed because the rule matches {@code AggregateExec → ExternalSourceExec}
 * and a FINAL aggregate's child is always another aggregate or exchange, never an external source.
 * <p>
 * Statistics come from {@code ExternalSourceExec.sourceMetadata()} for single-split queries, or
 * from merged per-split statistics in {@code FileSplit.splitStats()} for multi-split queries.
 * Falls back to normal execution when any split lacks statistics.
 * <p>
 * Substitution is skipped when the source has pushed scan-time predicates ({@code pushedExpressions}
 * or {@code pushedFilter}), because statistics describe whole splits before those predicates.
 */
public class PushAggregatesToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    private static final Logger logger = LogManager.getLogger(PushAggregatesToExternalSource.class);

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        if (aggregateExec.child() instanceof ExternalSourceExec == false) {
            return aggregateExec;
        }
        ExternalSourceExec externalExec = (ExternalSourceExec) aggregateExec.child();

        if (externalExec.pushedFilter() != null) {
            return aggregateExec;
        }

        AggregatorMode mode = aggregateExec.getMode();
        if (mode != AggregatorMode.SINGLE && mode != AggregatorMode.INITIAL) {
            return aggregateExec;
        }

        if (aggregateExec.groupings().isEmpty() == false) {
            return aggregateExec;
        }

        FormatReaderRegistry formatReaderRegistry = ctx == null || ctx.external() == null ? null : ctx.external().formatReaderRegistry();
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
        if (formatReader.aggregatePushdownSupport()
            .canPushAggregates(aggFunctions, List.of()) != AggregatePushdownSupport.Pushability.YES) {
            return aggregateExec;
        }

        // Row-group / footer statistics describe whole splits before any scan-time predicates. When a
        // reader applies a predicate during read ({@link ExternalSourceExec#pushedExpressions()} /
        // {@link ExternalSourceExec#pushedFilter()} after PushFiltersToSource removes FilterExec),
        // answering COUNT(*) / MIN / MAX purely from statistics would ignore that predicate — wrong counts.
        if (externalExec.pushedExpressions().isEmpty() == false || externalExec.pushedFilter() != null) {
            logger.info(
                () -> Strings.format(
                    "PushAggregatesToExternalSource: skipping stats substitution (source has pushed scan predicates)"
                        + " path=[{}] projections=[{}] type=[{}]",
                    externalExec.sourcePath(),
                    externalExec.pushedExpressions().size(),
                    externalExec.sourceType()
                )
            );
            return aggregateExec;
        }

        var stats = externalExec.effectiveSplitStats();
        if (stats == null) {
            return aggregateExec;
        }
        List<Object> values = new ArrayList<>(aggregateExec.aggregates().size());
        List<DataType> dataTypes = new ArrayList<>(aggregateExec.aggregates().size());
        boolean implicitNullsForAbsentColumn = formatReader.aggregatePushdownSupport().appliesImplicitNullsForAbsentColumn();
        if (resolveAggregateValues(aggregateExec.aggregates(), stats, values, dataTypes, implicitNullsForAbsentColumn) == false) {
            return aggregateExec;
        }

        List<Attribute> outputAttrs;
        Block[] blocks;
        if (mode == AggregatorMode.SINGLE) {
            outputAttrs = new ArrayList<>(aggregateExec.aggregates().size());
            for (NamedExpression agg : aggregateExec.aggregates()) {
                outputAttrs.add(agg.toAttribute());
            }
            blocks = ExternalSourceAggregatePushdown.buildFinalBlocks(values, dataTypes);
        } else {
            outputAttrs = aggregateExec.intermediateAttributes();
            blocks = ExternalSourceAggregatePushdown.buildIntermediateBlocks(values, dataTypes);
        }

        return new LocalSourceExec(aggregateExec.source(), outputAttrs, LocalSupplier.of(new Page(blocks)));
    }

    private boolean resolveAggregateValues(
        List<? extends NamedExpression> aggregates,
        org.elasticsearch.xpack.esql.datasources.spi.SplitStats stats,
        List<Object> values,
        List<DataType> dataTypes,
        boolean implicitNullsForAbsentColumn
    ) {
        for (int i = 0; i < aggregates.size(); i++) {
            NamedExpression agg = aggregates.get(i);
            if (agg instanceof Alias == false) {
                return false;
            }
            Expression child = ((Alias) agg).child();
            Object value = ExternalSourceAggregatePushdown.resolveFromStats(child, stats, implicitNullsForAbsentColumn);
            if (value == null) {
                return false;
            }
            values.add(value);
            dataTypes.add(child instanceof AggregateFunction af ? af.dataType() : DataType.LONG);
        }
        return true;
    }

    /**
     * Resolves the value of an ungrouped aggregate purely from split-level statistics.
     * <p>
     * For {@code COUNT(col)} we use {@code rowCount - columnNullCount(col)}. This relies on the
     * {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats} "implicit nulls" contract:
     * {@code columnNullCount} already includes both explicit nulls in files that contain the
     * column and rows in files that do not contain the column at all (those count as nulls
     * because every row would deserialize as {@code null}). When the column is fully absent
     * from a scope, {@code columnNullCount == rowCount}, so {@code COUNT(col) == 0} for that
     * scope — exactly the right answer for UNION_BY_NAME mixes.
     * <p>
     * The implicit-nulls contract is only sound when an absent column key genuinely means "all-null"
     * — true for footer formats (Parquet/ORC), which emit a stat for every physically present column.
     * Line-oriented text formats harvest per-column stats partially (count / projected scopes leave
     * present columns un-summarised), so for them an absent key means "not harvested," and applying
     * the contract would serve {@code rowCount - rowCount = 0} for a column that may be entirely
     * non-null. When {@code implicitNullsForAbsentColumn} is {@code false} (the format declares it via
     * {@link org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport#appliesImplicitNullsForAbsentColumn()}),
     * a column with no observed stats ({@code stats.hasColumn(name) == false}) safe-misses instead.
     * <p>
     * The other short-circuit is the rare "column present but stats unknown" case, where
     * {@code columnNullCount} returns {@code -1} and we bail out so the engine falls back to a
     * regular scan.
     * <p>
     * For {@code MIN}/{@code MAX} we read {@code columnMin}/{@code columnMax}. Under the SPI's
     * "implicit nulls" contract, {@link org.elasticsearch.xpack.esql.datasources.MergedSplitStats}
     * skips children whose null count equals their row count (absent column or all rows null) and
     * only poisons on a genuine unknown ({@code columnNullCount &lt; 0}). That skip is correct for
     * footer formats, where an absent column is genuinely all-null. For text formats under partial
     * harvest it is not: a contributing split that did not harvest the column is invisible, and
     * skipping it would serve a subset extremum. So, exactly as for {@code COUNT(col)}, when the
     * format does not apply implicit nulls ({@code implicitNullsForAbsentColumn == false}) and the
     * column was not observed by every contributing split ({@code stats.hasColumn(name) == false}),
     * we safe-miss. Otherwise a {@code null} result means either "no child contributed a candidate
     * value" or "incompatible/unknown stats" — both correct fall-back signals; the rule does not
     * pushdown.
     */

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
