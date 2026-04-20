/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Replaces {@code AggregateExec → ExternalSourceExec} with {@code LocalSourceExec}
 * when ungrouped aggregates (COUNT(*), MIN, MAX) can be computed from file-level statistics.
 * <p>
 * Works in both SINGLE mode (coordinator-only) and INITIAL mode (data node in distributed execution).
 * The coordinator's FINAL AggregateExec is never touched — it merges intermediate values from all
 * data nodes regardless of whether each data node pushed down or scanned.
 * <p>
 * Statistics come from {@code ExternalSourceExec.sourceMetadata()} for single-split queries, or
 * from merged per-split statistics in {@code FileSplit.statistics()} for multi-split queries.
 * Falls back to normal execution when any split lacks statistics.
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

        // Phase 1: Ungrouped only
        if (aggregateExec.groupings().isEmpty() == false) {
            return aggregateExec;
        }

        // Check format supports aggregate pushdown
        FormatReaderRegistry formatReaderRegistry = ctx != null ? ctx.formatReaderRegistry() : null;
        if (formatReaderRegistry == null) {
            return aggregateExec;
        }
        FormatReader formatReader = formatReaderRegistry.findByName(externalExec.sourceType());
        if (formatReader == null || formatReader.aggregatePushdownSupport() == AggregatePushdownSupport.UNSUPPORTED) {
            return aggregateExec;
        }

        // Extract aggregate functions and check pushability
        List<Expression> aggFunctions = extractAggregateFunctions(aggregateExec.aggregates());
        if (aggFunctions.isEmpty()) {
            return aggregateExec;
        }
        if (formatReader.aggregatePushdownSupport()
            .canPushAggregates(aggFunctions, List.of()) != AggregatePushdownSupport.Pushability.YES) {
            return aggregateExec;
        }

        Map<String, Object> sourceMetadata = SourceStatisticsSerializer.resolveEffectiveMetadata(
            externalExec.splits(),
            externalExec.sourceMetadata()
        );
        if (sourceMetadata == null) {
            return aggregateExec;
        }
        List<Object> values = resolveAggregateValues(aggregateExec.aggregates(), sourceMetadata);
        if (values == null) {
            return aggregateExec; // Some aggregate couldn't be resolved from metadata
        }

        // Build the output page based on mode
        AggregatorMode mode = aggregateExec.getMode();
        List<Attribute> outputAttrs;
        Block[] blocks;

        if (mode == AggregatorMode.SINGLE) {
            // Final values — same as PushStatsToExternalSource
            outputAttrs = new ArrayList<>(aggregateExec.aggregates().size());
            for (NamedExpression agg : aggregateExec.aggregates()) {
                outputAttrs.add(agg.toAttribute());
            }
            blocks = buildFinalBlocks(values);
        } else {
            // Intermediate format — value + seen boolean for each aggregate
            outputAttrs = aggregateExec.intermediateAttributes();
            blocks = buildIntermediateBlocks(values);
        }

        return new LocalSourceExec(aggregateExec.source(), outputAttrs, LocalSupplier.of(new Page(blocks)));
    }

    /**
     * Resolve aggregate values from sourceMetadata. Returns null if any value can't be resolved.
     */
    private List<Object> resolveAggregateValues(List<? extends NamedExpression> aggregates, Map<String, Object> sourceMetadata) {
        List<Object> values = new ArrayList<>(aggregates.size());
        for (int i = 0; i < aggregates.size(); i++) {
            NamedExpression agg = aggregates.get(i);
            if (agg instanceof Alias == false) {
                return null;
            }
            Expression child = ((Alias) agg).child();
            Object value = resolveFromMetadata(child, sourceMetadata);
            if (value == null) {
                return null;
            }
            values.add(value);
        }
        return values;
    }

    private Object resolveFromMetadata(Expression aggFunction, Map<String, Object> sourceMetadata) {
        if (aggFunction instanceof Count count) {
            if (count.hasFilter()) {
                return null;
            }
            Expression target = count.field();
            if (target.foldable()) {
                return SourceStatisticsSerializer.extractRowCount(sourceMetadata);
            }
            if (target instanceof Attribute ref) {
                Long rc = SourceStatisticsSerializer.extractRowCount(sourceMetadata);
                Long nc = SourceStatisticsSerializer.extractColumnNullCount(sourceMetadata, ref.name());
                if (rc != null && nc != null) {
                    return rc - nc;
                }
            }
            return null;
        } else if (aggFunction instanceof Min min) {
            if (min.hasFilter()) {
                return null;
            }
            if (min.field() instanceof Attribute ref) {
                return SourceStatisticsSerializer.extractColumnMin(sourceMetadata, ref.name());
            }
            return null;
        } else if (aggFunction instanceof Max max) {
            if (max.hasFilter()) {
                return null;
            }
            if (max.field() instanceof Attribute ref) {
                return SourceStatisticsSerializer.extractColumnMax(sourceMetadata, ref.name());
            }
            return null;
        }
        return null;
    }

    /**
     * Build blocks for SINGLE mode (final values).
     */
    private static Block[] buildFinalBlocks(List<Object> values) {
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[values.size()];
        for (int i = 0; i < values.size(); i++) {
            blocks[i] = buildBlock(blockFactory, values.get(i));
        }
        return blocks;
    }

    /**
     * Build blocks for INITIAL mode (intermediate format: value + seen boolean per aggregate).
     */
    private static Block[] buildIntermediateBlocks(List<Object> values) {
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        // Each aggregate produces 2 intermediate channels: value + seen
        Block[] blocks = new Block[values.size() * 2];
        for (int i = 0; i < values.size(); i++) {
            blocks[i * 2] = buildBlock(blockFactory, values.get(i));
            blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
        }
        return blocks;
    }

    private static Block buildBlock(org.elasticsearch.compute.data.BlockFactory blockFactory, Object value) {
        if (value instanceof Long l) {
            return blockFactory.newConstantLongBlockWith(l, 1);
        } else if (value instanceof Integer n) {
            return blockFactory.newConstantIntBlockWith(n, 1);
        } else if (value instanceof Double d) {
            return blockFactory.newConstantDoubleBlockWith(d, 1);
        } else if (value instanceof Boolean b) {
            return blockFactory.newConstantBooleanBlockWith(b, 1);
        } else if (value instanceof String s) {
            return blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef(s), 1);
        } else if (value instanceof Number n) {
            return blockFactory.newConstantLongBlockWith(n.longValue(), 1);
        } else {
            return blockFactory.newConstantNullBlock(1);
        }
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
