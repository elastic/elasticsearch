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
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Pushes ungrouped aggregate functions (COUNT, MIN, MAX) to external sources when the
 * required statistics are available in the file metadata. Replaces the AggregateExec +
 * ExternalSourceExec subtree with a LocalSourceExec containing pre-computed results.
 * <p>
 * Supports both SINGLE and INITIAL modes. In SINGLE mode the replacement produces final-value
 * blocks (one block per aggregate). In INITIAL mode the replacement produces intermediate-format
 * blocks matching {@link AggregateExec#intermediateAttributes()}: for each aggregate, a typed
 * value block followed by a {@code seen} boolean block.
 * <p>
 * Handles intermediate EvalExec (from EVAL), ProjectExec (from RENAME), and FilterExec
 * nodes between the aggregate and external source by resolving aliased attribute names
 * back to the original column names before metadata lookup, and by classifying filters
 * against per-split statistics when present.
 * <p>
 * Supports multi-split queries when per-split statistics are available in
 * {@link org.elasticsearch.xpack.esql.datasources.FileSplit#splitStats()}.
 * Statistics are merged across splits (sum row counts, min-of-mins, max-of-maxes).
 * Falls back to normal execution when any split lacks stats.
 * <p>
 * Note: MIN/MAX pushdown uses raw values from file metadata. For DATE/TIMESTAMP columns,
 * the raw values may not match ESQL's millisecond representation. A future enhancement
 * should convert these values using the column's data type.
 */
public class PushStatsToExternalSource extends PhysicalOptimizerRules.OptimizerRule<AggregateExec> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec) {
        ExternalSourceAggregatePushdown.ExternalSourceInfo info = ExternalSourceAggregatePushdown.extractExternalSource(
            aggregateExec.child()
        );
        if (info == null) {
            return aggregateExec;
        }
        ExternalSourceExec externalExec = info.externalExec();
        AttributeMap<Attribute> aliasReplacedBy = info.aliasReplacedBy();
        Expression filterCondition = info.filterCondition();

        AggregatorMode mode = aggregateExec.getMode();
        if (mode != AggregatorMode.SINGLE && mode != AggregatorMode.INITIAL) {
            return aggregateExec;
        }

        if (aggregateExec.groupings().isEmpty() == false) {
            return aggregateExec;
        }

        Expression filterForClassification = filterCondition;
        if (filterCondition != null && aliasReplacedBy.isEmpty() == false) {
            filterForClassification = filterCondition.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
        }

        SplitStats stats;
        if (filterForClassification != null) {
            stats = ExternalSourceAggregatePushdown.resolveFilteredStats(externalExec, filterForClassification);
        } else {
            stats = SplitStats.resolveEffectiveStats(externalExec.splits(), externalExec.sourceMetadata());
        }
        if (stats == null) {
            return aggregateExec;
        }
        List<? extends NamedExpression> aggregates = aggregateExec.aggregates();

        List<Object> values = new ArrayList<>(aggregates.size());
        List<DataType> dataTypes = new ArrayList<>(aggregates.size());

        for (NamedExpression agg : aggregates) {
            if (agg instanceof Alias == false) {
                return aggregateExec;
            }
            Alias alias = (Alias) agg;
            Expression aggExpr = alias.child();
            if (aliasReplacedBy.isEmpty() == false) {
                aggExpr = aggExpr.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
            }
            Object value = resolveFromStats(aggExpr, stats);
            if (value == null) {
                return aggregateExec;
            }
            values.add(value);
            dataTypes.add(aggExpr instanceof AggregateFunction af ? af.dataType() : DataType.LONG);
        }

        List<Attribute> outputAttrs;
        Block[] blocks;
        if (mode == AggregatorMode.SINGLE) {
            outputAttrs = new ArrayList<>(aggregates.size());
            for (NamedExpression agg : aggregates) {
                outputAttrs.add(agg.toAttribute());
            }
            blocks = buildBlocks(values, dataTypes);
        } else {
            outputAttrs = aggregateExec.intermediateAttributes();
            blocks = buildIntermediateBlocks(values, dataTypes);
        }

        return new LocalSourceExec(aggregateExec.source(), outputAttrs, LocalSupplier.of(new Page(blocks)));
    }

    private static Object resolveFromStats(Expression aggFunction, SplitStats stats) {
        if (aggFunction instanceof Count count) {
            return resolveCount(count, stats);
        } else if (aggFunction instanceof Min min) {
            return resolveMin(min, stats);
        } else if (aggFunction instanceof Max max) {
            return resolveMax(max, stats);
        }
        return null;
    }

    private static Object resolveCount(Count count, SplitStats stats) {
        if (count.hasFilter()) {
            return null;
        }
        Expression target = count.field();
        if (target.foldable()) {
            return stats.rowCount();
        }
        if (target instanceof ReferenceAttribute ref) {
            long nc = stats.columnNullCount(ref.name());
            if (nc >= 0) {
                return stats.rowCount() - nc;
            }
        }
        return null;
    }

    private static Object resolveMin(Min min, SplitStats stats) {
        if (min.hasFilter()) {
            return null;
        }
        Expression target = min.field();
        if (target instanceof ReferenceAttribute ref) {
            return stats.columnMin(ref.name());
        }
        return null;
    }

    private static Object resolveMax(Max max, SplitStats stats) {
        if (max.hasFilter()) {
            return null;
        }
        Expression target = max.field();
        if (target instanceof ReferenceAttribute ref) {
            return stats.columnMax(ref.name());
        }
        return null;
    }

    private static Block[] buildIntermediateBlocks(List<Object> values, List<DataType> dataTypes) {
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[values.size() * 2];
        for (int i = 0; i < values.size(); i++) {
            blocks[i * 2] = PushAggregatesToExternalSource.buildBlock(blockFactory, values.get(i), dataTypes.get(i));
            blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
        }
        return blocks;
    }

    private static Block[] buildBlocks(List<Object> values, List<DataType> dataTypes) {
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[values.size()];
        for (int i = 0; i < values.size(); i++) {
            blocks[i] = PushAggregatesToExternalSource.buildBlock(blockFactory, values.get(i), dataTypes.get(i));
        }
        return blocks;
    }
}
