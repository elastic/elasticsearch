/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Pushes ungrouped aggregate functions (COUNT, MIN, MAX) to external sources when the
 * required statistics are available in the file metadata. Replaces the AggregateExec +
 * ExternalSourceExec subtree with a LocalSourceExec containing pre-computed results.
 * <p>
 * Handles intermediate EvalExec (from EVAL) and ProjectExec (from RENAME) nodes between
 * the aggregate and external source by resolving aliased attribute names back to the
 * original column names before metadata lookup.
 * <p>
 * Supports multi-split queries when per-split statistics are available in
 * {@link FileSplit#statistics()}. Statistics are merged across splits (sum row counts,
 * min-of-mins, max-of-maxes). Falls back to normal execution when any split lacks stats.
 * <p>
 * Note: MIN/MAX pushdown uses raw values from file metadata. For DATE/TIMESTAMP columns,
 * the raw values may not match ESQL's millisecond representation. A future enhancement
 * should convert these values using the column's data type.
 */
public class PushStatsToExternalSource extends PhysicalOptimizerRules.OptimizerRule<AggregateExec> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec) {
        PhysicalPlan child = aggregateExec.child();
        ExternalSourceExec externalExec;
        AttributeMap<Attribute> aliasReplacedBy;

        if (child instanceof ExternalSourceExec ext) {
            externalExec = ext;
            aliasReplacedBy = AttributeMap.emptyAttributeMap();
        } else if (child instanceof EvalExec evalExec && evalExec.child() instanceof ExternalSourceExec ext) {
            externalExec = ext;
            aliasReplacedBy = PushFiltersToSource.getAliasReplacedBy(evalExec);
        } else if (child instanceof ProjectExec projectExec && projectExec.child() instanceof ExternalSourceExec ext) {
            externalExec = ext;
            aliasReplacedBy = PushFiltersToSource.getAliasReplacedBy(projectExec);
        } else {
            return aggregateExec;
        }

        if (aggregateExec.groupings().isEmpty() == false) {
            return aggregateExec;
        }

        Map<String, Object> sourceMetadata = resolveEffectiveMetadata(externalExec);
        if (sourceMetadata == null) {
            return aggregateExec;
        }
        List<? extends NamedExpression> aggregates = aggregateExec.aggregates();

        List<Object> values = new ArrayList<>(aggregates.size());
        List<Attribute> outputAttrs = new ArrayList<>(aggregates.size());

        for (NamedExpression agg : aggregates) {
            if (agg instanceof Alias == false) {
                return aggregateExec;
            }
            Alias alias = (Alias) agg;
            Expression aggExpr = alias.child();
            if (aliasReplacedBy.isEmpty() == false) {
                aggExpr = aggExpr.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
            }
            Object value = resolveFromMetadata(aggExpr, sourceMetadata);
            if (value == null) {
                return aggregateExec;
            }
            values.add(value);
            outputAttrs.add(agg.toAttribute());
        }

        Block[] blocks = buildBlocks(values);
        return new LocalSourceExec(aggregateExec.source(), outputAttrs, LocalSupplier.of(new Page(blocks)));
    }

    private static Object resolveFromMetadata(Expression aggFunction, Map<String, Object> sourceMetadata) {
        if (aggFunction instanceof Count count) {
            return resolveCount(count, sourceMetadata);
        } else if (aggFunction instanceof Min min) {
            return resolveMin(min, sourceMetadata);
        } else if (aggFunction instanceof Max max) {
            return resolveMax(max, sourceMetadata);
        }
        return null;
    }

    private static Object resolveCount(Count count, Map<String, Object> sourceMetadata) {
        if (count.hasFilter()) {
            return null;
        }
        Expression target = count.field();
        if (target.foldable()) {
            OptionalLong rc = SourceStatisticsSerializer.extractRowCount(sourceMetadata);
            return rc.isPresent() ? rc.getAsLong() : null;
        }
        if (target instanceof ReferenceAttribute ref) {
            OptionalLong rc = SourceStatisticsSerializer.extractRowCount(sourceMetadata);
            OptionalLong nc = SourceStatisticsSerializer.extractColumnNullCount(sourceMetadata, ref.name());
            if (rc.isPresent() && nc.isPresent()) {
                return rc.getAsLong() - nc.getAsLong();
            }
        }
        return null;
    }

    private static Object resolveMin(Min min, Map<String, Object> sourceMetadata) {
        if (min.hasFilter()) {
            return null;
        }
        Expression target = min.field();
        if (target instanceof ReferenceAttribute ref) {
            Optional<Object> minVal = SourceStatisticsSerializer.extractColumnMin(sourceMetadata, ref.name());
            return minVal.orElse(null);
        }
        return null;
    }

    private static Object resolveMax(Max max, Map<String, Object> sourceMetadata) {
        if (max.hasFilter()) {
            return null;
        }
        Expression target = max.field();
        if (target instanceof ReferenceAttribute ref) {
            Optional<Object> maxVal = SourceStatisticsSerializer.extractColumnMax(sourceMetadata, ref.name());
            return maxVal.orElse(null);
        }
        return null;
    }

    private static Map<String, Object> resolveEffectiveMetadata(ExternalSourceExec externalExec) {
        List<? extends ExternalSplit> splits = externalExec.splits();
        if (splits.size() <= 1) {
            return externalExec.sourceMetadata();
        }
        List<Map<String, Object>> splitStats = new ArrayList<>(splits.size());
        for (ExternalSplit split : splits) {
            if (split instanceof FileSplit fileSplit) {
                splitStats.add(fileSplit.statistics());
            } else {
                return null;
            }
        }
        return SourceStatisticsSerializer.mergeStatistics(splitStats);
    }

    private static Block[] buildBlocks(List<Object> values) {
        BlockFactory blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        Block[] blocks = new Block[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value instanceof Long l) {
                blocks[i] = blockFactory.newConstantLongBlockWith(l, 1);
            } else if (value instanceof Integer n) {
                blocks[i] = blockFactory.newConstantIntBlockWith(n, 1);
            } else if (value instanceof Double d) {
                blocks[i] = blockFactory.newConstantDoubleBlockWith(d, 1);
            } else if (value instanceof Boolean b) {
                blocks[i] = blockFactory.newConstantBooleanBlockWith(b, 1);
            } else if (value instanceof String s) {
                blocks[i] = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef(s), 1);
            } else if (value instanceof Number n) {
                blocks[i] = blockFactory.newConstantLongBlockWith(n.longValue(), 1);
            } else {
                blocks[i] = blockFactory.newConstantNullBlock(1);
            }
        }
        return blocks;
    }
}
