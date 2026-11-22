/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.UnaryAggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Answers an ungrouped {@code STATS MIN(field), MAX(field)...} over a whole index from the metadata of its segments, instead of
 * reading the field: the aggregate is replaced by a single row computed while planning.
 * <p>
 * This applies only when every aggregate is an unfiltered {@code MIN} or {@code MAX} of a field the statistics can answer exactly
 * (see {@link SearchStats#exactMinMax}), and nothing narrows the documents the aggregate reads: no filter and no sampling. The
 * aggregate has to sit directly on the source, which is also why a limit never applies: it stays above the source.
 * The one filter that is allowed is the {@code IS NOT NULL} one that the local logical optimizer adds for these aggregates.
 * If any of that fails the plan is left alone, so the query runs as it would without this rule.
 */
public class PushMinMaxStatsToLocalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext context) {
        AggregatorMode mode = aggregateExec.getMode();
        if (mode.isInputPartial()) {
            return aggregateExec;
        }
        // a time series aggregation reads each series as its own, so the bounds of the whole index do not describe what it reads
        if (aggregateExec instanceof TimeSeriesAggregateExec) {
            return aggregateExec;
        }
        // A scan then loads the values the way the preference asks, which can differ from the segment metadata
        if (context.configuration() != null
            && context.configuration().pragmas().fieldExtractPreference() != MappedFieldType.FieldExtractPreference.NONE) {
            return aggregateExec;
        }
        if (aggregateExec.groupings().isEmpty() == false || aggregateExec.aggregates().isEmpty()) {
            return aggregateExec;
        }

        if (aggregateExec.child() instanceof EsQueryExec == false) {
            return aggregateExec;
        }
        EsQueryExec queryExec = (EsQueryExec) aggregateExec.child();

        // What the aggregates read, before anything is asked of the statistics: a query that narrows the documents ends it
        Set<String> fields = new HashSet<>();
        List<FieldAttribute> targets = new ArrayList<>(aggregateExec.aggregates().size());
        List<Boolean> isMin = new ArrayList<>(aggregateExec.aggregates().size());
        for (NamedExpression aggregate : aggregateExec.aggregates()) {
            if (aggregate instanceof Alias == false) {
                return aggregateExec;
            }
            Expression function = ((Alias) aggregate).child();
            if (function instanceof Min == false && function instanceof Max == false) {
                return aggregateExec;
            }
            UnaryAggregateFunction minMax = (UnaryAggregateFunction) function;
            if (minMax.hasFilter() || minMax.hasWindow()) {
                return aggregateExec;
            }
            Expression target = minMax.field();
            if (target instanceof FieldAttribute == false || isSupported(target.dataType()) == false) {
                return aggregateExec;
            }
            targets.add((FieldAttribute) target);
            fields.add(((FieldAttribute) target).fieldName().string());
            isMin.add(function instanceof Min);
        }
        if (readsEveryDocumentWithAValue(queryExec.query(), fields) == false) {
            return aggregateExec;
        }

        List<SearchStats.ExactMinMax> bounds = new ArrayList<>(targets.size());
        for (FieldAttribute target : targets) {
            SearchStats.ExactMinMax exact = context.searchStats().exactMinMax(target.fieldName());
            // The shards of this node map the field as one type, but the query may read it as another: a field mapped as date in
            // one index and as date_nanos in another is read as date_nanos, whichever node a shard is on
            if (exact == null || (exact.type() != null && exact.type() != target.dataType())) {
                return aggregateExec;
            }
            bounds.add(exact);
        }

        List<Attribute> output;
        Block[] blocks;
        BlockFactory blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        if (mode == AggregatorMode.SINGLE) {
            output = new ArrayList<>(bounds.size());
            blocks = new Block[bounds.size()];
            for (int i = 0; i < bounds.size(); i++) {
                Attribute attribute = aggregateExec.aggregates().get(i).toAttribute();
                output.add(attribute);
                SearchStats.ExactMinMax exact = bounds.get(i);
                blocks[i] = exact.hasValues()
                    ? valueBlock(blockFactory, attribute.dataType(), isMin.get(i) ? exact.min() : exact.max())
                    : blockFactory.newConstantNullBlock(1);
            }
        } else {
            // the intermediate state of MIN and MAX is the value and whether any value was seen
            output = aggregateExec.intermediateAttributes();
            assert output.size() == bounds.size() * 2 : "expected a value and a seen flag per aggregate but got " + output;
            blocks = new Block[output.size()];
            for (int i = 0; i < bounds.size(); i++) {
                SearchStats.ExactMinMax exact = bounds.get(i);
                long value = exact.hasValues() ? (isMin.get(i) ? exact.min() : exact.max()) : 0L;
                blocks[i * 2] = valueBlock(blockFactory, output.get(i * 2).dataType(), value);
                blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(exact.hasValues(), 1);
            }
        }
        return new LocalSourceExec(aggregateExec.source(), output, LocalSupplier.of(new Page(blocks)));
    }

    /**
     * Whether the query leaves every document that holds a value of one of the {@code fields}, which are the fields the aggregates
     * read, so that the statistics of the index describe what the aggregates read. That is so without a query, and with the filter
     * {@code InferNonNullAggConstraint} adds, which keeps the documents that hold a value of any aggregated field and drops only
     * those that no aggregate would read anyway. A filter on a subset of the fields would drop documents that hold only the others.
     */
    static boolean readsEveryDocumentWithAValue(QueryBuilder query, Set<String> fields) {
        if (query == null || query instanceof MatchAllQueryBuilder) {
            return true;
        }
        Set<String> existing = new HashSet<>();
        return collectExistsFilter(query, existing) && existing.containsAll(fields);
    }

    /** Collects the fields of a query that is an {@code exists} on a field, or an "or" of them. Anything else is not that filter. */
    private static boolean collectExistsFilter(QueryBuilder query, Set<String> fields) {
        if (query instanceof ExistsQueryBuilder exists) {
            fields.add(exists.fieldName());
            return true;
        }
        if (query instanceof BoolQueryBuilder bool
            // more than one clause required to match drops the documents that hold only one of the fields
            && bool.minimumShouldMatch() == null
            && bool.must().isEmpty()
            && bool.filter().isEmpty()
            && bool.mustNot().isEmpty()
            && bool.should().isEmpty() == false) {
            for (QueryBuilder clause : bool.should()) {
                if (collectExistsFilter(clause, fields) == false) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /** The types {@link SearchStats#exactMinMax} can answer, as the types the aggregate sees. */
    private static boolean isSupported(DataType type) {
        return type == DataType.INTEGER || type == DataType.LONG || type == DataType.DATETIME || type == DataType.DATE_NANOS;
    }

    private static Block valueBlock(BlockFactory blockFactory, DataType type, long value) {
        ElementType elementType = PlannerUtils.toElementType(type);
        if (elementType == ElementType.INT) {
            return blockFactory.newConstantIntBlockWith(Math.toIntExact(value), 1);
        }
        if (elementType == ElementType.LONG) {
            return blockFactory.newConstantLongBlockWith(value, 1);
        }
        throw new IllegalStateException("MIN and MAX of [" + type + "] are not answered from statistics");
    }
}
