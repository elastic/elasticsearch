/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Tells the data nodes that a {@code STATS COUNT(*) BY key} is only read through {@code SORT count DESC | LIMIT N}.
 * <p>
 * The data node runs the partial aggregation and does not see the {@code TopN} above it, which runs on the coordinator. With the
 * {@code N} the data node can drop the rows of the groups that cannot be in the top {@code N} before it aggregates them. The
 * result of the query does not change. See {@code SketchFilterOperator}.
 * <p>
 * The rule stamps {@code N} on the {@link FragmentExec} below the aggregation. It does so only when the query has exactly this shape
 * (a single sort on the descending count, optionally through projections that only rename and evals that do not define it, over
 * a {@code COUNT(*)} with grouping columns that can be packed into a {@code long}), when the pragma is on, and when every node can read the stamp.
 */
public class MarkTopCountAggregations extends PhysicalOptimizerRules.ParameterizedOptimizerRule<TopNExec, PhysicalOptimizerContext> {
    /** The largest {@code N} that is worth filtering for. The proof needs the N-th group to be far above the noise. */
    static final int MAX_LIMIT = 1000;

    @Override
    protected PhysicalPlan rule(TopNExec topN, PhysicalOptimizerContext context) {
        if (context.configuration().pragmas().sketchFilter() == false
            || FragmentExec.supportsTopCountLimit(context.minimumVersion()) == false) {
            return topN;
        }
        Integer limit = topCountLimit(topN);
        if (limit == null) {
            return topN;
        }
        return topN.transformDown(FragmentExec.class, fragment -> fragment.withTopCountLimit(limit));
    }

    @Nullable
    private static Integer topCountLimit(TopNExec topN) {
        if (topN.order().size() != 1) {
            return null;
        }
        Order order = topN.order().getFirst();
        if (order.direction() != Order.OrderDirection.DESC) {
            return null;
        }
        if (topN.limit() instanceof Literal == false) {
            return null;
        }
        Object limitValue = ((Literal) topN.limit()).value();
        if (limitValue instanceof Number == false) {
            return null;
        }
        long limit = ((Number) limitValue).longValue();
        if (limit < 1 || limit > MAX_LIMIT) {
            return null;
        }
        Attribute sorted = Expressions.attribute(order.child());
        PhysicalPlan child = topN.child();
        // Walk down to the aggregation. A projection may only rename the sorted column, and an EVAL (for example the quotient that
        // AVG becomes) must not define it.
        while (sorted != null) {
            if (child instanceof ProjectExec project) {
                sorted = resolveThrough(project, sorted);
                child = project.child();
            } else if (child instanceof EvalExec eval) {
                for (Alias field : eval.fields()) {
                    if (field.toAttribute().semanticEquals(sorted)) {
                        return null;
                    }
                }
                child = eval.child();
            } else {
                break;
            }
        }
        if (sorted == null || child instanceof AggregateExec == false) {
            return null;
        }
        AggregateExec aggregate = (AggregateExec) child;
        if (aggregate.getMode() != AggregatorMode.FINAL || aggregate.child() instanceof ExchangeExec == false) {
            return null;
        }
        if (((ExchangeExec) aggregate.child()).child() instanceof FragmentExec == false) {
            return null;
        }
        Attribute count = countStarAttribute(aggregate);
        if (count == null || count.semanticEquals(sorted) == false) {
            return null;
        }
        return (int) limit;
    }

    /**
     * The attribute below {@code project} that {@code sorted} is a plain rename of, or {@code null} if it is anything else.
     */
    @Nullable
    private static Attribute resolveThrough(ProjectExec project, Attribute sorted) {
        for (NamedExpression projection : project.projections()) {
            if (projection.toAttribute().semanticEquals(sorted)) {
                return projection instanceof Alias alias ? Expressions.attribute(alias.child()) : projection.toAttribute();
            }
        }
        return null;
    }

    /**
     * The grouping columns of {@code aggregate} if they can be packed into one {@code long} without collisions: one {@code long}
     * column, one {@code int} column, or two {@code int} columns. Otherwise {@code null}. See {@code KeyColumns}.
     */
    @Nullable
    public static List<Attribute> keyAttributes(AggregateExec aggregate) {
        final List<? extends Expression> groupings = aggregate.groupings();
        if (groupings.isEmpty() || groupings.size() > 2) {
            return null;
        }
        final List<Attribute> attributes = new ArrayList<>(groupings.size());
        for (Expression grouping : groupings) {
            Attribute attribute = Expressions.attribute(grouping);
            if (attribute == null) {
                return null;
            }
            attributes.add(attribute);
        }
        final ElementType first = PlannerUtils.toElementType(attributes.getFirst().dataType());
        if (attributes.size() == 1) {
            return first == ElementType.LONG || first == ElementType.INT ? attributes : null;
        }
        return first == ElementType.INT && PlannerUtils.toElementType(attributes.get(1).dataType()) == ElementType.INT ? attributes : null;
    }

    /**
     * If {@code aggregate} has exactly one {@code COUNT(*)} (no filter) and grouping columns that {@link #keyAttributes} accepts, return
     * the attribute of that count. Otherwise return {@code null}. The data node uses it to check that it can filter.
     * <p>
     * Other aggregates next to the count are fine. The filter only drops the rows of keys that cannot be in the top {@code N}, and keeps
     * all rows of every other key, so every aggregate of a key that is kept is computed from all of its rows.
     */
    @Nullable
    public static Attribute countStarAttribute(AggregateExec aggregate) {
        if (keyAttributes(aggregate) == null) {
            return null;
        }
        Attribute count = null;
        for (NamedExpression named : aggregate.aggregates()) {
            Expression unwrapped = Alias.unwrap(named);
            if (unwrapped instanceof Count countFunction
                && countFunction.hasFilter() == false
                && countFunction.field() instanceof Literal) {
                if (count != null) {
                    return null; // two COUNT(*): not worth the question which one is sorted
                }
                count = named.toAttribute();
            }
        }
        return count;
    }
}
