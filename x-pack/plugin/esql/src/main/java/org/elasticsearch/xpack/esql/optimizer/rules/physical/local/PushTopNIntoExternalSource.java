/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;

/**
 * Annotates an {@link ExternalSourceExec} with a Top-N grouping hint when the local plan tree has the shape
 * {@code TopNExec → AggregateExec → ... → ExternalSourceExec} and the sort is on a single grouping key
 * (not on an aggregation result). The hint flows through the planner into the {@code BlockHash}, which prunes
 * non-competitive groups during aggregation, eliminating the need to materialize the full hash table.
 *
 * <p>Conditions for the rule to fire:
 * <ul>
 *   <li>The sort has exactly one {@link Order}.</li>
 *   <li>The sort key references one of the aggregate's grouping attributes (not an aggregation output).</li>
 *   <li>The aggregate has exactly one grouping (multi-key Top-N is deferred).</li>
 *   <li>The limit is a non-negative integer literal.</li>
 *   <li>The aggregate's child subtree contains an {@link ExternalSourceExec}.</li>
 * </ul>
 *
 * <p>The {@link TopNExec} and {@link AggregateExec} nodes are <em>not</em> removed: they remain as the
 * correctness safety net (necessary for distributed execution where each node prunes independently and the
 * coordinator merges partial results). Only the {@link ExternalSourceExec#pushedTopN()} field is set.
 */
public class PushTopNIntoExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<TopNExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(TopNExec topNExec, LocalPhysicalOptimizerContext ctx) {
        if (topNExec.child() instanceof AggregateExec aggregate && aggregate.getClass() == AggregateExec.class) {
            BlockHash.TopNDef topNDef = buildTopNDef(topNExec, aggregate, ctx);
            if (topNDef == null) {
                return topNExec;
            }
            PhysicalPlan annotatedAggregateChild = annotateExternalSource(aggregate.child(), topNDef);
            if (annotatedAggregateChild == aggregate.child()) {
                return topNExec;
            }
            return topNExec.replaceChild(aggregate.replaceChild(annotatedAggregateChild));
        }
        return topNExec;
    }

    /**
     * Builds a {@link BlockHash.TopNDef} hint when the input pattern matches the rule's preconditions. Returns
     * {@code null} when any check fails so the caller can leave the plan untouched.
     */
    private static BlockHash.TopNDef buildTopNDef(TopNExec topNExec, AggregateExec aggregate, LocalPhysicalOptimizerContext ctx) {
        List<Order> orders = topNExec.order();
        if (orders.size() != 1) {
            return null;
        }
        if (aggregate.groupings().size() != 1) {
            return null;
        }
        Attribute groupAttr = Expressions.attribute(aggregate.groupings().get(0));
        if (groupAttr == null) {
            return null;
        }
        Order order = orders.get(0);
        Attribute sortAttr = Expressions.attribute(order.child());
        if (sortAttr == null || sortAttr.semanticEquals(groupAttr) == false) {
            return null;
        }
        Expression limitExpr = topNExec.limit();
        if (limitExpr instanceof Literal == false || limitExpr.foldable() == false) {
            return null;
        }
        Object folded = limitExpr.fold(ctx.foldCtx());
        if (folded instanceof Number n) {
            int limit = n.intValue();
            if (limit <= 0) {
                return null;
            }
            boolean asc = order.direction() == Order.OrderDirection.ASC;
            boolean nullsFirst = order.nullsPosition() == Order.NullsPosition.FIRST;
            // Order index 0 because we only allow a single grouping/sort key.
            return new BlockHash.TopNDef(0, asc, nullsFirst, limit);
        }
        return null;
    }

    /**
     * Walks the AggregateExec child subtree and returns a copy where the (single) {@link ExternalSourceExec}
     * has its {@code pushedTopN} hint set. Mirrors the small set of intermediate-node shapes recognized by
     * {@code ExternalSourceAggregatePushdown.extractExternalSource}. Returns the original plan unchanged when
     * no external source is found or its hint is already set.
     */
    private static PhysicalPlan annotateExternalSource(PhysicalPlan plan, BlockHash.TopNDef topNDef) {
        if (plan instanceof ExternalSourceExec ext) {
            return ext.pushedTopN() != null ? ext : ext.withPushedTopN(topNDef);
        }
        if (plan instanceof EvalExec eval && eval.child() instanceof ExternalSourceExec ext) {
            return ext.pushedTopN() != null ? eval : eval.replaceChild(ext.withPushedTopN(topNDef));
        }
        if (plan instanceof ProjectExec project && project.child() instanceof ExternalSourceExec ext) {
            return ext.pushedTopN() != null ? project : project.replaceChild(ext.withPushedTopN(topNDef));
        }
        if (plan instanceof FilterExec filter) {
            PhysicalPlan filterChild = filter.child();
            if (filterChild instanceof ExternalSourceExec ext) {
                return ext.pushedTopN() != null ? filter : filter.replaceChild(ext.withPushedTopN(topNDef));
            }
            if (filterChild instanceof EvalExec eval && eval.child() instanceof ExternalSourceExec ext) {
                return ext.pushedTopN() != null ? filter : filter.replaceChild(eval.replaceChild(ext.withPushedTopN(topNDef)));
            }
            if (filterChild instanceof ProjectExec project && project.child() instanceof ExternalSourceExec ext) {
                return ext.pushedTopN() != null ? filter : filter.replaceChild(project.replaceChild(ext.withPushedTopN(topNDef)));
            }
        }
        return plan;
    }
}
