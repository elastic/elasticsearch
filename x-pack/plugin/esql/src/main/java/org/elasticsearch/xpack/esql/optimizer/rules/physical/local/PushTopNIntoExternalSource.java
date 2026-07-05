/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

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
 *   <li>The grouping element type is supported by a Top-N {@link BlockHash} implementation
 *       (currently {@link ElementType#LONG} and {@link ElementType#BYTES_REF}).</li>
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
            // Use the shared helper to validate the wrapper-shape contract — anything outside the small set of
            // recognized wrappers (Eval/Project/Filter, possibly nested) is rejected here, so the rebuild below
            // only needs to swap the leaf source within an already-accepted subtree.
            ExternalSourceExec ext = ExternalSourceAggregatePushdown.findExternalSource(aggregate.child());
            if (ext == null || ext.pushedTopN() != null) {
                return topNExec;
            }
            BlockHash.TopNDef topNDef = buildTopNDef(topNExec, aggregate, ctx);
            if (topNDef == null) {
                return topNExec;
            }
            ExternalSourceExec annotated = ext.withPushedTopN(topNDef);
            // Rebuild the aggregate-child subtree by retargeting the original ext leaf at the annotated copy.
            // findExternalSource has already validated the wrapper shape, so we know exactly one ExternalSourceExec
            // sits below; transformDown on the leaf class is the simplest faithful rebuild.
            PhysicalPlan rebuiltAggregateChild = aggregate.child().transformDown(ExternalSourceExec.class, e -> e == ext ? annotated : e);
            return topNExec.replaceChild(aggregate.replaceChild(rebuiltAggregateChild));
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
        // Skip the annotation when the grouping type is not yet handled by a Top-N BlockHash. This keeps the
        // hint as a strict promise that BlockHash#build will honor it; downstream code can rely on a non-null
        // pushedTopN to mean "Top-N pruning is wired in".
        ElementType groupingElementType = PlannerUtils.toElementType(groupAttr.dataType());
        if (groupingElementType != ElementType.LONG && groupingElementType != ElementType.BYTES_REF) {
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
            // Defensive: the analyzer normally constrains LIMIT to a non-negative int literal, but folding can
            // widen to Long. Math.toIntExact rejects values outside int range with an explicit ArithmeticException
            // rather than silently truncating; we still validate positivity ourselves so callers never see 0.
            int limit;
            try {
                limit = Math.toIntExact(n.longValue());
            } catch (ArithmeticException ignored) {
                return null;
            }
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

}
