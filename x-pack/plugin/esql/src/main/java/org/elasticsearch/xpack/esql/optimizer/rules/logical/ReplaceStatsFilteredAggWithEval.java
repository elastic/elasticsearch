/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Absent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Present;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces an aggregation function having a false/null filter with an EVAL node.
 * <pre>
 *     ... | STATS/INLINE STATS x = someAgg(y) WHERE FALSE {BY z} | ...
 *     =>
 *     ... | STATS/INLINE STATS x = someAgg(y) {BY z} > | EVAL x = NULL | KEEP x{, z} | ...
 * </pre>
 *
 * This rule is applied to both STATS' {@link Aggregate} and {@link InlineJoin} right-hand side {@link Aggregate} plans.
 * The logic is common for both, but the handling of the {@link InlineJoin} is slightly different when it comes to pruning
 * its right-hand side {@link Aggregate}.
 * Skipped in local optimizer: once a fragment contains an Agg, this can no longer be pruned, which the rule can do
 */
public class ReplaceStatsFilteredAggWithEval extends OptimizerRules.OptimizerRule<LogicalPlan> implements OptimizerRules.CoordinatorOnly {
    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        Aggregate aggregate;
        InlineJoin ij = null;
        if (plan instanceof Aggregate a) {
            aggregate = a;
        } else if (plan instanceof InlineJoin inlineJoin) {
            ij = inlineJoin;
            Holder<Aggregate> aggHolder = new Holder<>();
            inlineJoin.right().forEachDown(Aggregate.class, aggHolder::setIfAbsent);
            aggregate = aggHolder.get();
        } else {
            return plan; // not an Aggregate or InlineJoin, nothing to do
        }

        if (aggregate != null) {
            int oldAggSize = aggregate.aggregates().size();
            List<NamedExpression> newAggs = new ArrayList<>(oldAggSize);
            List<Alias> newEvals = new ArrayList<>(oldAggSize);
            List<NamedExpression> newProjections = new ArrayList<>(oldAggSize);

            for (var ne : aggregate.aggregates()) {
                if (ne instanceof Alias alias
                    && alias.child() instanceof AggregateFunction aggFunction
                    && aggFunction.hasFilter()
                    && aggFunction.filter() instanceof Literal literal
                    && Boolean.FALSE.equals(literal.value())) {

                    Object value = mapNullToValue(aggFunction);
                    Alias newAlias = alias.replaceChild(Literal.of(aggFunction, value));
                    newEvals.add(newAlias);
                    newProjections.add(newAlias.toAttribute());
                } else {
                    newAggs.add(ne); // agg function unchanged or grouping key
                    newProjections.add(ne.toAttribute());
                }
            }

            if (newEvals.isEmpty() == false) {
                if (newAggs.isEmpty()) { // the Aggregate node is pruned
                    if (ij != null) { // this is an Aggregate part of right-hand side of an InlineJoin
                        final LogicalPlan leftHandSide = ij.left(); // final so we can use it in the lambda below
                        // the aggregate becomes a simple Eval since it's not needed anymore (it was replaced with Literals)
                        var newRight = ij.right()
                            .transformDown(
                                Aggregate.class,
                                agg -> agg == aggregate ? new Eval(aggregate.source(), aggregate.child(), newEvals) : agg
                            );
                        // Remove the StubRelation since the right-hand side of the join is now part of the main plan
                        // and it won't be executed separately by the EsqlSession INLINE STATS planning.
                        newRight = InlineJoin.replaceStub(leftHandSide, newRight);

                        // project the correct output (the one of the former inlinejoin) and remove the InlineJoin altogether,
                        // replacing it with its right-hand side followed by its left-hand side
                        plan = new Project(ij.source(), newRight, ij.output());
                    } else { // this is a standalone Aggregate
                        plan = localRelation(aggregate.source(), newEvals);
                    }
                } else {
                    if (ij != null) { // this is an Aggregate part of right-hand side of an InlineJoin
                        plan = ij.replaceRight(
                            ij.right().transformUp(Aggregate.class, agg -> updateAggregate(agg, newAggs, newEvals, newProjections))
                        );
                    } else { // this is a standalone Aggregate
                        plan = updateAggregate(aggregate, newAggs, newEvals, newProjections);
                    }
                }
            }
        }
        return plan;
    }

    private static Object mapNullToValue(AggregateFunction aggFunction) {
        return switch (aggFunction) {
            case Count ignored -> 0L;
            case CountDistinct ignored -> 0L;
            case Absent ignored -> true;
            case Present ignored -> false;
            default -> null;
        };
    }

    private static LogicalPlan updateAggregate(
        Aggregate agg,
        List<NamedExpression> newAggs,
        List<Alias> newEvals,
        List<NamedExpression> newProjections
    ) {
        // only update the Aggregate and add an Eval for the removed aggregations
        LogicalPlan newAgg = agg.with(agg.child(), agg.groupings(), newAggs);
        newAgg = new Eval(agg.source(), newAgg, newEvals);
        newAgg = new Project(agg.source(), newAgg, newProjections);
        return newAgg;
    }

    private static LocalRelation localRelation(Source source, List<Alias> newEvals) {
        Block[] blocks = new Block[newEvals.size()];
        List<Attribute> attributes = new ArrayList<>(newEvals.size());
        for (int i = 0; i < newEvals.size(); i++) {
            Alias alias = newEvals.get(i);
            attributes.add(alias.toAttribute());
            blocks[i] = BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, ((Literal) alias.child()).value(), 1);
        }
        return new LocalRelation(source, attributes, LocalSupplier.of(new Page(blocks)));
    }
}
