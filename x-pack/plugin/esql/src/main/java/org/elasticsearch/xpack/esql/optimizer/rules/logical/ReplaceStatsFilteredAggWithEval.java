/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces an aggregation function having a false/null filter with an EVAL node.
 * <pre>
 *     ... | STATS/INLINESTATS x = someAgg(y) WHERE FALSE {BY z} | ...
 *     =>
 *     ... | STATS/INLINESTATS x = someAgg(y) {BY z} > | EVAL x = NULL | KEEP x{, z} | ...
 * </pre>
 *
 * This rule is applied to both STATS' {@link Aggregate} and {@link InlineJoin} right-hand side {@link Aggregate} plans.
 * The logic is common for both, but the handling of the {@link InlineJoin} is slightly different when it comes to pruning
 * its right-hand side {@link Aggregate}.
 */
public class ReplaceStatsFilteredAggWithEval extends OptimizerRules.OptimizerRule<LogicalPlan> {
    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        Aggregate aggregate;
        Holder<InlineJoin> ij = new Holder<>();
        if (plan instanceof Aggregate a) {
            aggregate = a;
        } else if (plan instanceof InlineJoin) {
            ij.set((InlineJoin) plan);
            Holder<Aggregate> aggHolder = new Holder<>();
            ij.get().right().forEachDown(p -> {
                if (aggHolder.get() == null && p instanceof Aggregate a) {
                    aggHolder.set(a);
                }
            });
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

                    Object value = aggFunction instanceof Count || aggFunction instanceof CountDistinct ? 0L : null;
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
                    if (ij.get() != null) { // this is an Aggregate part of right-hand side of an InlineJoin
                        // replace the right hand side Aggregate with an Eval
                        InlineJoin newIJ = new InlineJoin(ij.get().source(), ij.get().left(), ij.get().right().transformDown(p -> {
                            // both following steps are executed for an InlineJoin
                            if (p instanceof Aggregate a && a == aggregate) {
                                // the aggregate becomes a simple Eval since it's not needed anymore (it was replaced with Literals)
                                p = new Eval(aggregate.source(), aggregate.child(), newEvals);
                            } else if (p instanceof StubRelation) {
                                // Remove the StubRelation since the right-hand side of the join is now part of the main plan
                                // and it won't be executed separately by the EsqlSession inlinestats planning.
                                p = ij.get().left();
                            }
                            return p;
                        }), ij.get().config());
                        // project the correct output (the one of the former inlinejoin) and remove the InlineJoin altogether,
                        // replacing it with its right-hand side followed by its left-hand side
                        plan = new Project(newIJ.source(), newIJ.right(), newIJ.output());
                    } else { // this is a standalone Aggregate
                        plan = localRelation(aggregate.source(), newEvals);
                    }
                } else {
                    if (ij.get() != null) { // this is an Aggregate part of right-hand side of an InlineJoin
                        // only update the Aggregate and add an Eval for the removed aggregations
                        plan = ij.get().transformUp(Aggregate.class, agg -> {
                            // if the aggregate is not the one we are looking for, return it unchanged
                            if (agg != aggregate) {
                                return agg;
                            }
                            LogicalPlan newPlan = agg.with(aggregate.child(), aggregate.groupings(), newAggs);
                            newPlan = new Eval(aggregate.source(), newPlan, newEvals);
                            newPlan = new Project(aggregate.source(), newPlan, newProjections);
                            return newPlan;
                        });
                    } else { // this is a standalone Aggregate
                        plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
                        plan = new Eval(aggregate.source(), plan, newEvals);
                        plan = new Project(aggregate.source(), plan, newProjections);
                    }
                }
            }
        }
        return plan;
    }

    private static LocalRelation localRelation(Source source, List<Alias> newEvals) {
        Block[] blocks = new Block[newEvals.size()];
        List<Attribute> attributes = new ArrayList<>(newEvals.size());
        for (int i = 0; i < newEvals.size(); i++) {
            Alias alias = newEvals.get(i);
            attributes.add(alias.toAttribute());
            blocks[i] = BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, ((Literal) alias.child()).value(), 1);
        }
        return new LocalRelation(source, attributes, LocalSupplier.of(blocks));
    }
}
