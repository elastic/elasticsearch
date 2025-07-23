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
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces an aggregation function having a false/null filter with an EVAL node.
 * <pre>
 *     ... | INLINESTATS x = someAgg(y) WHERE FALSE {BY z} | ...
 *     =>
 *     ... | STATS x = someAgg(y) {BY z} > | EVAL x = NULL | KEEP x{, z} | ...
 * </pre>
 */
public class ReplaceInlinestatsFilteredAggWithEval extends OptimizerRules.OptimizerRule<InlineJoin> {
    @Override
    protected LogicalPlan rule(InlineJoin ij) {
        Aggregate aggregate;
        LogicalPlan aggParent = null;
        if (ij.right() instanceof Aggregate a) {// shortcut
            aggregate = a;
        } else {
            // the parent of the aggregate is only needed to maybe prune the Aggregate if it's not needed anymore
            Holder<Aggregate> aggHolder = new Holder<>();
            Holder<LogicalPlan> parentHolder = new Holder<>();
            ij.right().forEachDown(p -> {
                if (p instanceof UnaryPlan up && up.child() instanceof Aggregate a && aggHolder.get() == null) {
                    aggHolder.set(a);
                    parentHolder.set(up);
                }
            });
            aggregate = aggHolder.get();
            aggParent = parentHolder.get();
        }
        if (aggregate == null) {
            // the Aggregate has been pruned already and now the InlineJoin itself needs pruning
//            if (ij.right() instanceof Project proj) {
//                return InlineJoin.replaceStub(ij.left(), proj.child());
//            } else {
                return InlineJoin.replaceStub(ij.left(), ij.right());
            //}
            //return ij;
        }

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

        LogicalPlan plan = ij;
        if (newEvals.isEmpty() == false) {
            if (newAggs.isEmpty()) {
                if (aggParent != null) {
                    // here we don't prune the InlineJoin yet, to have the rest of the nodes go through the optimizer again
                    plan = new InlineJoin(ij.source(), ij.left(), ij.right().transformDown(p -> {
                        if (p instanceof UnaryPlan up && up.child() == aggregate) {
                            p = up.replaceChild(new Eval(aggregate.source(), aggregate.child(), newEvals));
                        }
                        return p;
                    }), ij.config());
                } else {
                    plan = new Eval(ij.source(), ij.left(), newEvals);
                }
            }/* else {
                plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
                plan = new Eval(aggregate.source(), plan, newEvals);
                plan = new Project(aggregate.source(), plan, newProjections);
            }*/
        }
        return plan;
    }
}
