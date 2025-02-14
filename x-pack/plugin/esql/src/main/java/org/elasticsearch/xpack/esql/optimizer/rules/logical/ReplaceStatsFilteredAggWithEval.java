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
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces an aggregation function having a false/null filter with an EVAL node.
 * <pre>
 *     ... | STATS x = someAgg(y) WHERE FALSE {BY z} | ...
 *     =>
 *     ... | STATS x = someAgg(y) {BY z} > | EVAL x = NULL | KEEP x{, z} | ...
 * </pre>
 */
public class ReplaceStatsFilteredAggWithEval extends OptimizerRules.OptimizerRule<Aggregate> {
    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
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

        LogicalPlan plan = aggregate;
        if (newEvals.isEmpty() == false) {
            if (newAggs.isEmpty()) { // the Aggregate node is pruned
                plan = localRelation(aggregate.source(), newEvals);
            } else {
                plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
                plan = new Eval(aggregate.source(), plan, newEvals);
                plan = new Project(aggregate.source(), plan, newProjections);
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
