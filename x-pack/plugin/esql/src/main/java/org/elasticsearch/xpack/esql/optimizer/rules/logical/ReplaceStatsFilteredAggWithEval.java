/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces an aggregation function having a false/null filter with an EVAL node.
 * <pre>
 *     ... | STATS x = someAgg(y) WHERE FALSE {BY z} | ...
 *     =>
 *     ... | STATS x = someAgg(y) {BY z} > | EVAL x = null | KEEP x{, z} | ...
 * </pre>
 */
public class ReplaceStatsFilteredAggWithEval extends OptimizerRules.OptimizerRule<Aggregate> {
    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        List<NamedExpression> newAggs = new ArrayList<>(aggregate.aggregates().size());
        List<Alias> newEvals = new ArrayList<>(aggregate.aggregates().size());
        List<NamedExpression> newProjections = new ArrayList<>(aggregate.aggregates().size());
        for (var ne : aggregate.aggregates()) {
            if (ne instanceof Alias alias
                && alias.child() instanceof AggregateFunction aggFunction
                && aggFunction.hasFilter()
                && aggFunction.filter() instanceof Literal literal
                && literal.fold().equals(false)) {
                Alias newAlias = alias.replaceChild(Literal.of(aggFunction, aggFunction instanceof Count ? 0L : null));
                newEvals.add(newAlias);
                newProjections.add(newAlias.toAttribute());
            } else {
                newAggs.add(ne); // agg function unchanged or grouping key
                newProjections.add(ne.toAttribute());
            }
        }

        LogicalPlan plan = aggregate;
        if (newEvals.isEmpty() == false) {
            plan = newEvals.size() < aggregate.aggregates().size() - aggregate.groupings().size()
                ? aggregate.with(aggregate.child(), aggregate.groupings(), newAggs)
                : aggregate.child();
            plan = new Eval(aggregate.source(), plan, newEvals);
            plan = new Project(aggregate.source(), plan, newProjections);
        }
        return plan;
    }
}
