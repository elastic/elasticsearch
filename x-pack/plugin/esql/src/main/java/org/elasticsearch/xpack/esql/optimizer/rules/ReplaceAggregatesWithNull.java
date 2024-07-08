/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
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
 * Looks for any aggregates functions (max(field), min(field)) that act on a foldable-to-null expression.
 * Except COUNT_DISTINCT and COUNT (which should return 0), all other aggregate functions should return null.
 *
 * This applies to eval x = null | stats max(x) but also max(null) or max(2 + null).
 * All aggregate functions that are also nullable (COUNT_DISTINCT and COUNT are exceptions), will get a NULL
 * field replacement by the FoldNull rule, COUNT_DISTINCT will benefit from PropagateEvalFoldables.
 */
public final class ReplaceAggregatesWithNull extends OptimizerRules.OptimizerRule<Aggregate> {

    public ReplaceAggregatesWithNull() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        var aggs = aggregate.aggregates();
        List<NamedExpression> newAggs = new ArrayList<>(aggs.size());
        boolean changed = false;
        List<Alias> transientEval = new ArrayList<>();

        for (NamedExpression agg : aggs) {
            Expression e = Alias.unwrap(agg);
            Object value = null;
            boolean isNewAgg = false;
            if (e instanceof AggregateFunction af
                && af.field().foldable()
                && (DataType.isNull(af.field().dataType())
                    || af.field() == null
                    || af.field() instanceof Literal lit && lit.value() == null)) {
                isNewAgg = true;
                if (af instanceof CountDistinct || af instanceof Count) {
                    value = 0L;
                }
            } else if (e instanceof Literal literal && literal.value() == null) {
                isNewAgg = true;
            }

            if (isNewAgg) {
                /*
                 * Add an eval for every null (even if they are all "null"s, they can have different data types,
                 * depending on the Aggregate function return type).
                 * Also, copy the original alias id so that other nodes using it down stream (e.g. eval referring to the original agg)
                 * don't have to be updated. PruneColumns makes use of the Attribute ids to decide if unused references can be removed.
                 */
                var aliased = new Alias(agg.source(), agg.name(), Literal.of(agg, value), agg.toAttribute().id(), true);
                transientEval.add(aliased);
                changed = true;
            } else {
                newAggs.add(agg);
            }
        }

        LogicalPlan plan = aggregate;
        if (changed) {
            var source = aggregate.source();
            if (newAggs.isEmpty() == false) {
                plan = new Aggregate(source, aggregate.child(), aggregate.aggregateType(), aggregate.groupings(), newAggs);
            } else {
                // All aggs actually have been optimized away
                // \_Aggregate[[],[AVG([NULL][NULL]) AS s]]
                // Replace by a local relation with one row, followed by an eval, e.g.
                // \_Eval[[MVAVG([NULL][NULL]) AS s]]
                // \_LocalRelation[[{e}#21],[ConstantNullBlock[positions=1]]]
                plan = new LocalRelation(
                    source,
                    List.of(new EmptyAttribute(source)),
                    LocalSupplier.of(new Block[] { BlockUtils.constantBlock(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, null, 1) })
                );
            }
            if (transientEval.isEmpty() == false) {
                plan = new Eval(source, plan, transientEval);
                plan = new Project(source, plan, Expressions.asAttributes(aggs));
            }
        }

        return plan;
    }
}
