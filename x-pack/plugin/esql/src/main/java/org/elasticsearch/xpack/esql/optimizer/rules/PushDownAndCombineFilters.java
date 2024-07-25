/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public final class PushDownAndCombineFilters extends OptimizerRules.OptimizerRule<Filter> {
    @Override
    protected LogicalPlan rule(Filter filter) {
        LogicalPlan plan = filter;
        LogicalPlan child = filter.child();
        Expression condition = filter.condition();

        if (child instanceof Filter f) {
            // combine nodes into a single Filter with updated ANDed condition
            plan = f.with(Predicates.combineAnd(List.of(f.condition(), condition)));
        } else if (child instanceof Aggregate agg) { // TODO: re-evaluate along with multi-value support
            // Only push [parts of] a filter past an agg if these/it operates on agg's grouping[s], not output.
            plan = maybePushDownPastUnary(
                filter,
                agg,
                e -> e instanceof Attribute && agg.output().contains(e) && agg.groupings().contains(e) == false
                    || e instanceof AggregateFunction
            );
        } else if (child instanceof Eval eval) {
            // Don't push if Filter (still) contains references of Eval's fields.
            var attributes = new AttributeSet(Expressions.asAttributes(eval.fields()));
            plan = maybePushDownPastUnary(filter, eval, attributes::contains);
        } else if (child instanceof RegexExtract re) {
            // Push down filters that do not rely on attributes created by RegexExtract
            var attributes = new AttributeSet(Expressions.asAttributes(re.extractedFields()));
            plan = maybePushDownPastUnary(filter, re, attributes::contains);
        } else if (child instanceof Enrich enrich) {
            // Push down filters that do not rely on attributes created by Enrich
            var attributes = new AttributeSet(Expressions.asAttributes(enrich.enrichFields()));
            plan = maybePushDownPastUnary(filter, enrich, attributes::contains);
        } else if (child instanceof Project) {
            return LogicalPlanOptimizer.pushDownPastProject(filter);
        } else if (child instanceof OrderBy orderBy) {
            // swap the filter with its child
            plan = orderBy.replaceChild(filter.with(orderBy.child(), condition));
        }
        // cannot push past a Limit, this could change the tailing result set returned
        return plan;
    }

    private static LogicalPlan maybePushDownPastUnary(Filter filter, UnaryPlan unary, Predicate<Expression> cannotPush) {
        LogicalPlan plan;
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : Predicates.splitAnd(filter.condition())) {
            (exp.anyMatch(cannotPush) ? nonPushable : pushable).add(exp);
        }
        // Push the filter down even if it might not be pushable all the way to ES eventually: eval'ing it closer to the source,
        // potentially still in the Exec Engine, distributes the computation.
        if (pushable.size() > 0) {
            if (nonPushable.size() > 0) {
                Filter pushed = new Filter(filter.source(), unary.child(), Predicates.combineAnd(pushable));
                plan = filter.with(unary.replaceChild(pushed), Predicates.combineAnd(nonPushable));
            } else {
                plan = unary.replaceChild(filter.with(unary.child(), filter.condition()));
            }
        } else {
            plan = filter;
        }
        return plan;
    }
}
