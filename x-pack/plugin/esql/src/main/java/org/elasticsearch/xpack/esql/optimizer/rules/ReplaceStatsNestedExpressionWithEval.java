/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Replace nested expressions inside an aggregate with synthetic eval (which end up being projected away by the aggregate).
 * stats sum(a + 1) by x % 2
 * becomes
 * eval `a + 1` = a + 1, `x % 2` = x % 2 | stats sum(`a+1`_ref) by `x % 2`_ref
 */
public final class ReplaceStatsNestedExpressionWithEval extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        List<Alias> evals = new ArrayList<>();
        Map<String, Attribute> evalNames = new HashMap<>();
        Map<GroupingFunction, Attribute> groupingAttributes = new HashMap<>();
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings());
        boolean groupingChanged = false;

        // start with the groupings since the aggs might duplicate it
        for (int i = 0, s = newGroupings.size(); i < s; i++) {
            Expression g = newGroupings.get(i);
            // move the alias into an eval and replace it with its attribute
            if (g instanceof Alias as) {
                groupingChanged = true;
                var attr = as.toAttribute();
                evals.add(as);
                evalNames.put(as.name(), attr);
                newGroupings.set(i, attr);
                if (as.child() instanceof GroupingFunction gf) {
                    groupingAttributes.put(gf, attr);
                }
            }
        }

        Holder<Boolean> aggsChanged = new Holder<>(false);
        List<? extends NamedExpression> aggs = aggregate.aggregates();
        List<NamedExpression> newAggs = new ArrayList<>(aggs.size());

        // map to track common expressions
        Map<Expression, Attribute> expToAttribute = new HashMap<>();
        for (Alias a : evals) {
            expToAttribute.put(a.child().canonical(), a.toAttribute());
        }

        int[] counter = new int[] { 0 };
        // for the aggs make sure to unwrap the agg function and check the existing groupings
        for (NamedExpression agg : aggs) {
            NamedExpression a = (NamedExpression) agg.transformDown(Alias.class, as -> {
                // if the child is a nested expression
                Expression child = as.child();

                // do not replace nested aggregates
                if (child instanceof AggregateFunction af) {
                    Holder<Boolean> foundNestedAggs = new Holder<>(Boolean.FALSE);
                    af.children().forEach(e -> e.forEachDown(AggregateFunction.class, unused -> foundNestedAggs.set(Boolean.TRUE)));
                    if (foundNestedAggs.get()) {
                        return as;
                    }
                }

                // shortcut for common scenario
                if (child instanceof AggregateFunction af && af.field() instanceof Attribute) {
                    return as;
                }

                // check if the alias matches any from grouping otherwise unwrap it
                Attribute ref = evalNames.get(as.name());
                if (ref != null) {
                    aggsChanged.set(true);
                    return ref;
                }

                // 1. look for the aggregate function
                var replaced = child.transformUp(AggregateFunction.class, af -> {
                    Expression result = af;

                    Expression field = af.field();
                    // 2. if the field is a nested expression (not attribute or literal), replace it
                    if (field instanceof Attribute == false && field.foldable() == false) {
                        // 3. create a new alias if one doesn't exist yet no reference
                        Attribute attr = expToAttribute.computeIfAbsent(field.canonical(), k -> {
                            Alias newAlias = new Alias(k.source(), syntheticName(k, af, counter[0]++), null, k, null, true);
                            evals.add(newAlias);
                            return newAlias.toAttribute();
                        });
                        aggsChanged.set(true);
                        // replace field with attribute
                        List<Expression> newChildren = new ArrayList<>(af.children());
                        newChildren.set(0, attr);
                        result = af.replaceChildren(newChildren);
                    }
                    return result;
                });
                // replace any grouping functions with their references pointing to the added synthetic eval
                replaced = replaced.transformDown(GroupingFunction.class, gf -> {
                    aggsChanged.set(true);
                    // should never return null, as it's verified.
                    // but even if broken, the transform will fail safely; otoh, returning `gf` will fail later due to incorrect plan.
                    return groupingAttributes.get(gf);
                });

                return as.replaceChild(replaced);
            });

            newAggs.add(a);
        }

        if (evals.size() > 0) {
            var groupings = groupingChanged ? newGroupings : aggregate.groupings();
            var aggregates = aggsChanged.get() ? newAggs : aggregate.aggregates();

            var newEval = new Eval(aggregate.source(), aggregate.child(), evals);
            aggregate = new Aggregate(aggregate.source(), newEval, aggregate.aggregateType(), groupings, aggregates);
        }

        return aggregate;
    }

    static String syntheticName(Expression expression, AggregateFunction af, int counter) {
        return LogicalPlanOptimizer.temporaryName(expression, af, counter);
    }
}
