/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Replace nested expressions inside a {@link Aggregate} with synthetic eval.
 * {@code STATS SUM(a + 1) BY x % 2}
 * becomes
 * {@code EVAL `a + 1` = a + 1, `x % 2` = x % 2 | STATS SUM(`a+1`_ref) BY `x % 2`_ref}
 * and
 * {@code INLINE STATS SUM(a + 1) BY x % 2}
 * becomes
 * {@code EVAL `a + 1` = a + 1, `x % 2` = x % 2 | INLINE STATS SUM(`a+1`_ref) BY `x % 2`_ref}
 */
public final class ReplaceAggregateNestedExpressionWithEval extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformDown(p -> switch (p) {
            case InlineJoin inlineJoin -> rule(inlineJoin);
            // aggs having a StubRelation child are handled by the InlineJoin case above, only deal with the "stand-alone" Aggregate here.
            case Aggregate agg -> isInlineStats(agg) ? agg : rule(agg, null);
            default -> p;
        });
    }

    /**
     * Returns {@code true} if the Aggregate has a {@code StubRelation} as (grand)child, meaning it is under a {@code InlineJoin}, i.e.,
     * part of an {@code INLINE STATS}.
     */
    private static boolean isInlineStats(Aggregate aggregate) {
        var child = aggregate.child();
        while (child instanceof UnaryPlan unary) {
            child = unary.child();
        }
        return child instanceof StubRelation;
    }

    /**
     * The InlineJoin will perform the join on the groupings, so any expressions used within the group part of the Aggregate should be
     * performed on the left side of the join. The expressions used within the aggregates part of the Aggregate will remain on the right.
     */
    private static LogicalPlan rule(InlineJoin inlineJoin) {
        Holder<Eval> evalHolder = new Holder<>(null);
        LogicalPlan newRight = inlineJoin.right().transformDown(Aggregate.class, agg -> rule(agg, evalHolder));
        Eval eval = evalHolder.get();
        return eval != null
            ? new InlineJoin(inlineJoin.source(), eval.replaceChild(inlineJoin.left()), newRight, inlineJoin.config())
            : inlineJoin.replaceRight(newRight);
    }

    private static LogicalPlan rule(Aggregate aggregate, @Nullable Holder<Eval> evalForIJHolder) {
        Map<String, Attribute> evalNames = new HashMap<>();
        Map<GroupingFunction, Attribute> groupingAttributes = new HashMap<>();
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings());
        // Evaluations needed for expressions within the groupings
        // "| STATS c = COUNT(*) BY a + 1" --> "| EVAL `a + 1` = a + 1 | STATS s = COUNT(*) BY `a + 1`_ref"
        List<Alias> groupsEvals = new ArrayList<>(newGroupings.size());
        boolean groupingChanged = false;

        // start with the groupings since the aggs might reuse/reference them
        for (int i = 0, s = newGroupings.size(); i < s; i++) {
            Expression g = newGroupings.get(i);
            if (g instanceof Alias as) {
                Expression asChild = as.child();
                // for non-evaluable grouping functions, replace their nested expressions with attributes and extract the expression out
                // into an eval (added later below)
                if (asChild instanceof GroupingFunction.NonEvaluatableGroupingFunction gf) {
                    Expression newGroupingFunction = transformNonEvaluatableGroupingFunction(gf, groupsEvals);
                    if (newGroupingFunction != gf) {
                        groupingChanged = true;
                        newGroupings.set(i, as.replaceChild(newGroupingFunction));
                    }
                } else {
                    // Move the alias into an eval and replace it with its attribute.
                    groupingChanged = true;
                    var attr = as.toAttribute();
                    groupsEvals.add(as);
                    evalNames.put(as.name(), attr);
                    newGroupings.set(i, attr);
                    if (asChild instanceof GroupingFunction.EvaluatableGroupingFunction gf) {
                        groupingAttributes.put(gf, attr);
                    }
                }
            }
        }

        Holder<Boolean> aggsChanged = new Holder<>(false);
        List<? extends NamedExpression> aggs = aggregate.aggregates();
        List<NamedExpression> newAggs = new ArrayList<>(aggs.size());
        // Evaluations needed for expressions within the aggs
        // "| STATS s = SUM(a + 1)" --> "| EVAL `a + 1` = a + 1 | STATS s = SUM(`a + 1`_ref)"
        // (i.e. not outside, like `| STATS s = SUM(a) + 1`)
        List<Alias> aggsEvals = new ArrayList<>(aggs.size());

        // map to track common expressions
        Map<Expression, Attribute> expToAttribute = new HashMap<>();
        for (Alias a : groupsEvals) {
            expToAttribute.put(a.child().canonical(), a.toAttribute());
        }

        int[] counter = new int[] { 0 };
        // for the aggs make sure to unwrap the agg function and check the existing groupings
        for (NamedExpression agg : aggs) {
            NamedExpression a = (NamedExpression) agg.transformDown(Alias.class, as -> {
                // if the child is a nested expression
                Expression child = as.child();

                if (child instanceof AggregateFunction af && skipOptimisingAgg(af)) {
                    return as;
                }

                // check if the alias matches any from grouping otherwise unwrap it
                Attribute ref = evalNames.get(as.name());
                if (ref != null) {
                    aggsChanged.set(true);
                    return ref;
                }

                // look for the aggregate function
                var replaced = child.transformUp(
                    AggregateFunction.class,
                    af -> transformAggregateFunction(af, expToAttribute, aggsEvals, counter, aggsChanged)
                );
                // replace any evaluatable grouping functions with their references pointing to the added synthetic eval
                replaced = replaced.transformDown(GroupingFunction.EvaluatableGroupingFunction.class, gf -> {
                    aggsChanged.set(true);
                    // should never return null, as it's verified.
                    // but even if broken, the transform will fail safely; otoh, returning `gf` will fail later due to incorrect plan.
                    return groupingAttributes.get(gf);
                });

                return as.replaceChild(replaced);
            });

            newAggs.add(a);
        }

        if (groupingChanged || aggsChanged.get()) {
            List<Alias> rightEvals;
            if (evalForIJHolder != null) { // this is an INLINE STATS scenario, group evals go to the LHS
                if (groupsEvals.size() > 0) {
                    var eval = new Eval(aggregate.source(), aggregate.child(), groupsEvals);
                    evalForIJHolder.set(eval);

                    // update the StubRelation to include the refs that'll come from (to be added to) the LHS Eval
                    aggregate = (Aggregate) aggregate.transformDown(StubRelation.class, sr -> sr.extendWith(eval));
                }
                rightEvals = aggsEvals; // aggs evals that remain on the RHS, i.e. those that feed into the Aggregate
            } else { // this is a regular STATS scenario, all evals remain on the RHS
                rightEvals = groupsEvals;
                rightEvals.addAll(aggsEvals);
            }
            // add a RHS Eval if needed, going under the Aggregate
            var aggChild = rightEvals.size() > 0 ? new Eval(aggregate.source(), aggregate.child(), rightEvals) : aggregate.child();

            var groupings = groupingChanged ? newGroupings : aggregate.groupings();
            var aggregates = aggsChanged.get() ? newAggs : aggregate.aggregates();
            aggregate = aggregate.with(aggChild, groupings, aggregates);
        }

        return aggregate;
    }

    private static Expression transformNonEvaluatableGroupingFunction(
        GroupingFunction.NonEvaluatableGroupingFunction gf,
        List<Alias> evals
    ) {
        int counter = 0;
        boolean childrenChanged = false;
        List<Expression> newChildren = new ArrayList<>(gf.children().size());

        for (Expression ex : gf.children()) {
            if (ex instanceof Attribute || ex instanceof MapExpression) {
                newChildren.add(ex);
            } else { // TODO: foldables shouldn't require eval'ing either
                var alias = new Alias(ex.source(), syntheticName(ex, gf, counter++), ex, null, true);
                evals.add(alias);
                newChildren.add(alias.toAttribute());
                childrenChanged = true;
            }
        }

        return childrenChanged ? gf.replaceChildren(newChildren) : gf;
    }

    private static boolean skipOptimisingAgg(AggregateFunction af) {
        // shortcut for the common scenario
        if (af.field() instanceof Attribute) {
            return true;
        }

        // do not replace nested aggregates
        Holder<Boolean> foundNestedAggs = new Holder<>(Boolean.FALSE);
        af.field().forEachDown(AggregateFunction.class, unused -> foundNestedAggs.set(Boolean.TRUE));
        return foundNestedAggs.get();
    }

    private static Expression transformAggregateFunction(
        AggregateFunction af,
        Map<Expression, Attribute> expToAttribute,
        List<Alias> evals,
        int[] counter,
        Holder<Boolean> aggsChanged
    ) {
        Expression result = af;

        Expression field = af.field();
        // if the field is a nested expression (not attribute or literal), replace it
        if (field instanceof Attribute == false && field.foldable() == false) {
            // create a new alias if one doesn't exist yet
            Attribute attr = expToAttribute.computeIfAbsent(field.canonical(), k -> {
                Alias newAlias = new Alias(k.source(), syntheticName(k, af, counter[0]++), k, null, true);
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
    }

    private static String syntheticName(Expression expression, Expression func, int counter) {
        return TemporaryNameUtils.temporaryName(expression, func, counter);
    }
}
