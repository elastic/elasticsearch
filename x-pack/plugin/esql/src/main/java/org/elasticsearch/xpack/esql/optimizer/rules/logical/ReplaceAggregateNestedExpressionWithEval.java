/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Replace nested expressions inside a {@link Aggregate} with synthetic eval.
 * <p>
 * A nested expression in a {@code STATS}:
 * <pre>
 *     STATS SUM(a + 1) BY x % 2
 * </pre>
 * becomes:
 * <pre>
 *     EVAL `a + 1` = a + 1, `x % 2` = x % 2 | STATS SUM(`a + 1`) BY `x % 2`
 * </pre>
 * The same applies to {@code INLINE STATS}:
 * <pre>
 *     INLINE STATS SUM(a + 1) BY x % 2
 * </pre>
 * becomes:
 * <pre>
 *     EVAL `a + 1` = a + 1, `x % 2` = x % 2 | INLINE STATS SUM(`a + 1`) BY `x % 2`
 * </pre>
 * <p>
 * When {@code extractConstants} is set, constant fields  are materialized too. For example:
 * <pre>
 *     STATS TOP(42, 10, "asc", "n/a")
 * </pre>
 * becomes:
 * <pre>
 *     EVAL `42` = 42, `n/a` = "n/a" | STATS TOP(`42`, 10, "asc", `n/a`)
 * </pre>
 */
public final class ReplaceAggregateNestedExpressionWithEval extends OptimizerRules.OptimizerRule<Aggregate> {

    private final boolean locallyUniqueNames;
    private final boolean extractConstants;

    /**
     * @param locallyUniqueNames when {@code true}, the synthetic eval names generated for extracted nested expressions are made
     *                           globally unique instead of being derived deterministically from the extracted expression.
     * @param extractConstants   when {@code true}, in addition to nested expressions, also materialize constant input fields
     *                           into the pre-agg eval.
     */
    public ReplaceAggregateNestedExpressionWithEval(boolean locallyUniqueNames, boolean extractConstants) {
        this.locallyUniqueNames = locallyUniqueNames;
        this.extractConstants = extractConstants;
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        List<Alias> evals = new ArrayList<>();
        Map<String, Attribute> evalNames = new HashMap<>();
        Map<GroupingFunction, Attribute> groupingAttributes = new HashMap<>();
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings());
        boolean groupingChanged = false;

        // start with the groupings since the aggs might reuse/reference them
        for (int i = 0, s = newGroupings.size(); i < s; i++) {
            Expression g = newGroupings.get(i);
            if (g instanceof Alias as) {
                Expression asChild = as.child();
                // for non-evaluable grouping functions, replace their nested expressions with attributes and extract the expression out
                // into an eval (added later below)
                if (asChild instanceof GroupingFunction.NonEvaluatableGroupingFunction gf) {
                    Expression newGroupingFunction = transformNonEvaluatableGroupingFunction(gf, evals);
                    if (newGroupingFunction != gf) {
                        groupingChanged = true;
                        newGroupings.set(i, as.replaceChild(newGroupingFunction));
                    }
                } else {
                    // Move the alias into an eval and replace it with its attribute.
                    groupingChanged = true;
                    var attr = as.toAttribute();
                    evals.add(as);
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
                    af -> transformAggregateFunction(af, expToAttribute, evals, counter, aggsChanged)
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

        if (evals.size() > 0) {
            var groupings = groupingChanged ? newGroupings : aggregate.groupings();
            var aggregates = aggsChanged.get() ? newAggs : aggregate.aggregates();

            var newEval = new Eval(aggregate.source(), aggregate.child(), evals);
            aggregate = aggregate.with(newEval, groupings, aggregates);
        }

        return aggregate;
    }

    private Expression transformNonEvaluatableGroupingFunction(GroupingFunction.NonEvaluatableGroupingFunction gf, List<Alias> evals) {
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

    private boolean skipOptimisingAgg(AggregateFunction af) {
        // do not replace nested aggregates
        if (containsAggregate(af.field())
            || af.parameters().stream().anyMatch(ReplaceAggregateNestedExpressionWithEval::containsAggregate)) {
            return true;
        }
        // check if the field or any parameter needs to be extracted into an eval
        return af.fields().stream().noneMatch(field -> needsExtraction(af, field));
    }

    private static boolean containsAggregate(Expression e) {
        return e.anyMatch(child -> child instanceof AggregateFunction);
    }

    /**
     * Whether an aggregate input (field or parameter) must be materialized into a synthetic pre-agg eval.
     */
    private boolean needsExtraction(AggregateFunction af, Expression input) {
        if (input instanceof Attribute) {
            // already a channel, e.g. x in SUM(x), or both x and y in TOP(x, 3, "asc", y)
            return false;
        }
        if (input.foldable() == false) {
            // a nested expression must always be computed first, e.g. 2*x + 1 in SUM(2*x + 1)
            return true;
        }
        if (extractConstants == false) {
            // a constant input is only materialized when extractConstants is enabled
            return false;
        }
        if (af instanceof Count || af instanceof CountApproximate) {
            // COUNT reads a constant field directly, so it needs no channel
            return false;
        }
        return true;
    }

    private Expression transformAggregateFunction(
        AggregateFunction af,
        Map<Expression, Attribute> expToAttribute,
        List<Alias> evals,
        int[] counter,
        Holder<Boolean> aggsChanged
    ) {
        if (skipOptimisingAgg(af)) {
            return af;
        }
        boolean changed = false;
        List<Expression> newFields = new ArrayList<>(af.fields());
        for (int i = 0; i < af.fields().size(); i++) {
            Expression field = af.fields().get(i);
            if (needsExtraction(af, field)) {
                newFields.set(i, extractIntoEval(field, af, expToAttribute, evals, counter));
                changed = true;
            }
        }
        if (changed) {
            aggsChanged.set(true);
            return af.withFields(newFields);
        }
        return af;
    }

    /**
     * Return an attribute referencing an eval that computes {@code expression}, creating (and registering) the synthetic
     * eval if an equivalent one doesn't exist yet. Deduplication is by {@link Expression#canonical()} so that a value used
     * both as a field and as a parameter (or shared across aggregates) is evaluated only once.
     */
    private Attribute extractIntoEval(
        Expression expression,
        AggregateFunction af,
        Map<Expression, Attribute> expToAttribute,
        List<Alias> evals,
        int[] counter
    ) {
        return expToAttribute.computeIfAbsent(expression.canonical(), k -> {
            Alias newAlias = new Alias(k.source(), syntheticName(k, af, counter[0]++), k, null, true);
            evals.add(newAlias);
            return newAlias.toAttribute();
        });
    }

    private String syntheticName(Expression expression, Expression func, int counter) {
        return locallyUniqueNames
            ? TemporaryNameGenerator.locallyUniqueTemporaryName(TemporaryNameGenerator.toString(expression))
            : TemporaryNameGenerator.temporaryName(expression, func, counter);
    }
}
