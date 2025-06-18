/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public final class PushDownAndCombineFilters extends OptimizerRules.OptimizerRule<Filter> {
    @Override
    protected LogicalPlan rule(Filter filter) {
        LogicalPlan plan = filter;
        LogicalPlan child = filter.child();
        Expression condition = filter.condition();

        // TODO: Push down past STATS if the filter is only on the groups; but take into account how `STATS ... BY field` handles
        // multi-values: It seems to be equivalent to `EVAL field = MV_DEDUPE(field) | MV_EXPAND(field) | STATS ... BY field`, where the
        // last `STATS ... BY field` can assume that `field` is single-valued (to be checked more thoroughly).
        // https://github.com/elastic/elasticsearch/issues/115311
        if (child instanceof Filter f) {
            // combine nodes into a single Filter with updated ANDed condition
            plan = f.with(Predicates.combineAnd(List.of(f.condition(), condition)));
        } else if (child instanceof Eval eval) {
            // Don't push if Filter (still) contains references to Eval's fields.
            // Account for simple aliases in the Eval, though - these shouldn't stop us.
            AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
            for (Alias alias : eval.fields()) {
                aliasesBuilder.put(alias.toAttribute(), alias.child());
            }
            AttributeMap<Expression> evalAliases = aliasesBuilder.build();

            Function<Expression, Expression> resolveRenames = expr -> expr.transformDown(ReferenceAttribute.class, r -> {
                Expression resolved = evalAliases.resolve(r, null);
                // Avoid resolving to an intermediate attribute that only lives inside the Eval - only replace if the attribute existed
                // before the Eval.
                if (resolved instanceof Attribute && eval.inputSet().contains(resolved)) {
                    return resolved;
                }
                return r;
            });

            plan = maybePushDownPastUnary(filter, eval, evalAliases::containsKey, resolveRenames);
        } else if (child instanceof RegexExtract re) {
            // Push down filters that do not rely on attributes created by RegexExtract
            var attributes = AttributeSet.of(Expressions.asAttributes(re.extractedFields()));
            plan = maybePushDownPastUnary(filter, re, attributes::contains, NO_OP);
        } else if (child instanceof Completion completion) {
            // Push down filters that do not rely on attributes created by Cpmpletion
            var attributes = AttributeSet.of(completion.generatedAttributes());
            plan = maybePushDownPastUnary(filter, completion, attributes::contains, NO_OP);
        } else if (child instanceof Enrich enrich) {
            // Push down filters that do not rely on attributes created by Enrich
            var attributes = AttributeSet.of(Expressions.asAttributes(enrich.enrichFields()));
            plan = maybePushDownPastUnary(filter, enrich, attributes::contains, NO_OP);
        } else if (child instanceof Project) {
            return PushDownUtils.pushDownPastProject(filter);
        } else if (child instanceof OrderBy orderBy) {
            // swap the filter with its child
            plan = orderBy.replaceChild(filter.with(orderBy.child(), condition));
        } else if (child instanceof Join join && child instanceof InlineJoin == false) {
            // TODO: could we do better here about pushing down filters for inlinestats?
            // See also https://github.com/elastic/elasticsearch/issues/127497
            // Push down past INLINESTATS if the condition is on the groupings
            return pushDownPastJoin(filter, join);
        }
        // cannot push past a Limit, this could change the tailing result set returned
        return plan;
    }

    private record ScopedFilter(List<Expression> commonFilters, List<Expression> leftFilters, List<Expression> rightFilters) {}

    // split the filter condition in 3 parts:
    // 1. filter scoped to the left
    // 2. filter scoped to the right
    // 3. filter that requires both sides to be evaluated
    private static ScopedFilter scopeFilter(List<Expression> filters, LogicalPlan left, LogicalPlan right) {
        List<Expression> rest = new ArrayList<>(filters);
        List<Expression> leftFilters = new ArrayList<>();
        List<Expression> rightFilters = new ArrayList<>();

        AttributeSet leftOutput = left.outputSet();
        AttributeSet rightOutput = right.outputSet();

        // first remove things that are left scoped only
        rest.removeIf(f -> f.references().subsetOf(leftOutput) && leftFilters.add(f));
        // followed by right scoped only
        rest.removeIf(f -> f.references().subsetOf(rightOutput) && rightFilters.add(f));
        return new ScopedFilter(rest, leftFilters, rightFilters);
    }

    private static LogicalPlan pushDownPastJoin(Filter filter, Join join) {
        LogicalPlan plan = filter;
        // pushdown only through LEFT joins
        // TODO: generalize this for other join types
        if (join.config().type() == JoinTypes.LEFT) {
            LogicalPlan left = join.left();
            LogicalPlan right = join.right();

            // split the filter condition in 3 parts:
            // 1. filter scoped to the left
            // 2. filter scoped to the right
            // 3. filter that requires both sides to be evaluated
            ScopedFilter scoped = scopeFilter(Predicates.splitAnd(filter.condition()), left, right);
            // push the left scoped filter down to the left child, keep the rest intact
            if (scoped.leftFilters.size() > 0) {
                // push the filter down to the left child
                left = new Filter(left.source(), left, Predicates.combineAnd(scoped.leftFilters));
                // update the join with the new left child
                join = (Join) join.replaceLeft(left);

                // keep the remaining filters in place, otherwise return the new join;
                Expression remainingFilter = Predicates.combineAnd(CollectionUtils.combine(scoped.commonFilters, scoped.rightFilters));
                plan = remainingFilter != null ? filter.with(join, remainingFilter) : join;
            }
        }
        // ignore the rest of the join
        return plan;
    }

    private static Function<Expression, Expression> NO_OP = expression -> expression;

    private static LogicalPlan maybePushDownPastUnary(
        Filter filter,
        UnaryPlan unary,
        Predicate<Expression> cannotPush,
        Function<Expression, Expression> resolveRenames
    ) {
        LogicalPlan plan;
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : Predicates.splitAnd(filter.condition())) {
            Expression resolvedExp = resolveRenames.apply(exp);
            if (resolvedExp.anyMatch(cannotPush)) {
                // Add the original expression to the non-pushables.
                nonPushable.add(exp);
            } else {
                // When we can push down, we use the resolved expression.
                pushable.add(resolvedExp);
            }
        }
        // Push the filter down even if it might not be pushable all the way to ES eventually: eval'ing it closer to the source,
        // potentially still in the Exec Engine, distributes the computation.
        if (pushable.isEmpty() == false) {
            Filter pushed = filter.with(unary.child(), Predicates.combineAnd(pushable));
            if (nonPushable.isEmpty() == false) {
                plan = filter.with(unary.replaceChild(pushed), Predicates.combineAnd(nonPushable));
            } else {
                plan = unary.replaceChild(pushed);
            }
        } else {
            plan = filter;
        }
        return plan;
    }
}
