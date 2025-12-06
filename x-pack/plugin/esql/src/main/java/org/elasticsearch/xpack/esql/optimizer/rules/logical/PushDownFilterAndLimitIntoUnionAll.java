/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Push down filters that can be evaluated by the {@code UnionAll} branch to each branch, below the added {@code Limit} and
 * {@code Subquery}, so that the filters can be pushed down further to the data source when possible. Filters that cannot be pushed down
 * remain above the {@code UnionAll}.
 *
 * Also push down the {@code Limit} added by {@code AddImplicitForkLimit} below the {@code Subquery}, so that the other rules related
 * to {@code Limit} optimization can be applied.
 *
 * This rule applies for certain patterns of {@code UnionAll} branches. The branches of a {@code UnionAll}/{@code Fork} plan has a similar
 * pattern, as {@code Fork} adds {@code EsqlProject}, an optional {@code Eval} and {@code Limit} on top of its actual children. In case
 * there is mismatched data types on the same field across different {@code UnionAll} branches, a {@code ConvertFunction} could also be
 * added in the optional {@code Eval}.
 *
 * If the patterns of the {@code UnionAll} branches do not match the following expected patterns, the rule is not applied.
 *
 *   EsqlProject
 *     Eval (optional) - added when the output of each UnionAll branch are not exactly the same
 *       Limit
 *         EsRelation
 * or
 *   EsqlProject
 *     Eval (optional)
 *       Limit
 *         Subquery
 * or
 *   Limit   - CombineProjections may remove the EsqlProject on top of the limit
 *     Subquery
 */
public final class PushDownFilterAndLimitIntoUnionAll extends Rule<LogicalPlan, LogicalPlan> {

    private static final String UNIONALL = "unionall";

    private static final String prefix = Attribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX + UNIONALL + SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR;

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        // push down filter below UnionAll if possible
        LogicalPlan planWithFilterPushedDownPastUnionAll = logicalPlan.transformDown(
            Filter.class,
            filter -> filter.child() instanceof UnionAll unionAll ? maybePushDownPastUnionAll(filter, unionAll) : filter
        );
        // push down limit or limit + filter below Subquery
        return planWithFilterPushedDownPastUnionAll.transformDown(
            Limit.class,
            PushDownFilterAndLimitIntoUnionAll::pushLimitAndFilterPastSubquery
        );
    }

    private static LogicalPlan maybePushDownPastUnionAll(Filter filter, UnionAll unionAll) {
        AttributeSet unionAllOutputSet = unionAll.outputSet();
        // check ReferenceAttribute name and id to make sure it is from the UnionAll output
        Tuple<List<Expression>, List<Expression>> pushablesAndNonPushables = splitPushableAndNonPushablePredicates(
            Predicates.splitAnd(filter.condition()),
            exp -> isSubset(exp.references(), unionAllOutputSet) == false
        );
        List<Expression> pushable = pushablesAndNonPushables.v1();
        List<Expression> nonPushable = pushablesAndNonPushables.v2();

        if (pushable.isEmpty()) {
            return filter; // nothing to push down
        }
        // Push the filter down to each child of the UnionAll, the child of a UnionAll is always a project followed by an optional eval
        // and then limit or a limit added by fork and then the real child, if there is unknown pattern, keep the filter and UnionAll plan
        // unchanged
        List<LogicalPlan> newChildren = new ArrayList<>();
        boolean changed = false;
        for (LogicalPlan child : unionAll.children()) {
            LogicalPlan newChild = switch (child) {
                case Project project -> maybePushDownFilterPastProjectForUnionAllChild(pushable, project);
                case Limit limit -> maybePushDownFilterPastLimitForUnionAllChild(pushable, limit);
                default -> null; // TODO may add a general push down for unexpected pattern
            };

            if (newChild == null) {
                // Unexpected pattern, keep plan unchanged without pushing down filters
                return filter;
            }

            if (newChild != child) {
                changed = true;
                newChildren.add(newChild);
            } else {
                // Theoretically, all the pushable predicates should be pushed down into each child, in case one child is not changed
                // it is because the plan pattern is not as expected, preserve the filter on top of UnionAll to make sure correct results
                // are returned and avoid infinite loop of the rule.
                return filter;
            }
        }

        if (changed == false) { // nothing changed, return the original plan
            return filter;
        }

        LogicalPlan newUnionAll = unionAll.replaceChildren(newChildren);
        if (nonPushable.isEmpty()) {
            return newUnionAll;
        } else {
            return filter.with(newUnionAll, Predicates.combineAnd(nonPushable));
        }
    }

    /**
     * Handle UnionAll branch pattern below, if the pattern does not match, the plan is returned unchanged
     * Filter (pushable predicates)
     *   UnionAll
     *     Project
     *       Eval (optional)
     *         Limit
     *           EsRelation
     *      Project
     *        Eval (optional)
     *          Limit
     *            Subquery
     *  becomes the following after pushing down the filters that can be evaluated by the UnionAll branches
     *  UnionAll
     *    Project
     *      Eval (optional)
     *        Limit
     *          Filter (pushable predicates)
     *            EsRelation
     *     Project
     *       Eval (optional)
     *         Limit
     *           Filter (pushable predicates)
     *             Subquery
     */
    private static LogicalPlan maybePushDownFilterPastProjectForUnionAllChild(List<Expression> pushable, Project project) {
        List<Expression> resolvedPushable = resolvePushableAgainstOutput(pushable, project.projections());
        if (resolvedPushable == null) {
            return project;
        }
        LogicalPlan child = project.child();
        // check if the predicates' attributes' name and id are in the child's output, if so push down
        Tuple<List<Expression>, List<Expression>> pushablesAndNonPushables = splitPushableAndNonPushablePredicates(
            resolvedPushable,
            exp -> isSubset(exp.references(), child.outputSet()) == false
        );
        List<Expression> newResolvedPushable = pushablesAndNonPushables.v1();
        List<Expression> newResolvedNonPushable = pushablesAndNonPushables.v2();

        // nothing to push down
        if (newResolvedPushable.isEmpty()) {
            return newResolvedNonPushable.isEmpty() ? project : filterWithPlanAsChild(project, newResolvedNonPushable);
        }

        LogicalPlan planWithNewResolvedPushablePushedDown = project;
        if (child instanceof Eval eval) {
            planWithNewResolvedPushablePushedDown = pushDownFilterPastEvalForUnionAllChild(newResolvedPushable, project, eval);
        } else if (child instanceof Limit limit) {
            LogicalPlan newLimit = pushDownFilterPastLimitForUnionAllChild(newResolvedPushable, limit);
            planWithNewResolvedPushablePushedDown = project.replaceChild(newLimit);
        }

        if (planWithNewResolvedPushablePushedDown == project) {
            // There are pushable predicates, but they cannot be pushed down because unexpected pattern is found below project,
            // predicates cannot be push down, otherwise it may cause wrong results.
            return project;
        }

        return newResolvedNonPushable.isEmpty()
            ? planWithNewResolvedPushablePushedDown
            : filterWithPlanAsChild(planWithNewResolvedPushablePushedDown, newResolvedNonPushable); // create a filter above the new plan
    }

    /**
     * Handle UnionAll branch pattern, if the pattern does not match, the plan is returned unchanged
     * Filter (pushable predicates)
     *   UnionAll
     *     Limit
     *       Subquery
     * Becomes the following after pushing down the filters that can be evaluated by the UnionAll branches
     * UnionAll
     *   Limit
     *     Filter (pushable predicates)
     *       Subquery
     */
    private static LogicalPlan maybePushDownFilterPastLimitForUnionAllChild(List<Expression> pushable, Limit limit) {
        List<Expression> resolvedPushable = resolvePushableAgainstOutput(pushable, limit.output());
        if (resolvedPushable == null) {
            return limit;
        }
        return pushDownFilterPastLimitForUnionAllChild(resolvedPushable, limit);
    }

    /**
     * Handle UnionAll branch pattern
     * Project
     *   Eval (optional)
     *     Limit
     *       EsRelation
     *  or
     *  Project
     *    Eval (optional)
     *      Limit
     *        Subquery
     */
    private static LogicalPlan pushDownFilterPastEvalForUnionAllChild(List<Expression> pushable, Project project, Eval eval) {
        // if the pushable references any attribute created by the eval, we cannot push down
        AttributeMap<Expression> evalAliases = buildEvaAliases(eval);
        Tuple<List<Expression>, List<Expression>> pushablesAndNonPushables = splitPushableAndNonPushablePredicates(
            pushable,
            exp -> exp.references().stream().anyMatch(evalAliases::containsKey)
        );
        List<Expression> pushables = pushablesAndNonPushables.v1();
        List<Expression> nonPushables = pushablesAndNonPushables.v2();

        LogicalPlan evalChild = eval.child();

        // Nothing to push down under eval and limit
        if (pushables.isEmpty()) {
            return nonPushables.isEmpty()
                ? project // nothing at all
                : projectWithFilterAsChild(project, eval, nonPushables);
        }

        // Push down all pushable predicates below eval and limit
        if (evalChild instanceof Limit limit) {
            LogicalPlan newLimit = pushDownFilterPastLimitForUnionAllChild(pushables, limit);
            LogicalPlan newEval = eval.replaceChild(newLimit);

            return nonPushables.isEmpty() ? project.replaceChild(newEval) : projectWithFilterAsChild(project, newEval, nonPushables);
        }

        return project;
    }

    /**
     * Push down the filter below project.
     */
    private static LogicalPlan projectWithFilterAsChild(Project project, LogicalPlan child, List<Expression> predicates) {
        Expression combined = Predicates.combineAnd(predicates);
        return project.replaceChild(new Filter(project.source(), child, combined));
    }

    /**
     * Create a filter on top of the logical plan.
     */
    private static Filter filterWithPlanAsChild(LogicalPlan logicalPlan, List<Expression> predicates) {
        Expression combined = Predicates.combineAnd(predicates);
        return new Filter(logicalPlan.source(), logicalPlan, combined);
    }

    /**
     * limit does not create any new attributes, so we should push down all pushable predicates,
     * the caller should make sure the pushable is really pushable.
     */
    private static LogicalPlan pushDownFilterPastLimitForUnionAllChild(List<Expression> pushable, Limit limit) {
        if (pushable.isEmpty()) {
            return limit;
        }
        Expression combined = Predicates.combineAnd(pushable);
        Filter pushed = new Filter(limit.source(), limit.child(), combined);
        return limit.replaceChild(pushed);
    }

    /**
     * Check if all attributes in subset are also in superset by checking their names and ids.
     */
    private static boolean isSubset(AttributeSet subset, AttributeSet superset) {
        return subset.stream()
            .allMatch(
                attr -> superset.stream().anyMatch(superAttr -> superAttr.name().equals(attr.name()) && superAttr.id().equals(attr.id()))
            );
    }

    /**
     * Build a map of eval aliases to their corresponding expressions.
     */
    private static AttributeMap<Expression> buildEvaAliases(Eval eval) {
        AttributeMap.Builder<Expression> builder = AttributeMap.builder();
        for (Alias alias : eval.fields()) {
            builder.put(alias.toAttribute(), alias.child());
        }
        return builder.build();
    }

    /**
     * Split the predicates into pushable and non-pushable based on the given check.
     */
    private static Tuple<List<Expression>, List<Expression>> splitPushableAndNonPushablePredicates(
        List<Expression> predicates,
        Predicate<Expression> nonPushableCheck
    ) {
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : predicates) {
            if (nonPushableCheck.test(exp)) {
                nonPushable.add(exp);
            } else {
                pushable.add(exp);
            }
        }
        return Tuple.tuple(pushable, nonPushable);
    }

    /**
     * Resolve the pushable predicates against the output of UnionAll, so that the attributes in the predicates can be matched by
     * the attributes in the UnionAll child. If the pushable predicates have no references, they are considered pushable as is,
     * for example some full text functions like QSTR and KQL do not reference any field or reference attribute.
     */
    private static List<Expression> resolvePushableAgainstOutput(List<Expression> pushable, List<? extends NamedExpression> output) {
        List<Expression> resolved = new ArrayList<>();
        for (Expression exp : pushable) {
            // Some full text functions may not have references like QSTR and KQL, if not, it is pushable as is.
            // Limiting the check to full text function is not enough, as there could be a full text function could be under a Not
            if (exp.references().isEmpty()) {
                resolved.add(exp);
                continue;
            }
            Expression resolvedExp = resolveUnionAllOutputByName(exp, output);
            // Make sure the pushable predicates can find their corresponding attributes in the output,
            // if there is any predicate that cannot be resolved, return null to indicate the whole filter push down cannot be done.
            if (resolvedExp == null || resolvedExp == exp) {
                // cannot find the attribute in the child project, cannot push down this filter
                return null;
            }
            resolved.add(resolvedExp);
        }
        // If some pushable predicates cannot be resolved against the output, cannot push filter down.
        // This should not happen, however we need to be cautious here, if the predicate is removed from
        // the main query, and it is not pushed down into the UnionAll child, the result will be incorrect.
        return resolved.size() == pushable.size() ? resolved : null;
    }

    /**
     * The UnionAll/Fork outputs have the same names as it children's outputs, however they have different ids.
     * Convert the pushable predicates to use the child's attributes, so that they can be pushed down further.
     */
    private static Expression resolveUnionAllOutputByName(Expression expr, List<? extends NamedExpression> namedExpressions) {
        // A temporary expression is created with temporary attributes names, as sometimes transform expression does not transform
        // one ReferenceAttribute to another ReferenceAttribute with the same name, different id successfully.
        // rename the output of the UnionAll to a temporary name with a prefix
        Expression renamed = expr.transformUp(Attribute.class, attr -> {
            for (NamedExpression ne : namedExpressions) {
                if (ne.name().equals(attr.name())) {
                    // $$subquery$attr.name()
                    return attr.withName(rawTemporaryName(UNIONALL, ne.name()));
                }
            }
            return attr;
        });

        return renamed.transformUp(Attribute.class, attr -> {
            String originalName = attr.name().startsWith(prefix) ? attr.name().substring(prefix.length()) : attr.name();
            for (NamedExpression ne : namedExpressions) {
                if (ne.name().equals(originalName)) {
                    return ne.toAttribute();
                }
            }
            return attr;
        });
    }

    /**
     * Subquery does not create any new attributes, so  limit or limit + filter can be pushed down safely.
     */
    private static LogicalPlan pushLimitAndFilterPastSubquery(Limit limit) {
        LogicalPlan child = limit.child();
        if (child instanceof Subquery subquery) {
            // push limit - added by AddImplicitForkLimit, below subquery
            Limit newLimit = limit.replaceChild(subquery.child());
            return subquery.replaceChild(newLimit);
        }
        if (child instanceof Filter filter && filter.child() instanceof Subquery subquery) {
            // push down both limit - added by AddImplicitForkLimit and filter below subquery
            Filter newFilter = filter.replaceChild(subquery.child());
            Limit newLimit = limit.replaceChild(newFilter);
            return subquery.replaceChild(newLimit);
        }
        return limit;
    }
}
