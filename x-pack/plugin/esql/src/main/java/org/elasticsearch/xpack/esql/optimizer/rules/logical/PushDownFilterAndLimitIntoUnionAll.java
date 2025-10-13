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
 * Push down filters that can be evaluated by the UnionAll child to each child, below the added
 * {@code Limit} and {@code Subquery}, so that the filters can be pushed down further to the
 * data source when possible. Filters that cannot be pushed down remain above the UnionAll.
 *
 * Also push down the {@code Limit} added by {@code AddImplicitForkLimit} below the
 * {@code Subquery}, so that the other rules related to {@code Limit} optimization can be applied.
 */
public final class PushDownFilterAndLimitIntoUnionAll extends Rule<LogicalPlan, LogicalPlan> {

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

    /* Push down filters that can be evaluated by the UnionAll branch to each branch,
     * so that the filters can be pushed down further to the data source when possible.
     * Filters that cannot be pushed down remain above the UnionAll.
     *
     * The children of a UnionAll/Fork plan has a similar pattern, as Fork adds EsqlProject,
     * an optional Eval and Limit on top of its actual children.
     * UnionAll
     *   EsqlProject
     *     Eval (optional) - added by Fork when the output of each UnionAll child are not exactly the same
     *       Limit
     *         EsRelation
     *   EsqlProject
     *     Eval (optional)
     *       Limit
     *         Subquery
     *   Limit   - CombineProjections may remove the EsqlProject on top of the limit
     *       Subquery
     *
     * Push down the filter below limit when possible
     */
    private static LogicalPlan maybePushDownPastUnionAll(Filter filter, UnionAll unionAll) {
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : Predicates.splitAnd(filter.condition())) {
            if (exp.references().subsetOf(unionAll.outputSet())) {
                pushable.add(exp);
            } else {
                nonPushable.add(exp);
            }
        }
        if (pushable.isEmpty()) {
            return filter; // nothing to push down
        }
        // Push the filter down to each child of the UnionAll, the child of a UnionAll is always
        // a project followed by an optional eval and then limit or a limit added by fork and
        // then the real child, if there is unknown pattern, keep the filter and UnionAll plan unchanged
        List<LogicalPlan> newChildren = new ArrayList<>();
        boolean changed = false;
        for (LogicalPlan child : unionAll.children()) {
            LogicalPlan newChild = switch (child) {
                case Project project -> maybePushDownFilterPastProjectForUnionAllChild(pushable, project);
                case Limit limit -> maybePushDownFilterPastLimitForUnionAllChild(pushable, limit);
                default -> null; // TODO add a general push down for unexpected pattern
            };

            if (newChild == null) {
                // Unexpected pattern, keep plan unchanged without pushing down filters
                return filter;
            }

            if (newChild != child) {
                changed = true;
                newChildren.add(newChild);
            } else {
                // Theoretically, all the pushable predicates should be pushed down into each child,
                // in case one child is not changed, preserve the filter on top of UnionAll to make sure
                // correct results are returned and avoid infinite loop of the rule.
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

    private static LogicalPlan maybePushDownFilterPastProjectForUnionAllChild(List<Expression> pushable, Project project) {
        List<Expression> resolvedPushable = resolvePushableAgainstOutput(pushable, project.projections());
        if (resolvedPushable == null) {
            return project;
        }
        LogicalPlan child = project.child();
        if (child instanceof Eval eval) {
            return pushDownFilterPastEvalForUnionAllChild(resolvedPushable, project, eval);
        } else if (child instanceof Limit limit) {
            LogicalPlan newLimit = pushDownFilterPastLimitForUnionAllChild(resolvedPushable, limit);
            return project.replaceChild(newLimit);
        }
        return project;
    }

    private static LogicalPlan maybePushDownFilterPastLimitForUnionAllChild(List<Expression> pushable, Limit limit) {
        List<Expression> resolvedPushable = resolvePushableAgainstOutput(pushable, limit.output());
        if (resolvedPushable == null) {
            return limit;
        }
        return pushDownFilterPastLimitForUnionAllChild(resolvedPushable, limit);
    }

    /**
     * Attempts to resolve all pushable expressions against the given output attributes.
     * Returns a fully resolved list if successful, or null if any expression cannot be resolved.
     */
    private static List<Expression> resolvePushableAgainstOutput(List<Expression> pushable, List<? extends NamedExpression> output) {
        List<Expression> resolved = new ArrayList<>();
        for (Expression exp : pushable) {
            Expression replaced = resolveUnionAllOutputByName(exp, output);
            // Make sure the pushable predicates can find their corresponding attributes in the output
            if (replaced == null || replaced == exp) {
                // cannot find the attribute in the child project, cannot push down this filter
                return null;
            }
            resolved.add(replaced);
        }
        // If some pushable predicates cannot be resolved against the output, cannot push filter down.
        // This should not happen, however we need to be cautious here, if the predicate is removed from
        // the main query, and it is not pushed down into the UnionAll child, the result will be incorrect.
        return resolved.size() == pushable.size() ? resolved : null;
    }

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
                : withFilter(project, eval, nonPushables); // Push down filter references eval created attributes below project, above eval
        }

        // Push down all pushable predicates below eval and limit
        if (evalChild instanceof Limit limit) {
            LogicalPlan newLimit = pushDownFilterPastLimitForUnionAllChild(pushables, limit);
            LogicalPlan newEval = eval.replaceChild(newLimit);

            return nonPushables.isEmpty() ? project.replaceChild(newEval) : withFilter(project, newEval, nonPushables);
        }

        return project;
    }

    private static LogicalPlan withFilter(Project project, LogicalPlan child, List<Expression> predicates) {
        Expression combined = Predicates.combineAnd(predicates);
        return project.replaceChild(new Filter(project.source(), child, combined));
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

    private static AttributeMap<Expression> buildEvaAliases(Eval eval) {
        AttributeMap.Builder<Expression> builder = AttributeMap.builder();
        for (Alias alias : eval.fields()) {
            builder.put(alias.toAttribute(), alias.child());
        }
        return builder.build();
    }

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
     * The UnionAll/Fork outputs have the same names as it children's outputs, however they have different ids.
     * Convert the pushable predicates to use the child's attributes, so that they can be pushed down further.
     */
    private static Expression resolveUnionAllOutputByName(Expression expr, List<? extends NamedExpression> namedExpressions) {
        // A temporary expression is created with temporary attributes names, as sometimes transform expression does not transform
        // one ReferenceAttribute to another ReferenceAttribute with the same name, different id successfully.
        String UNIONALL = "unionall";
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

        String prefix = Attribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX + UNIONALL + SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR;
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
