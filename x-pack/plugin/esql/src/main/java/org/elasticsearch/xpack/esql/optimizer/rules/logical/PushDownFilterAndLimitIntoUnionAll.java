/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Push down filters that can be evaluated by the {@code UnionAll} branch to each branch, and below {@code Subquery},
 * so that the filters can be pushed down further to the data source when possible. Filters that cannot be pushed down
 * remain above the {@code UnionAll}.
 *
 * This rule applies for certain patterns of {@code UnionAll} branches. The branches of a {@code UnionAll}/{@code Fork} plan has a similar
 * pattern, {@code Fork} adds {@code EsqlProject}, an optional {@code Eval} and an implicit {@code Limit} on top of each branch. However
 * {@code UnionAll} branches do not have the implicit {@code Limit} appended to each branch, this is difference between {@code Fork}
 * and {@code UnionAll}.
 *
 * In case there is mismatched data types on the same field across different {@code UnionAll} branches, a {@code ConvertFunction} could
 * also be added in the optional {@code Eval}.
 *
 * If the patterns of the {@code UnionAll} branches do not match the following expected patterns, the rule is not applied.
 *
 *   Project
 *     Eval (optional) - added when the output of each UnionAll branch are not exactly the same
 *         EsRelation
 * or
 *   Project
 *     Eval (optional)
 *         Subquery
 * or
 *     Subquery - CombineProjections may remove the EsqlProject on top of the subquery
 */
public class PushDownFilterAndLimitIntoUnionAll extends OptimizerRules.ParameterizedOptimizerRule<LogicalPlan, LogicalOptimizerContext> {

    private static final String UNIONALL = "unionall";

    private static final String prefix = Attribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX + UNIONALL + SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR;

    public PushDownFilterAndLimitIntoUnionAll() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan logicalPlan, LogicalOptimizerContext context) {
        // push down filter below UnionAll if possible
        LogicalPlan planWithFilterPushedDownPastUnionAll = logicalPlan.transformDown(
            Filter.class,
            filter -> filter.child() instanceof UnionAll unionAll ? maybePushDownPastUnionAll(filter, unionAll) : filter
        );

        // push down filter below Subquery
        LogicalPlan planWithFilterPushedDownPastSubquery = planWithFilterPushedDownPastUnionAll.transformDown(
            Filter.class,
            PushDownFilterAndLimitIntoUnionAll::pushFilterPastSubquery
        );

        // Append limit to a subquery if:
        // 1. there is knn in the subquery with implicitK, but there is no limit after knn,
        // 2. there is unbounded sort in the subquery
        LogicalPlan planWithImplicitLimitAdded = planWithFilterPushedDownPastSubquery.transformDown(
            UnionAll.class,
            unionAll -> maybeAppendLimitToSubquery(unionAll, context)
        );

        // push down the implicit limit below Subquery, this is mainly to push limit close to sort,
        // so that they can be transformed to TopN later
        return planWithImplicitLimitAdded.transformDown(Limit.class, PushDownFilterAndLimitIntoUnionAll::pushLimitPastSubquery);
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
        // and then the real child, if there is unknown pattern, keep the filter and UnionAll plan unchanged
        List<LogicalPlan> newChildren = new ArrayList<>();
        boolean changed = false;
        for (LogicalPlan child : unionAll.children()) {
            LogicalPlan newChild = child instanceof Project project
                ? maybePushDownFilterPastProjectForUnionAllChild(pushable, project)
                : null;

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
     *           EsRelation
     *      Project
     *        Eval (optional)
     *            Subquery
     *  becomes the following after pushing down the filters that can be evaluated by the UnionAll branches
     *  UnionAll
     *    Filter (pushable predicates)
     *      Project
     *        Eval (optional)
     *          EsRelation
     *    Filter (pushable predicates)
     *      Project
     *        Eval (optional)
     *          Subquery
     *  {@code PushDownAndCombineFilters} will be able to combine the filters pushed down into each branch further,
     *  closer to {@code EsRelation} or {@code Subquery}
     */
    private static LogicalPlan maybePushDownFilterPastProjectForUnionAllChild(List<Expression> pushable, Project project) {
        List<Expression> resolvedPushable = resolvePushableAgainstOutput(pushable, project.projections());
        if (resolvedPushable == null) {
            return project;
        }
        return filterWithPlanAsChild(project, resolvedPushable);
    }

    /**
     * Create a filter on top of the logical plan.
     */
    private static Filter filterWithPlanAsChild(LogicalPlan logicalPlan, List<Expression> predicates) {
        Expression combined = Predicates.combineAnd(predicates);
        return new Filter(logicalPlan.source(), logicalPlan, combined);
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
     * Subquery does not create any new attributes, so filter can be pushed down safely.
     */
    private static LogicalPlan pushFilterPastSubquery(Filter filter) {
        LogicalPlan child = filter.child();
        if (child instanceof Subquery subquery) {
            Filter newFilter = filter.replaceChild(subquery.child());
            return subquery.replaceChild(newFilter);
        }
        return filter;
    }

    /**
     * {@code Knn} requires special handling, as it has an implicitK, it can be set from the limit or optional parameters in the query.
     * implicitK is not serialized, so it is not sent to non-coordinator nodes, therefore we need to push down the limit to the subquery
     * so that Knn can get the limitK from the limit.
     *
     * The input to this method is an {@code UnionAll} branch, check if there is {@code Knn} in the plan, if so collect its implicitK,
     * and append a {@code Limit} to the subquery if there isn't one already.
     *
     * A similar situation happens to {@code Sort} without limit, which means unbounded sort, we also need to append a limit to the subquery
     * to avoid unbounded sort in the subquery.
     */
    private static LogicalPlan maybeAppendLimitToSubquery(UnionAll unionAll, LogicalOptimizerContext context) {
        List<LogicalPlan> oldChildren = unionAll.children();
        List<LogicalPlan> newChildren = new ArrayList<>(oldChildren.size());
        boolean changed = false;
        for (LogicalPlan child : oldChildren) {
            LogicalPlan newChildAfterCheckingKnn = appendLimitIfNeededForKnn(child, context);
            LogicalPlan newChildAfterCheckingOrderBy = appendLimitIfNeededForOrderBy(newChildAfterCheckingKnn, context);
            if (newChildAfterCheckingOrderBy != child) {
                changed = true;
            }
            newChildren.add(newChildAfterCheckingOrderBy);
        }
        return changed ? unionAll.replaceChildren(newChildren) : unionAll;
    }

    private static LogicalPlan appendLimitIfNeededForKnn(LogicalPlan subquery, LogicalOptimizerContext context) {
        Holder<Integer> maxImplicitK = new Holder<>(null);

        boolean foundLimitAfterKnn = subquery.forEachDownMayReturnEarly((plan, hasLimitAfterKnn) -> {
            if (plan instanceof Limit && maxImplicitK.get() == null) { // found a limit before finding knn
                hasLimitAfterKnn.set(true);
                return;
            }

            // haven't found limit yet, look for knn in the plan
            plan.forEachExpression(Knn.class, knn -> {
                Integer k = knn.implicitK();
                if (k != null) {
                    Integer currentMax = maxImplicitK.get();
                    maxImplicitK.set(currentMax == null ? k : Math.max(currentMax, k));
                }
            });
        });

        // there is knn with implicitK and there is no limit after knn, append a limit
        Integer k = maxImplicitK.get();
        if (k != null && foundLimitAfterKnn == false) {
            // check the implicit K against default and maximum implicit limit
            int maxImplicitLimit = context.configuration().resultTruncationMaxSize(false);
            return planWithLimit(subquery, Math.max(k, maxImplicitLimit));
        }
        return subquery;
    }

    private static LogicalPlan appendLimitIfNeededForOrderBy(LogicalPlan subquery, LogicalOptimizerContext context) {
        Holder<OrderBy> unboundedSort = new Holder<>(null);

        boolean foundLimitAfterSort = subquery.forEachDownMayReturnEarly((plan, hasLimitAfterSort) -> {
            if (plan instanceof Limit && unboundedSort.get() == null) { // found a limit before finding sort
                hasLimitAfterSort.set(true);
                return;
            }

            if (unboundedSort.get() != null) {
                return; // already found unbounded sort, return early
            }

            if (plan instanceof OrderBy orderBy) {
                unboundedSort.set(orderBy);
            }
        });

        // there is unbounded sort, append a limit right on top of the sort
        if (unboundedSort.get() != null && foundLimitAfterSort == false) {
            // append a limit with maximum implicit limit
            int maxImplicitLimit = context.configuration().resultTruncationMaxSize(false);
            return planWithLimit(subquery, maxImplicitLimit);
        }

        return subquery;
    }

    private static Limit planWithLimit(LogicalPlan plan, int limitValue) {
        Source source = plan.source();
        return new Limit(source, new Literal(source, limitValue, DataType.INTEGER), plan);
    }

    /**
     * {@code Subquery} does not create any new attributes, so {@code limit} can be pushed down safely.
     */
    private static LogicalPlan pushLimitPastSubquery(Limit limit) {
        LogicalPlan child = limit.child();
        if (child instanceof Subquery subquery) {
            // push limit - added by AddImplicitForkLimit, below subquery
            Limit newLimit = limit.replaceChild(subquery.child());
            return subquery.replaceChild(newLimit);
        }
        return limit;
    }
}
