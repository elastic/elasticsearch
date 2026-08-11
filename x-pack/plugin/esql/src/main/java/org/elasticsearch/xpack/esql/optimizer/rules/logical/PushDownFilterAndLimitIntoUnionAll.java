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
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;

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
        // Push the filter down to each child of the UnionAll.
        // Supported branch shapes:
        // • Project (> Eval?) > {EsRelation | Subquery} — subquery-shape from FORK
        // • EsRelation or ExternalRelation — direct-leaf shape from heterogeneous FROM
        // If any branch has an unrecognised shape or cannot resolve the predicate, leave the filter
        // above the UnionAll unchanged.
        List<LogicalPlan> newChildren = new ArrayList<>();
        boolean changed = false;
        for (LogicalPlan child : unionAll.children()) {
            LogicalPlan newChild;
            if (child instanceof Project project) {
                newChild = maybePushDownFilterPastProjectForUnionAllChild(pushable, project);
            } else if (child instanceof EsRelation || child instanceof ExternalRelation) {
                newChild = maybePushDownFilterPastLeafForUnionAllChild(pushable, child);
            } else {
                // Unexpected pattern, keep plan unchanged without pushing down filters
                return filter;
            }

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
     * Handle a direct-leaf UnionAll branch ({@link EsRelation} or {@link ExternalRelation}).
     * Resolves the pushable predicates by name against the leaf's output and wraps the leaf in a
     * new {@link Filter}. Returns the original {@code leaf} unchanged if any predicate cannot be
     * resolved (caller treats this as "cannot push", keeping the filter above the UnionAll).
     */
    private static LogicalPlan maybePushDownFilterPastLeafForUnionAllChild(List<Expression> pushable, LogicalPlan leaf) {
        List<Expression> resolved = resolvePushableAgainstOutput(pushable, leaf.output());
        if (resolved == null) {
            return leaf;
        }
        return filterWithPlanAsChild(leaf, resolved);
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

    /**
     * Bounds an unbounded {@code Knn} in one {@code UnionAll} branch by appending a {@code Limit} at the branch root, or returns the
     * branch unchanged if every {@code Knn} in it is already bounded.
     *
     * <p>A {@code Knn} carries an {@code implicitK} - how many nearest neighbours to fetch - which {@code PushLimitToKnn} derives from
     * the nearest enclosing {@code Limit}. {@code implicitK} is not serialized, so it does not survive the trip to a remote node; the
     * appended {@code Limit} is what lets {@code PushLimitToKnn} re-derive it there. Without one the {@code Knn} arrives with no k and
     * {@code Knn.postOptimizationVerification} rejects the query.
     *
     * <p>The per-path search matches {@link #appendLimitIfNeededForOrderBy}: a {@code Limit} bounds everything below it, a nested
     * {@code UnionAll} ends our concern, and an {@link AbstractSubqueryJoin} is walked on the left only. Unlike that method the first
     * match on a path is not enough, because the appended limit has to cover the largest k any unbounded {@code Knn} asked for, so
     * {@link #maxUnboundedImplicitK} accumulates rather than stopping.
     *
     * <p>{@link #maybeAppendLimitToSubquery} calls this once per branch. In
     * {@code FROM colors, (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800")) METADATA _score | LIMIT 5} only the second
     * branch holds a {@code Knn}, so only it is rewritten. Its {@code implicitK} is 5, taken from the outer {@code LIMIT}:
     * <pre>
     * Project
     *   Subquery
     *     Filter[KNN(rgb_vector, ..., implicitK=5)]
     *       EsRelation[colors]
     * </pre>
     * becomes, with a limit of {@code max(5, resultTruncationMaxSize)}:
     * <pre>
     * Limit[10000]
     *   Project
     *     Subquery
     *       Filter[KNN(rgb_vector, ..., implicitK=5)]
     *         EsRelation[colors]
     * </pre>
     * The limit pushdown rules then walk it under the {@code Subquery} and {@code PushLimitToKnn} re-reads it on the next pass, so the
     * optimized branch ends up as:
     * <pre>
     * Project
     *   Subquery
     *     Limit[10000]
     *       Filter[KNN(rgb_vector, ..., implicitK=10000)]
     *         EsRelation[colors]
     * </pre>
     *
     * <p>With nesting, a {@code Knn} written at an outer level does not stay there. In
     * {@code FROM (FROM (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800")), (FROM colors METADATA _score | WHERE
     * knn(rgb_vector, "0000ff")) METADATA _score | WHERE knn(rgb_vector, "ff0000")), (FROM colors) METADATA _score | LIMIT 5} there is a
     * {@code knn} at the middle level and one in each innermost subquery, and the optimized plan is:
     * <pre>
     * Limit[5]
     *   UnionAll                                                                            (outer)
     *     Project
     *       Subquery
     *         UnionAll                                                                      (inner)
     *           Project
     *             Subquery
     *               Limit[10000]
     *                 Filter[KNN(rgb_vector, [0.0, 120.0, 0.0]) AND KNN(rgb_vector, [-1.0, 0.0, 0.0])]
     *                   EsRelation[colors]
     *           Project
     *             Subquery
     *               Limit[10000]
     *                 Filter[KNN(rgb_vector, [0.0, 0.0, -1.0]) AND KNN(rgb_vector, [-1.0, 0.0, 0.0])]
     *                   EsRelation[colors]
     *     Project
     *       Eval
     *         Subquery
     *           EsRelation[colors]
     * </pre>
     * The middle level's {@code knn} is pushed into both inner branches by {@link #maybePushDownPastUnionAll} before this method runs -
     * they are earlier steps of the same {@link #rule} invocation - and {@code PushDownAndCombineFilters} merges it with each branch's
     * own {@code knn} into one {@code Filter}. So by the time this runs both {@code Knn}s on a path sit in a single node,
     * {@link #maxImplicitK} takes the larger of the two, and one {@code Limit} bounds both; {@code PushLimitToKnn} then re-derives the
     * same k for each of them from that limit.
     *
     * <p>Nothing is appended to the outer union's first branch: the search stops at the inner {@code UnionAll} before reaching any
     * {@code Knn}, and the inner branches are bounded when the enclosing transformDown reaches that inner {@code UnionAll}. The outer
     * union's second branch holds no {@code Knn} and is left alone.
     *
     * <p>A branch that bounds its own {@code Knn} - {@code (FROM colors METADATA _score | WHERE knn(rgb_vector, "007800") | LIMIT 7)} -
     * is returned unchanged, because the search stops at that {@code Limit} before reaching the {@code Filter}, and k stays 7:
     * <pre>
     * Project
     *   Subquery
     *     Limit[7]
     *       Filter[KNN(rgb_vector, ..., implicitK=7)]
     *         EsRelation[colors]
     * </pre>
     */
    private static LogicalPlan appendLimitIfNeededForKnn(LogicalPlan subquery, LogicalOptimizerContext context) {
        Integer k = maxUnboundedImplicitK(subquery);

        if (k != null) {
            // Raise k to the maximum implicit limit when it is lower, so the appended limit never truncates the branch more than an
            // unbounded one would have been truncated anyway.
            int maxImplicitLimit = context.configuration().resultTruncationMaxSize(false);
            return planWithLimit(subquery, Math.max(k, maxImplicitLimit));
        }
        return subquery;
    }

    /**
     * The largest {@code implicitK} of any {@code Knn} reachable from {@code plan} without crossing a {@code Limit} - which already
     * bounds everything below it - or a nested {@code UnionAll} - whose own branches are bounded when the enclosing transformDown
     * reaches them; {@code null} if there is none. An {@link AbstractSubqueryJoin} is walked on the left only: see
     * {@link #limitSearchChildren}.
     *
     * Each path is cut independently, so one branch's {@code Limit} cannot suppress another branch's {@code Knn}, which would make the
     * outcome depend on the order the branches happen to be written in.
     */
    private static Integer maxUnboundedImplicitK(LogicalPlan plan) {
        if (plan instanceof Limit || plan instanceof UnionAll) {
            return null;
        }
        Integer max = maxImplicitK(plan);
        for (LogicalPlan child : limitSearchChildren(plan)) {
            Integer childMax = maxUnboundedImplicitK(child);
            if (childMax != null) {
                max = max == null ? childMax : Math.max(max, childMax);
            }
        }
        return max;
    }

    /**
     * The largest {@code implicitK} of any {@code Knn} in this node's own expressions, or {@code null} if it holds none. Only the node
     * is inspected, not its children, so callers control the traversal.
     */
    private static Integer maxImplicitK(LogicalPlan plan) {
        Holder<Integer> maxImplicitK = new Holder<>(null);
        plan.forEachExpression(Knn.class, knn -> {
            Integer k = knn.implicitK();
            if (k != null) {
                Integer currentMax = maxImplicitK.get();
                maxImplicitK.set(currentMax == null ? k : Math.max(currentMax, k));
            }
        });
        return maxImplicitK.get();
    }

    /**
     * Bounds an unbounded {@code SORT} in one {@code UnionAll} branch by appending a {@code Limit} at the branch root, or returns the
     * branch unchanged if every sort in it is already bounded.
     *
     * <p>For each path down the branch, the search finds the first node that settles the question: a {@code Limit} already bounds
     * whatever is below it, an {@code OrderBy} below no {@code Limit} is the unbounded sort we have to bound, and a nested
     * {@code UnionAll} ends our concern - its own branches are handled when the enclosing transformDown reaches it. An
     * {@link AbstractSubqueryJoin} is walked on the left only: see {@link #limitSearchChildren}.
     *
     * <p>Each path is cut independently. Walking the whole subtree with one shared "found a limit" flag would let an unrelated branch
     * of a nested union abort the search, making the outcome depend on the order the branches happen to be written in.
     *
     * <p>Existence is all this needs - the appended limit is always {@code resultTruncationMaxSize}, with no per-node value to combine -
     * so stopping at the first match on a path loses nothing. Once an {@code OrderBy} is reached without crossing a {@code Limit} the
     * answer is settled, and a deeper sort cannot change it. That is what separates this from {@link #appendLimitIfNeededForKnn}, which
     * takes a maximum and so cannot stop early.
     *
     * <p>{@link #maybeAppendLimitToSubquery} calls this once per branch, so in
     * {@code FROM (FROM test | SORT last_name), (FROM languages | SORT language_name)} - where both branches carry an unbounded sort -
     * both are rewritten, independently and identically. Taking the first:
     * <pre>
     * Project
     *   Eval
     *     Subquery
     *       OrderBy[last_name ASC]
     *         EsRelation[test]
     * </pre>
     * becomes:
     * <pre>
     * Limit[10000]
     *   Project
     *     Eval
     *       Subquery
     *         OrderBy[last_name ASC]
     *           EsRelation[test]
     * </pre>
     * The limit goes at the branch root rather than directly on the sort so that the existing pushdown rules can walk it past
     * {@code Project}/{@code Eval} and under the {@code Subquery}, where {@code ReplaceLimitAndSortAsTopN} fuses the two. Both branches
     * end up bounded the same way, under the query's implicit default limit:
     * <pre>
     * Limit[1000]
     *   UnionAll
     *     Project
     *       Eval
     *         Subquery
     *           TopN[[Order[last_name, ASC]], 10000]
     *             EsRelation[test]
     *     Project
     *       Eval
     *         Subquery
     *           TopN[[Order[language_name, ASC]], 10000]
     *             EsRelation[languages]
     * </pre>
     *
     * <p>With nesting, each level is bounded by its own invocation. In
     * {@code FROM (FROM (FROM test | SORT last_name), (FROM test | SORT first_name) | SORT emp_no), (FROM languages | SORT
     * language_name)} there is an unbounded sort at all three levels, and the optimized plan carries three separate limits:
     * <pre>
     * Limit[1000]
     *   UnionAll                                              (outer)
     *     Project
     *       Eval
     *         Subquery
     *           TopN[[Order[emp_no, ASC]], 10000]             (the middle level's own sort)
     *             UnionAll                                    (inner)
     *               Project
     *                 Subquery
     *                   TopN[[Order[last_name, ASC]], 10000]
     *                     EsRelation[test]
     *               Project
     *                 Subquery
     *                   TopN[[Order[first_name, ASC]], 10000]
     *                     EsRelation[test]
     *     Project
     *       Eval
     *         Subquery
     *           TopN[[Order[language_name, ASC]], 10000]
     *             EsRelation[languages]
     * </pre>
     * Called on the outer union's first branch, the search reaches {@code OrderBy[emp_no]} before the inner {@code UnionAll} and bounds
     * that branch. It does not descend past the inner {@code UnionAll}, so at that point the two innermost sorts are still unbounded;
     * they get their own limits when the enclosing transformDown reaches the inner {@code UnionAll} and this is called once per inner
     * branch. Every one of the three limits is {@code resultTruncationMaxSize} - the value does not shrink with depth.
     *
     * <p>A branch that bounds its own sort - {@code (FROM test | SORT last_name | LIMIT 5)} - is returned unchanged, because the search
     * stops at that {@code Limit} before reaching the {@code OrderBy}.
     */
    private static LogicalPlan appendLimitIfNeededForOrderBy(LogicalPlan subquery, LogicalOptimizerContext context) {
        if (hasUnboundedSort(subquery)) {
            int maxImplicitLimit = context.configuration().resultTruncationMaxSize(false);
            return planWithLimit(subquery, maxImplicitLimit);
        }

        return subquery;
    }

    /**
     * Whether {@code plan} contains an {@code OrderBy} that is not already under a {@code Limit}, ignoring nested {@code UnionAll}s
     * (handled by a later transformDown) and the right side of an {@link AbstractSubqueryJoin} (see {@link #limitSearchChildren}).
     */
    private static boolean hasUnboundedSort(LogicalPlan plan) {
        if (plan instanceof Limit || plan instanceof UnionAll) {
            return false;
        }
        if (plan instanceof OrderBy) {
            return true;
        }
        for (LogicalPlan child : limitSearchChildren(plan)) {
            if (hasUnboundedSort(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Children of {@code plan} that this rule may search for a {@code Knn} or unbounded {@code SORT} to bound with a {@code Limit}
     * at the union-branch root. Skip the RHS of an {@link AbstractSubqueryJoin} because the RHS is an independently executed subquery.
     */
    private static List<LogicalPlan> limitSearchChildren(LogicalPlan plan) {
        if (plan instanceof AbstractSubqueryJoin join) {
            return List.of(join.left());
        }
        return plan.children();
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
