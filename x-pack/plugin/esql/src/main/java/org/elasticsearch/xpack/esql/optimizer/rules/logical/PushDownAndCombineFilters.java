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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.SYNTHETIC_ATTRIBUTE_NAME_SEPARATOR;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Perform filters as early as possible in the logical plan by pushing them past certain plan nodes (like {@link Eval},
 * {@link RegexExtract}, {@link Enrich}, {@link Project}, {@link OrderBy} and left {@link Join}s) where possible.
 * Ideally, the filter ends up all the way down at the data source and can be turned into a Lucene query.
 * When pushing down past nodes, only conditions that do not depend on fields created by those nodes are pushed down; if the condition
 * consists of {@code AND}s, we split out the parts that do not depend on the previous node.
 * For joins, it splits the filter condition into parts that can be applied to the left or right side of the join and only pushes down
 * the left hand side filters to the left child.
 *
 * Also combines adjacent filters using a logical {@code AND}.
 */
public final class PushDownAndCombineFilters extends OptimizerRules.ParameterizedOptimizerRule<Filter, LogicalOptimizerContext> {

    public PushDownAndCombineFilters() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Filter filter, LogicalOptimizerContext ctx) {
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
        } else if (child instanceof InferencePlan<?> inferencePlan) {
            // Push down filters that do not rely on attributes created by Completion
            var attributes = AttributeSet.of(inferencePlan.generatedAttributes());
            plan = maybePushDownPastUnary(filter, inferencePlan, attributes::contains, NO_OP);
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
            // TODO: could we do better here about pushing down filters for inline stats?
            // See also https://github.com/elastic/elasticsearch/issues/127497
            // Push down past INLINE STATS if the condition is on the groupings
            return pushDownPastJoin(filter, join, ctx.foldCtx());
        } else if (child instanceof UnionAll unionAll) {
            // Push down filters that can be evaluated using only the output of the UnionAll
            plan = maybePushDownPastUnionAll(filter, unionAll);
        } else if (child instanceof Subquery subquery) {
            // subquery is a placeholder, push down the filter to the child of the subquery
            plan = subquery.replaceChild(new Filter(filter.source(), subquery.child(), filter.condition()));
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

    private static LogicalPlan pushDownPastJoin(Filter filter, Join join, FoldContext foldCtx) {
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
            boolean optimizationApplied = false;
            // push the left scoped filter down to the left child
            if (scoped.leftFilters.size() > 0) {
                // push the filter down to the left child
                left = new Filter(left.source(), left, Predicates.combineAnd(scoped.leftFilters));
                // update the join with the new left child
                join = (Join) join.replaceLeft(left);
                // we completely applied the left filters, so we can remove them from the scoped filters
                scoped = new ScopedFilter(scoped.commonFilters(), List.of(), scoped.rightFilters);
                optimizationApplied = true;
            }
            // push the right scoped filter down to the right child
            // We check if each AND component of the filter is already part of the right side filter before we add it
            // In the future, this optimization can apply to other types of joins as well such as InlineJoin
            // but for now we limit it to LEFT joins only, till filters are supported for other join types
            if (scoped.rightFilters().isEmpty() == false) {
                List<Expression> rightPushableFilters = buildRightPushableFilters(scoped.rightFilters(), foldCtx);
                if (rightPushableFilters.isEmpty() == false) {
                    if (join.right() instanceof Filter existingRightFilter) {
                        // merge the unique AND filter components from rightPushableFilters and existingRightFilter.condition()

                        List<Expression> existingFilters = new ArrayList<>(Predicates.splitAnd(existingRightFilter.condition()));
                        int sizeBefore = existingFilters.size();
                        rightPushableFilters.stream().filter(e -> existingFilters.contains(e) == false).forEach(existingFilters::add);
                        if (sizeBefore != existingFilters.size()) {
                            right = existingRightFilter.with(Predicates.combineAnd(existingFilters));
                            join = (Join) join.replaceRight(right);
                            optimizationApplied = true;
                        } // else nothing needs to be updated
                    } else {
                        // create a new filter on top of the right child
                        right = new Filter(right.source(), right, Predicates.combineAnd(rightPushableFilters));
                        // update the join with the new right child
                        join = (Join) join.replaceRight(right);
                        optimizationApplied = true;
                    }
                }
                /*
                We still want to reapply the filters that we just applied to the right child,
                so we do NOT update scoped, and we do NOT mark optimizationApplied as true.
                This is because by pushing them on the right side, we filter what rows we get from the right side
                But we do not limit the output rows of the join as the rows are kept as not matched on the left side
                So we end up applying the right filters twice, once on the right side and once on top of the join
                This will result in major performance optimization when the lookup join is expanding
                and applying the right filters reduces the expansion significantly.
                For example, consider an expanding lookup join of 100,000 rows table with 10,000 lookup table
                with filter of selectivity 0.1% on the right side(keeps 10 out of 10,000 rows of the lookup table).
                In the non-optimized version the filter is not pushed to the right, and we get an explosion of records.
                We have 100,000x10,000 = 1,000,000,000 rows after the join without the optimization.
                Then we filter then out to only 1,000,000 rows.
                With the optimization we apply the filter early so after the expanding join we only have 1,000,000 rows.
                This reduced max number of rows used by a factor of 1,000

                In the future, once we have inner join support, it is usually possible to convert the lookup join into an inner join
                This would allow us to not reapply the filters pushed to the right side again above the join,
                as the inner join would only return rows that match on both sides.
                */
            }
            if (optimizationApplied) {
                // if we pushed down some filters, we need to update the filters to reapply above the join
                Expression remainingFilter = Predicates.combineAnd(CollectionUtils.combine(scoped.commonFilters, scoped.rightFilters));
                plan = remainingFilter != null ? filter.with(join, remainingFilter) : join;
            }
        }
        // ignore the rest of the join
        return plan;
    }

    /**
     * Builds the right pushable filters for the given expressions.
     */
    private static List<Expression> buildRightPushableFilters(List<Expression> expressions, FoldContext foldCtx) {
        return expressions.stream().filter(x -> isRightPushableFilter(x, foldCtx)).toList();
    }

    /**
     * Determines if the given expression can be pushed down to the right side of a join.
     * A filter is right pushable if the filter's predicate evaluates to false or null when all fields are set to null
     * This rule helps us guard against the case where we don't know if a field is null because:
     * 1. the field is null in the source data or
     * 2. the field is null because there was no match in the join
     * If the null could be an issue we just say the filter is not pushable and we avoid this issue
     * In this context pushable means that we can push the filter down to the right side of a LEFT join
     * We do not check if the filter is pushable to Lucene or not here
     */
    private static boolean isRightPushableFilter(Expression filter, FoldContext foldCtx) {
        // traverse the filter tree
        // replace any reference to an attribute with a null literal
        Expression nullifiedFilter = filter.transformUp(Attribute.class, r -> new Literal(r.source(), null, r.dataType()));
        // try to fold the filter
        // check if the folded filter evaluates to false or null, if yes return true
        // pushable WHERE field > 1 (evaluates to null), WHERE field is NOT NULL (evaluates to false)
        // not pushable WHERE field is NULL (evaluates to true), WHERE coalesce(field, 10) == 10 (evaluates to true)
        if (nullifiedFilter.foldable()) {
            Object folded = nullifiedFilter.fold(foldCtx);
            return folded == null || Boolean.FALSE.equals(folded);
        }
        return false;
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

    /* Push down filters that can be evaluated by the UnionAll child/leg to each child/leg,
     * so that the filters can be pushed down further to the data source when possible.
     * Filters that cannot be pushed down remain above the UnionAll.
     *
     * The children of a UnionAll/Fork plan has a similar pattern, as Fork adds EsqlProject,
     * an optional Eval and Limit on top of its actual children.
     * UnionAll
     *   EsqlProject
     *     Eval (optional)
     *       Limit
     *         EsRelation
     *   EsqlProject
     *     Eval (optional)
     *       Limit
     *         Subquery
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
        // Preserve the filter on top of UnionAll if not all pushable predicates can be pushed down into UnionAll children.
        // This happens when the pushable predicate contains ReferenceAttribute that cannot be mapped to children's output correctly.
        boolean preserveOriginalFilterOnTopOfUnionAll = false;
        // Push the filter down to each child of the UnionAll, the child of a UnionAll is always a project
        // followed by an optional eval and then limit added by fork and then the real child
        List<LogicalPlan> newChildren = new ArrayList<>();
        boolean changed = false;
        for (LogicalPlan child : unionAll.children()) {
            if (child instanceof Project project) {
                LogicalPlan newChild = maybePushDownFilterPastEvalAndLimitForUnionAllChild(pushable, project);
                if (newChild != child) {
                    changed = true;
                } else {
                    preserveOriginalFilterOnTopOfUnionAll = true;
                }
                newChildren.add(newChild);
            } else { // unexpected pattern, just add the child as is
                newChildren.add(child);
            }
        }

        if (changed == false) { // nothing changed, return the original plan
            return filter;
        }

        LogicalPlan newUnionAll = unionAll.replaceChildren(newChildren);
        if (preserveOriginalFilterOnTopOfUnionAll) {
            // Preserve the filter on top of UnionAll as some pushable predicates cannot be pushed down
            // to make sure correct results are returned
            return filter.replaceChild(newUnionAll);
        }
        if (nonPushable.isEmpty()) {
            return newUnionAll;
        } else {
            return filter.with(newUnionAll, Predicates.combineAnd(nonPushable));
        }
    }

    private static LogicalPlan maybePushDownFilterPastEvalAndLimitForUnionAllChild(List<Expression> pushable, Project project) {
        List<Expression> resolvedPushable = new ArrayList<>();
        // Make sure the pushable predicates can find their corresponding attributes in the child project
        for (Expression exp : pushable) {
            Expression replaced = resolveUnionAllOutputByName(exp, project.projections());
            if (replaced == null || replaced == exp) {
                // cannot find the attribute in the child project, cannot push down this filter
                return project;
            } else {
                resolvedPushable.add(replaced);
            }
        }
        if (resolvedPushable.size() != pushable.size()) {
            // Some pushable predicates cannot be resolved to the child project, cannot push down.
            // This should not happen, however we need to be cautious here, if the predicate is removed from the main query,
            // and it is not pushed down into the UnionAll child, the result will be incorrect.
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
}
