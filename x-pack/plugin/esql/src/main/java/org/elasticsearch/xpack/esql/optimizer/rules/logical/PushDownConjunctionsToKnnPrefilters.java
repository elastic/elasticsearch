/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Stack;

/**
 * Rewrites an expression tree to push down conjunctions in the prefilter of {@link Knn} functions.
 * knn functions won't contain other knn functions as a prefilter, to avoid circular dependencies.
 *  Given an expression tree like {@code (A OR B) AND (C AND knn())} this rule will rewrite it to
 *     {@code (A OR B) AND (C AND knn(filterExpressions = [(A OR B), C]))}
*/
public class PushDownConjunctionsToKnnPrefilters extends OptimizerRules.OptimizerRule<Filter> {

    @Override
    protected LogicalPlan rule(Filter filter) {
        Stack<Expression> filters = new Stack<>();
        Expression condition = filter.condition();
        Expression newCondition = pushConjunctionsToKnn(condition, filters, null);

        return condition.equals(newCondition) ? filter : filter.with(newCondition);
    }

    /**
     * Updates knn function prefilters. This method processes conjunctions so knn functions on one side of the conjunction receive
     * the other side of the conjunction as a prefilter
     *
     * @param expression expression to process recursively
     * @param filters current filters to apply to the expression. They contain expressions on the other side of the traversed conjunctions
     * @param addedFilter a new filter to add to the list of filters for the processing
     * @return the updated expression, or the original expression if it doesn't need to be updated
     */
    private static Expression pushConjunctionsToKnn(Expression expression, Stack<Expression> filters, Expression addedFilter) {
        if (addedFilter != null) {
            filters.push(addedFilter);
        }
        Expression result = switch (expression) {
            case And and:
                // Traverse both sides of the And, using the other side as the added filter
                Expression newLeft = pushConjunctionsToKnn(and.left(), filters, and.right());
                Expression newRight = pushConjunctionsToKnn(and.right(), filters, and.left());
                if (newLeft.equals(and.left()) && newRight.equals(and.right())) {
                    yield and;
                }
                yield and.replaceChildrenSameSize(List.of(newLeft, newRight));
            case Knn knn:
                // We don't want knn expressions to have other knn expressions as a prefilter to avoid circular dependencies
                List<Expression> newFilters = filters.stream()
                    .map(PushDownConjunctionsToKnnPrefilters::removeKnn)
                    .filter(Objects::nonNull)
                    .toList();
                if (newFilters.equals(knn.filterExpressions())) {
                    yield knn;
                }
                yield knn.withFilters(newFilters);
            default:
                List<Expression> children = expression.children();
                boolean childrenChanged = false;

                // This copies transformChildren algorithm to avoid unnecessary changes
                List<Expression> transformedChildren = null;

                for (int i = 0, s = children.size(); i < s; i++) {
                    Expression child = children.get(i);
                    Expression next = pushConjunctionsToKnn(child, filters, null);
                    if (child.equals(next) == false) {
                        // lazy copy + replacement in place
                        if (childrenChanged == false) {
                            childrenChanged = true;
                            transformedChildren = new ArrayList<>(children);
                        }
                        transformedChildren.set(i, next);
                    }
                }

                yield (childrenChanged ? expression.replaceChildrenSameSize(transformedChildren) : expression);
        };

        if (addedFilter != null) {
            filters.pop();
        }

        return result;
    }

    /**
     * Removes knn functions from the expression tree
     * @param expression expression to process
     * @return expression without knn functions, or null if the expression is a knn function
     */
    private static Expression removeKnn(Expression expression) {
        if (expression.children().isEmpty()) {
            return expression;
        }
        if (expression instanceof Knn) {
            return null;
        }

        List<Expression> filteredChildren = expression.children()
            .stream()
            .map(PushDownConjunctionsToKnnPrefilters::removeKnn)
            .filter(Objects::nonNull)
            .toList();
        if (filteredChildren.equals(expression.children())) {
            return expression;
        } else if (filteredChildren.isEmpty()) {
            return null;
        } else if (expression instanceof BinaryLogic && filteredChildren.size() == 1) {
            // Simplify an AND / OR expression to a single child
            return filteredChildren.getFirst();
        } else {
            return expression.replaceChildrenSameSize(filteredChildren);
        }
    }
}
