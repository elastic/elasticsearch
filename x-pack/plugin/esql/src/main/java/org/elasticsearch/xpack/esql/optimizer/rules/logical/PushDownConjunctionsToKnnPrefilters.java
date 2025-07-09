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
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class PushDownConjunctionsToKnnPrefilters extends OptimizerRules.OptimizerRule<Filter> {

    @Override
    protected LogicalPlan rule(Filter filter) {
        Stack<Expression> filters = new Stack<>();
        Expression condition = filter.condition();
        Expression newCondition = pushConjunctionsToKnn(condition, filters, null);

        return condition.equals(newCondition) ? filter : filter.with(newCondition);
    }

    private static Expression pushConjunctionsToKnn(Expression expression, List<Expression> filters, Expression addedFilter) {
        if (addedFilter != null) {
            filters.add(addedFilter);
        }
        Expression result = switch(expression) {
            case And and:
                Expression newLeft = pushConjunctionsToKnn(and.left(), filters, and.right());
                Expression newRight = pushConjunctionsToKnn(and.right(), filters, and.left());
                if (newLeft.equals(and.left()) && newRight.equals(and.right())) {
                    yield and;
                }
                yield and.replaceChildrenSameSize(List.of(newLeft, newRight));
            case Knn knn:
                yield knn.withFilters(List.copyOf(filters));
            default:
                List<Expression> children = expression.children();
                boolean childrenChanged = false;

                // TODO This copies transformChildren
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
            filters.removeLast();
        }

        return result;
    }
}
