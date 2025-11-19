/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.Translatable.YES;

/**
 * When KNN is part off a conjuction, we will have added the conjunction as a prefilter in
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownConjunctionsToKnnPrefilters }. To avoid including the prefilter
 * as an additional scorable clause in the final query, we can remove the conjunction that has already been included into the prefilters
 * in case KNN can be pushed down.
 */
public class RemoveKnnPushablePrefiltersFromConjunctions extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FilterExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(FilterExec filter, LocalPhysicalOptimizerContext context) {

        Expression newCondition = filter.condition().transformUp(exp -> {
            if (exp instanceof And and) {
                Expression left = and.left();
                Expression right = and.right();
                // When one of the sides is a knn function and the other side is used as a prefilter,
                // returns knn as the other side is redundant
                if (isPushableKnnAndContainsFilters(left, right, context)) {
                    return left;
                } else if (isPushableKnnAndContainsFilters(right, left, context)) {
                    return right;
                }
            }
            return exp;
        });
        if (newCondition.equals(filter.condition())) {
            return filter;
        }

        return new FilterExec(filter.source(), filter.child(), newCondition);
    }

    private static boolean isPushableKnnAndContainsFilters(Expression first, Expression second, LocalPhysicalOptimizerContext context) {
        return first instanceof Knn knn
            && knn.translatable(LucenePushdownPredicates.from(context.searchStats(), context.flags())) == YES
            && second instanceof Knn == false
            && knn.filterExpressions().contains(second);
    }

}
