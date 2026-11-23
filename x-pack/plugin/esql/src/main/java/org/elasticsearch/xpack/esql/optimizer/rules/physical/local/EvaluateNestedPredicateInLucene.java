/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

/**
 * Lets Lucene evaluate a {@code NESTED_ANY} predicate that {@link PushNestedPredicateToSource} could not remove from the plan,
 * as in {@code EVAL} or beside a parent condition in a disjunction. It matches the parent against a nested query, which stops
 * at the parent's first matching child and never reads the children. Only an exact translation can replace the evaluator,
 * because one that needs a recheck would match parents the predicate does not.
 */
public final class EvaluateNestedPredicateInLucene extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    PhysicalPlan,
    LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        if (plan instanceof FilterExec == false && plan instanceof EvalExec == false) {
            return plan;
        }
        LucenePushdownPredicates children = PushNestedPredicateToSource.childFields(context.flags());
        return plan.transformExpressionsOnly(NestedPredicate.class, predicate -> {
            if (predicate.queryBuilder() != null) {
                return predicate;
            }
            PushNestedPredicateToSource.Translation translation = PushNestedPredicateToSource.translate(
                predicate,
                children,
                context.searchStats()
            );
            return translation != null && translation.exact() ? predicate.withQueryBuilder(translation.query()) : predicate;
        });
    }
}
