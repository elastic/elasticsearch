/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

/**
 * Removes empty branches from a {@link UnionPlan} ({@code FORK}, subquery {@code UnionAll},
 * or {@link org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll}). The simplest case is a
 * {@code FORK} branch that contains {@code WHERE false}. Empty branches also appear when
 * {@code FORK} is used for conditional execution based on query parameters:
 * {@snippet lang="esql" :
 * FROM my-index METADATA _score
 * | ...
 * | FORK ( SORT _score | LIMIT 10) // return top hits every time
 *        ( WHERE ?include_completion | STATS s = values(title) | COMPLETION ...)
 * }
 */
public class PruneEmptyUnionBranches extends OptimizerRules.OptimizerRule<UnionPlan> {
    @Override
    protected LogicalPlan rule(UnionPlan unionPlan) {
        // Special case first: every branch is empty → collapse to an empty LocalRelation.
        // pruneEmptyBranches's all-empty defensive no-op leaves the union untouched, which is
        // why we have to detect this case ourselves before delegating.
        if (unionPlan.children().stream().allMatch(PruneEmptyUnionBranches::isEmptyLocalRelation)) {
            return new LocalRelation(unionPlan.source(), unionPlan.output(), EmptyLocalSupplier.EMPTY);
        }
        // For UnionPlan the base implementation calls replaceChildren. ViewUnionAll overrides
        // pruneEmptyBranches so the named-subqueries map stays in sync.
        return unionPlan.pruneEmptyBranches(PruneEmptyUnionBranches::isEmptyLocalRelation);
    }

    private static boolean isEmptyLocalRelation(LogicalPlan plan) {
        return plan instanceof LocalRelation lr && lr.hasEmptySupplier();
    }
}
