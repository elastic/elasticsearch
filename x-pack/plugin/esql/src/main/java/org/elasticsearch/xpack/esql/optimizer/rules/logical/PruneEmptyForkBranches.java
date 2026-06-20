/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

/**
 * Removes FORK branches or subqueries from UnionAll that only contain an empty LocalRelation.
 * The simplest case where this can happen is when a FORK branch contains {@code WHERE false}.
 * In practice, empty FORK branches can also happen when FORK is used for conditional execution of the query
 * based on query parameters:
 * {@snippet lang="esql" :
 * FROM my-index METADATA _score
 * | ...
 * | FORK ( SORT _score | LIMIT 10) // return top hits every time
 *        ( WHERE ?include_completion | STATS s = values(title) | COMPLETION ...)
 * }
 */
public class PruneEmptyForkBranches extends OptimizerRules.OptimizerRule<Fork> {
    @Override
    protected LogicalPlan rule(Fork fork) {
        // Special case first: every branch is empty → collapse to an empty LocalRelation.
        // pruneEmptyBranches's all-empty defensive no-op leaves the Fork untouched, which is
        // why we have to detect this case ourselves before delegating.
        if (fork.children().stream().allMatch(PruneEmptyForkBranches::isEmptyLocalRelation)) {
            return new LocalRelation(fork.source(), fork.output(), EmptyLocalSupplier.EMPTY);
        }
        // For Fork itself the base implementation calls replaceChildren and returns a new Fork.
        // For UnionAll/ViewUnionAll the polymorphic overrides take care of the single-survivor
        // collapse and (for ViewUnionAll) the named-subqueries map.
        return fork.pruneEmptyBranches(PruneEmptyForkBranches::isEmptyLocalRelation);
    }

    private static boolean isEmptyLocalRelation(LogicalPlan plan) {
        return plan instanceof LocalRelation lr && lr.hasEmptySupplier();
    }
}
