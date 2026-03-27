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

import java.util.ArrayList;
import java.util.List;

/**
 * Removes FORK branches or subqueries from UnionAll that only contain an empty LocalRelation.
 * The simplest case where this can happen is when a FORK branch contains {@code WHERE false}.
 * In practice, empty FORK branches can also happen when FORK is used for conditional execution of the query
 * based on query parameters:
 * <pre>{@code
 * FROM my-index METADATA _score
 * | ...
 * | FORK ( SORT _score | LIMIT 10) // return top hits every time
 *        ( WHERE ?include_completion | STATS s = values(title) | COMPLETION ...)
 * }</pre>
 */
public class PruneEmptyForkBranches extends OptimizerRules.OptimizerRule<Fork> {
    @Override
    protected LogicalPlan rule(Fork fork) {
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (LogicalPlan forkChild : fork.children()) {
            if (forkChild instanceof LocalRelation localRelation && localRelation.hasEmptySupplier()) {
                continue;
            }

            newChildren.add(forkChild);
        }

        // we removed all children - just return an empty relation
        if (newChildren.isEmpty()) {
            return new LocalRelation(fork.source(), fork.output(), EmptyLocalSupplier.EMPTY);
        }

        return newChildren.size() != fork.children().size() ? fork.replaceChildren(newChildren) : fork;
    }
}
