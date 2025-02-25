/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;

import java.util.function.IntSupplier;

/**
 * Attempts to rewrite the fragment enclosed in a data-node plan based on the progress of the query. For example:
 * `FROM x | LIMIT 100` can be safely rewritten to `FROM x | LIMIT 40` if 60 rows have already been collected.
 * TODO: Rewrite TopN to filters
 */
public final class QueryProgressFragmentOptimizer {
    private final PhysicalVerifier verifier = PhysicalVerifier.INSTANCE;
    private final IntSupplier alreadyCollectedLimit;

    /**
     * @param alreadyCollectedLimit the supplier to get the number of rows already collected
     */
    public QueryProgressFragmentOptimizer(@Nullable IntSupplier alreadyCollectedLimit) {
        this.alreadyCollectedLimit = alreadyCollectedLimit;
    }

    /**
     * Attempts to optimize the fragment enclosed in a data-node plan based on the progress of the query.
     * @param plan the input plan
     * @return the optimized plan. If this returns null, the query can be early terminated.
     */
    public PhysicalPlan optimizeFragment(PhysicalPlan plan) {
        if (alreadyCollectedLimit == null) {
            return plan;
        }
        final var fragments = plan.collectFirstChildren(p -> p instanceof FragmentExec);
        if (fragments.size() != 1) {
            return plan;
        }
        final FragmentExec fragment = (FragmentExec) fragments.getFirst();
        final var pipelineBreakers = fragment.fragment().collectFirstChildren(Mapper::isPipelineBreaker);
        if (pipelineBreakers.isEmpty()) {
            return plan;
        }
        // Rewrite LIMIT
        if (pipelineBreakers.getFirst() instanceof Limit firstLimit) {
            final int collected = alreadyCollectedLimit.getAsInt();
            if (collected == 0) {
                return plan;
            }
            final int originalLimit = (int) firstLimit.limit().fold(FoldContext.small());
            if (originalLimit <= collected) {
                return null;
            }
            final var newFragment = fragment.fragment().transformUp(Limit.class, l -> {
                if (l == firstLimit) {
                    return l.withLimit(Literal.of(firstLimit.limit(), originalLimit - collected));
                } else {
                    return l;
                }
            });
            var newPlan = plan.transformUp(FragmentExec.class, f -> {
                if (f == fragment) {
                    return new FragmentExec(newFragment);
                } else {
                    return f;
                }
            });
            verifier.verify(newPlan);
            return newPlan;
        }

        return plan;
    }
}
