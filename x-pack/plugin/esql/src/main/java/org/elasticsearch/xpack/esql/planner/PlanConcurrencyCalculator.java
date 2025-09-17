/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Calculates the maximum number of nodes that should be queried concurrently for the given data node plan.
 * <p>
 *     Used to avoid overloading the cluster with concurrent requests that may not be needed.
 * </p>
 */
public class PlanConcurrencyCalculator {
    public static final PlanConcurrencyCalculator INSTANCE = new PlanConcurrencyCalculator();

    private PlanConcurrencyCalculator() {}

    /**
     * @return {@code null} if there should be no limit, otherwise, the maximum number of nodes that should be queried concurrently.
     */
    @Nullable
    public Integer calculateNodesConcurrency(PhysicalPlan dataNodePlan, Configuration configuration) {
        // If available, pragma overrides any calculation
        if (configuration.pragmas().maxConcurrentNodesPerCluster() > 0) {
            return configuration.pragmas().maxConcurrentNodesPerCluster();
        }
        if (dataNodePlan == null) {
            return null;
        }

        Integer dataNodeLimit = getDataNodeLimit(dataNodePlan);

        if (dataNodeLimit != null) {
            return limitToConcurrency(dataNodeLimit);
        }

        return null;
    }

    private Integer limitToConcurrency(int limit) {
        // For high limits, don't limit the concurrency
        if (limit > 1000) {
            return null;
        }

        // At least 2 nodes, otherwise log2(limit). E.g.
        // Limit | Concurrency
        // 1 | 2
        // 10 | 3
        // 1000 | 9
        return Math.max(2, (int) (Math.log(limit) / Math.log(2)));
    }

    @Nullable
    private Integer getDataNodeLimit(PhysicalPlan dataNodePlan) {
        LogicalPlan logicalPlan = getFragmentPlan(dataNodePlan);

        // State machine to find:
        // A relation
        Holder<Boolean> relationFound = new Holder<>(false);
        // ...followed by no other node that could break the calculation
        Holder<Boolean> forbiddenNodeFound = new Holder<>(false);
        // ...and finally, a limit
        Holder<Integer> limitValue = new Holder<>(null);

        logicalPlan.forEachUp(node -> {
            // If a limit or a forbidden command was already found, ignore the rest
            if (limitValue.get() == null && forbiddenNodeFound.get() == false) {
                if (node instanceof EsRelation) {
                    relationFound.set(true);
                } else if (relationFound.get()) {
                    if (node instanceof Limit limit && limit.limit() instanceof Literal literalLimit) {
                        limitValue.set((Integer) literalLimit.value());
                    } else {
                        forbiddenNodeFound.set(true);
                    }
                }
            }
        });

        return limitValue.get();
    }

    private LogicalPlan getFragmentPlan(PhysicalPlan plan) {
        Holder<LogicalPlan> foundPlan = new Holder<>();
        plan.forEachDown(node -> {
            if (node instanceof FragmentExec fragment) {
                foundPlan.set(fragment.fragment());
            }
        });
        return foundPlan.get();
    }
}
