/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Set;

public class PlanConcurrencyCalculator {
    public static final PlanConcurrencyCalculator INSTANCE = new PlanConcurrencyCalculator();

    /**
     * Set of allowed {@link LogicalPlan} classes that may safely appear before the {@link Limit}.
     */
    private static final Set<Class<? extends LogicalPlan>> ALLOWED_PLAN_CLASSES = Set.of(EsRelation.class, Limit.class);

    private PlanConcurrencyCalculator() {}

    /**
     * Calculates the maximum number of nodes that should be executed concurrently for the given data node plan.
     * <p>
     *     Used to avoid overloading the cluster with concurrent requests that may not be needed.
     * </p>
     *
     * @return Null if there should be no limit, otherwise, the maximum number of nodes that should be executed concurrently.
     */
    public Integer calculateNodesConcurrency(PhysicalPlan dataNodePlan, Configuration configuration) {
        // If available, pragma overrides any calculation
        if (configuration.pragmas().maxConcurrentNodesPerCluster() > 0) {
            return configuration.pragmas().maxConcurrentNodesPerCluster();
        }

        // TODO: Should this class take into account the node roles/tiers? What about the index modes like LOOKUP?
        // TODO: Interactions with functions like SAMPLE()?

        // TODO: ---
        // # Positive cases
        // - FROM | LIMIT | _anything_: Fragment[EsRelation, Limit]
        // -

        // # Negative cases
        // - FROM | STATS: Fragment[EsRelation, Aggregate]
        // - SORT: Fragment[EsRelation, TopN]
        // - WHERE: Fragment[EsRelation, Filter]

        Integer dataNodeLimit = getDataNodeLimit(dataNodePlan);

        if (dataNodeLimit != null) {
            return limitToConcurrency(dataNodeLimit);
        }

        return null;
    }

    private int limitToConcurrency(int limit) {
        // TODO: Should there be a calculation running like:
        // "after each node finishes, recalculate the concurrency based on the amount of results"
        return limit;
    }

    private Integer getDataNodeLimit(PhysicalPlan dataNodePlan) {
        LogicalPlan logicalPlan = getFragmentPlan(dataNodePlan);


        // State machine to find:
        // A relation
        Holder<Boolean> relationFound = new Holder<>(false);
        // ...followed by NO non-whitelisted nodes that could break the calculation
        Holder<Boolean> forbiddenNodeFound = new Holder<>(false);
        // ...and finally, a limit
        Holder<Integer> limitValue = new Holder<>(null);

        logicalPlan.forEachUp(node -> {
            // If a limit or a blacklisted command was already found, ignore the rest
            if (limitValue.get() == null && forbiddenNodeFound.get() == false) {
                if (node instanceof EsRelation) {
                    relationFound.set(true);
                } else if (relationFound.get()) {
                    if (ALLOWED_PLAN_CLASSES.contains(node.getClass()) == false) {
                        forbiddenNodeFound.set(true);
                    } else if (node instanceof Limit limit && limit.limit() instanceof Literal literalLimit) {
                        limitValue.set((Integer) literalLimit.value());
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
