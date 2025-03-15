/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

public class PlanConcurrencyCalculator {
    public static final PlanConcurrencyCalculator INSTANCE = new PlanConcurrencyCalculator();

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
        // TODO: Request FoldContext or a context containing it

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
        // TODO: Do some conversion here
        return limit;
    }

    private Integer getDataNodeLimit(PhysicalPlan dataNodePlan) {
        LogicalPlan logicalPlan = getFragmentPlan(dataNodePlan);

        // State machine to find:
        // A relation
        Holder<Boolean> relationFound = new Holder<>(false);
        // ...followed by NO filters
        Holder<Boolean> filterFound = new Holder<>(false);
        // ...and finally, a limit
        Holder<Integer> limitValue = new Holder<>(null);

        logicalPlan.forEachUp(node -> {
            if (node instanceof EsRelation) {
                relationFound.set(true);
            } else if (node instanceof Filter) {
                filterFound.set(true);
            } else if (relationFound.get() && filterFound.get() == false) {
                // We only care about the limit if there's a relation before it, and no filter in between
                if (node instanceof Limit limit) {
                    assert limitValue.get() == null : "Multiple limits found in the same data node plan";
                    limitValue.set((Integer) limit.limit().fold(FoldContext.small()));
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
