/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class CreateSideChannels extends ParameterizedRule<PhysicalPlan, PhysicalPlan, LocalPhysicalOptimizerContext> {
    @Override
    public PhysicalPlan apply(PhysicalPlan physicalPlan, LocalPhysicalOptimizerContext localPhysicalOptimizerContext) {
        Scan scan = new Scan(localPhysicalOptimizerContext.globalBreaker());
        physicalPlan.forEachDown(scan);
        if (scan.plan.isEmpty()) {
            return physicalPlan;
        }
        physicalPlan = physicalPlan.transformDown(p -> {
            List<Function<PhysicalPlan, PhysicalPlan>> actions = scan.plan.remove(p);
            if (actions != null) {
                for (Function<PhysicalPlan, PhysicalPlan> action : actions) {
                    p = action.apply(p);
                }
            }
            return p;
        });
        if (scan.plan.isEmpty() == false) {
            throw new IllegalStateException("didn't consume entire plan: " + scan.plan);
        }
        return physicalPlan;
    }

    private static class Scan implements Consumer<PhysicalPlan> {
        private final CircuitBreaker breaker;
        private final Map<PhysicalPlan, List<Function<PhysicalPlan, PhysicalPlan>>> plan = new IdentityHashMap<>();
        private TopNExec lastTopN;

        private Scan(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public void accept(PhysicalPlan physicalPlan) {
            if (physicalPlan instanceof TopNExec topN) {
                this.lastTopN = topN;
                return;
            }
            // NOCOMMIT are there any nodes that should *clear* lastTopN? because the minCompetitive can't flow through them
            if (physicalPlan instanceof EsQueryExec query) {
                onQueryExec(query);
            }
        }

        private void onQueryExec(EsQueryExec query) {
            if (lastTopN == null) {
                return;
            }
            SharedMinCompetitive.Supplier minCompetitive = new SharedMinCompetitive.Supplier(breaker, lastTopN.minCompetitiveKeyConfig());
            Function<EsQueryExec, EsQueryExec> offer = query.offerMinCompetitive(minCompetitive, lastTopN.order());
            if (offer == null) {
                return;
            }
            plan.computeIfAbsent(query, k -> new ArrayList<>()).add(p -> offer.apply((EsQueryExec) p));
            plan.computeIfAbsent(lastTopN, k -> new ArrayList<>()).add(p -> {
                TopNExec topN = (TopNExec) p;
                return topN.withMinCompetitive(minCompetitive);
            });
        }
    }

}
